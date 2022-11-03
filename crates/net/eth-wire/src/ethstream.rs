use crate::{
    error::{EthStreamError, HandshakeError},
    types::{forkid::ForkFilter, EthMessage, ProtocolMessage, Status},
};
use bytes::{Bytes, BytesMut};
use futures::{ready, Sink, SinkExt, StreamExt};
use pin_project::pin_project;
use reth_rlp::{Decodable, Encodable};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio_stream::Stream;

/// [`MAX_MESSAGE_SIZE`] is the maximum cap on the size of a protocol message.
// https://github.com/ethereum/go-ethereum/blob/30602163d5d8321fbc68afdcbbaf2362b2641bde/eth/protocols/eth/protocol.go#L50
const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// An `EthStream` wraps over any `Stream` that yields bytes and makes it
/// compatible with eth-networking protocol messages, which get RLP encoded/decoded.
#[pin_project]
pub struct EthStream<S> {
    #[pin]
    inner: S,
    /// Whether the `Status` handshake has been completed
    authed: bool,
}

impl<S> EthStream<S> {
    /// Creates a new unauthed [`EthStream`] from a provided stream. You will need
    /// to manually handshake a peer.
    pub fn new(inner: S) -> Self {
        Self { inner, authed: false }
    }
}

impl<S, E> EthStream<S>
where
    S: Stream<Item = Result<bytes::BytesMut, E>> + Sink<bytes::Bytes, Error = E> + Unpin,
    EthStreamError: From<E>,
{
    /// Given an instantiated transport layer, it proceeds to return an [`EthStream`]
    /// after performing a [`Status`] message handshake as specified in
    pub async fn connect(
        inner: S,
        status: Status,
        fork_filter: ForkFilter,
    ) -> Result<Self, EthStreamError> {
        let mut this = Self::new(inner);
        this.handshake(status, fork_filter).await?;
        Ok(this)
    }

    /// Performs a handshake with the connected peer over the transport stream.
    pub async fn handshake(
        &mut self,
        status: Status,
        fork_filter: ForkFilter,
    ) -> Result<(), EthStreamError> {
        tracing::trace!("sending eth status ...");
        self.send(EthMessage::Status(status)).await?;

        tracing::trace!("waiting for eth status from peer ...");
        let msg = self
            .next()
            .await
            .ok_or_else(|| EthStreamError::HandshakeError(HandshakeError::NoResponse))??;

        // TODO: Add any missing checks
        // https://github.com/ethereum/go-ethereum/blob/9244d5cd61f3ea5a7645fdf2a1a96d53421e412f/eth/protocols/eth/handshake.go#L87-L89
        match msg {
            EthMessage::Status(resp) => {
                self.authed = true;

                if status.genesis != resp.genesis {
                    return Err(HandshakeError::MismatchedGenesis {
                        expected: status.genesis,
                        got: resp.genesis,
                    }
                    .into())
                }

                if status.version != resp.version {
                    return Err(HandshakeError::MismatchedProtocolVersion {
                        expected: status.version,
                        got: resp.version,
                    }
                    .into())
                }

                if status.chain != resp.chain {
                    return Err(HandshakeError::MismatchedChain {
                        expected: status.chain,
                        got: resp.chain,
                    }
                    .into())
                }

                Ok(fork_filter.validate(resp.forkid).map_err(HandshakeError::InvalidFork)?)
            }
            _ => Err(EthStreamError::HandshakeError(HandshakeError::NonStatusMessageInHandshake)),
        }
    }
}

impl<S, E> Stream for EthStream<S>
where
    S: Stream<Item = Result<bytes::BytesMut, E>> + Unpin,
    EthStreamError: From<E>,
{
    type Item = Result<EthMessage, EthStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let res = ready!(this.inner.poll_next(cx));
        let bytes = match res {
            Some(Ok(bytes)) => bytes,
            Some(Err(err)) => return Poll::Ready(Some(Err(err.into()))),
            None => return Poll::Ready(None),
        };

        if bytes.len() > MAX_MESSAGE_SIZE {
            return Poll::Ready(Some(Err(EthStreamError::MessageTooBig(bytes.len()))))
        }

        let msg = match ProtocolMessage::decode(&mut bytes.as_ref()) {
            Ok(m) => m,
            Err(err) => return Poll::Ready(Some(Err(err.into()))),
        };

        if *this.authed && matches!(msg.message, EthMessage::Status(_)) {
            return Poll::Ready(Some(Err(EthStreamError::HandshakeError(
                HandshakeError::StatusNotInHandshake,
            ))))
        }

        Poll::Ready(Some(Ok(msg.message)))
    }
}

impl<S, E> Sink<EthMessage> for EthStream<S>
where
    S: Sink<Bytes, Error = E> + Unpin,
    EthStreamError: From<E>,
{
    type Error = EthStreamError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx).map_err(Into::into)
    }

    fn start_send(self: Pin<&mut Self>, item: EthMessage) -> Result<(), Self::Error> {
        if self.authed && matches!(item, EthMessage::Status(_)) {
            return Err(EthStreamError::HandshakeError(HandshakeError::StatusNotInHandshake))
        }

        let mut bytes = BytesMut::new();
        ProtocolMessage::from(item).encode(&mut bytes);
        let bytes = bytes.freeze();

        self.project().inner.start_send(bytes)?;

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx).map_err(Into::into)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        p2pstream::{CapabilityMessage, HelloMessage, ProtocolVersion, UnauthedP2PStream},
        types::{broadcast::BlockHashNumber, forkid::ForkFilter, EthMessage, Status},
        EthStream, PassthroughCodec,
    };
    use futures::{SinkExt, StreamExt};
    use reth_ecies::{stream::ECIESStream, util::pk2id};
    use secp256k1::{SecretKey, SECP256K1};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::Decoder;

    use crate::types::EthVersion;
    use ethers_core::types::Chain;
    use reth_primitives::{H256, U256};

    #[tokio::test]
    async fn can_handshake() {
        let genesis = H256::random();
        let fork_filter = ForkFilter::new(0, genesis, vec![]);

        let status = Status {
            version: EthVersion::Eth67 as u8,
            chain: Chain::Mainnet.into(),
            total_difficulty: U256::from(0),
            blockhash: H256::random(),
            genesis,
            // Pass the current fork id.
            forkid: fork_filter.current(),
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        let status_clone = status;
        let fork_filter_clone = fork_filter.clone();
        let handle = tokio::spawn(async move {
            // roughly based off of the design of tokio::net::TcpListener
            let (incoming, _) = listener.accept().await.unwrap();
            let stream = crate::PassthroughCodec::default().framed(incoming);
            let _ = EthStream::connect(stream, status_clone, fork_filter_clone).await.unwrap();
        });

        let outgoing = TcpStream::connect(local_addr).await.unwrap();
        let sink = crate::PassthroughCodec::default().framed(outgoing);

        // try to connect
        let _ = EthStream::connect(sink, status, fork_filter).await.unwrap();

        // wait for it to finish
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn can_write_and_read_cleartext() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let test_msg = EthMessage::NewBlockHashes(
            vec![
                BlockHashNumber { hash: reth_primitives::H256::random(), number: 5 },
                BlockHashNumber { hash: reth_primitives::H256::random(), number: 6 },
            ]
            .into(),
        );

        let test_msg_clone = test_msg.clone();
        let handle = tokio::spawn(async move {
            // roughly based off of the design of tokio::net::TcpListener
            let (incoming, _) = listener.accept().await.unwrap();
            let stream = PassthroughCodec::default().framed(incoming);
            let mut stream = EthStream::new(stream);

            // use the stream to get the next message
            let message = stream.next().await.unwrap().unwrap();
            assert_eq!(message, test_msg_clone);
        });

        let outgoing = TcpStream::connect(local_addr).await.unwrap();
        let sink = PassthroughCodec::default().framed(outgoing);
        let mut client_stream = EthStream::new(sink);

        client_stream.send(test_msg).await.unwrap();

        // make sure the server receives the message and asserts before ending the test
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn can_write_and_read_ecies() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let server_key = SecretKey::new(&mut rand::thread_rng());
        let test_msg = EthMessage::NewBlockHashes(
            vec![
                BlockHashNumber { hash: reth_primitives::H256::random(), number: 5 },
                BlockHashNumber { hash: reth_primitives::H256::random(), number: 6 },
            ]
            .into(),
        );

        let test_msg_clone = test_msg.clone();
        let handle = tokio::spawn(async move {
            // roughly based off of the design of tokio::net::TcpListener
            let (incoming, _) = listener.accept().await.unwrap();
            let stream = ECIESStream::incoming(incoming, server_key).await.unwrap();
            let mut stream = EthStream::new(stream);

            // use the stream to get the next message
            let message = stream.next().await.unwrap().unwrap();
            assert_eq!(message, test_msg_clone);
        });

        // create the server pubkey
        let server_id = pk2id(&server_key.public_key(SECP256K1));

        let client_key = SecretKey::new(&mut rand::thread_rng());

        let outgoing = TcpStream::connect(local_addr).await.unwrap();
        let outgoing = ECIESStream::connect(outgoing, client_key, server_id).await.unwrap();
        let mut client_stream = EthStream::new(outgoing);

        client_stream.send(test_msg).await.unwrap();

        // make sure the server receives the message and asserts before ending the test
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn ethstream_over_p2p() {
        // create a p2p stream and server, then confirm that the two are authed
        // create tcpstream
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let server_key = SecretKey::new(&mut rand::thread_rng());
        let test_msg = EthMessage::NewBlockHashes(
            vec![
                BlockHashNumber { hash: reth_primitives::H256::random(), number: 5 },
                BlockHashNumber { hash: reth_primitives::H256::random(), number: 6 },
            ]
            .into(),
        );

        let genesis = H256::random();
        let fork_filter = ForkFilter::new(0, genesis, vec![]);

        let status = Status {
            version: EthVersion::Eth67 as u8,
            chain: Chain::Mainnet.into(),
            total_difficulty: U256::from(0),
            blockhash: H256::random(),
            genesis,
            // Pass the current fork id.
            forkid: fork_filter.current(),
        };

        let status_copy = status;
        let fork_filter_clone = fork_filter.clone();
        let test_msg_clone = test_msg.clone();
        let handle = tokio::spawn(async move {
            // roughly based off of the design of tokio::net::TcpListener
            let (incoming, _) = listener.accept().await.unwrap();
            let stream = ECIESStream::incoming(incoming, server_key).await.unwrap();

            let server_hello = HelloMessage {
                protocol_version: ProtocolVersion::V5,
                client_version: "bitcoind/1.0.0".to_string(),
                capabilities: vec![CapabilityMessage::new(
                    "eth".to_string(),
                    EthVersion::Eth67 as usize,
                )],
                port: 30303,
                id: pk2id(&server_key.public_key(SECP256K1)),
            };

            let unauthed_stream = UnauthedP2PStream::new(stream);
            let p2p_stream = unauthed_stream.handshake(server_hello).await.unwrap();
            let mut eth_stream = EthStream::new(p2p_stream);
            eth_stream.handshake(status_copy, fork_filter_clone).await.unwrap();

            // use the stream to get the next message
            let message = eth_stream.next().await.unwrap().unwrap();
            assert_eq!(message, test_msg_clone);
        });

        // create the server pubkey
        let server_id = pk2id(&server_key.public_key(SECP256K1));

        let client_key = SecretKey::new(&mut rand::thread_rng());

        let outgoing = TcpStream::connect(local_addr).await.unwrap();
        let sink = ECIESStream::connect(outgoing, client_key, server_id).await.unwrap();

        let client_hello = HelloMessage {
            protocol_version: ProtocolVersion::V5,
            client_version: "bitcoind/1.0.0".to_string(),
            capabilities: vec![CapabilityMessage::new(
                "eth".to_string(),
                EthVersion::Eth67 as usize,
            )],
            port: 30303,
            id: pk2id(&client_key.public_key(SECP256K1)),
        };

        let unauthed_stream = UnauthedP2PStream::new(sink);
        let p2p_stream = unauthed_stream.handshake(client_hello).await.unwrap();
        let mut client_stream = EthStream::new(p2p_stream);
        client_stream.handshake(status, fork_filter).await.unwrap();

        client_stream.send(test_msg).await.unwrap();

        // make sure the server receives the message and asserts before ending the test
        handle.await.unwrap();
    }
}