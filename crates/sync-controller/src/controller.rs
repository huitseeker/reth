use futures::{Future, FutureExt, StreamExt};
use reth_db::database::Database;
use reth_executor::blockchain_tree::BlockchainTree;
use reth_interfaces::{
    consensus::{Consensus, ForkchoiceState},
    sync::SyncStateUpdater,
};
use reth_primitives::SealedBlock;
use reth_provider::ExecutorFactory;
use reth_stages::{Pipeline, PipelineError, PipelineFut};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::*;

pub enum PipelineState<DB: Database, U: SyncStateUpdater> {
    Idle(Pipeline<DB, U>),
    Running(PipelineFut<DB, U>),
}

pub enum SyncControllerMessage {
    ForkchoiceUpdated(ForkchoiceState),
    NewPayload(SealedBlock), // TODO: add oneshot for sending result back
}

#[must_use = "Future does nothing unless polled"]
pub struct SyncController<DB: Database, U: SyncStateUpdater, C: Consensus, EF: ExecutorFactory> {
    db: Arc<DB>,
    pipeline_state: Option<PipelineState<DB, U>>,
    blockchain_tree: BlockchainTree<DB, C, EF>,
    message_rx: UnboundedReceiverStream<SyncControllerMessage>,
    forkchoice_state: Option<ForkchoiceState>,
    is_sync_needed: bool,
}

impl<DB, U, C, EF> SyncController<DB, U, C, EF>
where
    DB: Database + Unpin + 'static,
    U: SyncStateUpdater + Unpin + 'static,
    C: Consensus,
    EF: ExecutorFactory + 'static,
{
    pub fn new(
        db: Arc<DB>,
        pipeline: Pipeline<DB, U>,
        blockchain_tree: BlockchainTree<DB, C, EF>,
        message_rx: UnboundedReceiver<SyncControllerMessage>,
    ) -> Self {
        Self {
            db,
            pipeline_state: Some(PipelineState::Idle(pipeline)),
            blockchain_tree,
            message_rx: UnboundedReceiverStream::new(message_rx),
            forkchoice_state: None,
            is_sync_needed: true,
        }
    }

    fn set_next_pipeline_state(
        &mut self,
        cx: &mut Context<'_>,
        sync_needed: bool,
    ) -> Result<(), PipelineError> {
        // Lookup the forkchoice state. We can't launch the pipeline without the tip.
        let forckchoice_state = match &self.forkchoice_state {
            Some(state) => state,
            None => return Ok(()),
        };

        let next_state = match self.pipeline_state.take().expect("pipeline state is set") {
            PipelineState::Running(mut fut) => {
                match fut.poll_unpin(cx) {
                    Poll::Ready((pipeline, result)) => {
                        // Any pipeline error at this point is fatal.
                        result?;

                        if sync_needed {
                            PipelineState::Running(
                                pipeline
                                    .run_as_fut(self.db.clone(), forckchoice_state.head_block_hash),
                            )
                        } else {
                            PipelineState::Idle(pipeline)
                        }
                    }
                    Poll::Pending => PipelineState::Running(fut),
                }
            }
            PipelineState::Idle(pipeline) => {
                if sync_needed {
                    PipelineState::Running(
                        pipeline.run_as_fut(self.db.clone(), forckchoice_state.head_block_hash),
                    )
                } else {
                    PipelineState::Idle(pipeline)
                }
            }
        };
        self.pipeline_state = Some(next_state);
        Ok(())
    }
}

/// On initialization, the controller will poll the message receiver and return [Poll::Pending]
/// until the first forkchoice update message is received.
///
/// As soon as the controller receives the first forkchoice updated message and updates the local
/// forkchoice state, it will launch the pipeline to sync to the head hash.
/// While the pipeline is syncing, the controller will keep processing messages from the receiver
/// and forwarding them to the blockchain tree.
impl<DB, U, C, EF> Future for SyncController<DB, U, C, EF>
where
    DB: Database + Unpin + 'static,
    U: SyncStateUpdater + Unpin + 'static,
    C: Consensus + Unpin,
    EF: ExecutorFactory + Unpin + 'static,
{
    type Output = Result<(), PipelineError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut pipeline_sync_needed = false;
        let pipeline_is_idle = matches!(this.pipeline_state, Some(PipelineState::Idle(_)));
        while let Poll::Ready(Some(msg)) = this.message_rx.poll_next_unpin(cx) {
            match msg {
                SyncControllerMessage::ForkchoiceUpdated(state) => {
                    if pipeline_is_idle {
                        // TODO: handle error
                        this.blockchain_tree.make_canonical(&state.head_block_hash).unwrap();
                    }
                    this.forkchoice_state = Some(state);
                }
                SyncControllerMessage::NewPayload(block) => {
                    if pipeline_is_idle {
                        // TODO: handle error
                        this.blockchain_tree.insert_block(block).unwrap();
                    }
                    // TODO: else put into a buffer
                }
            }
        }

        // TODO:
        match this.set_next_pipeline_state(cx, pipeline_sync_needed) {
            Ok(()) => Poll::Pending,
            error @ Err(_) => Poll::Ready(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_db::mdbx::{test_utils::create_test_rw_db, Env, EnvKind, WriteMap};
    use reth_executor::test_utils::TestExecutorFactory;
    use reth_interfaces::{
        p2p::headers::downloader::HeaderDownloader,
        sync::NoopSyncStateUpdate,
        test_utils::{TestConsensus, TestHeaderDownloader},
    };
    use reth_primitives::{ChainSpecBuilder, H256, MAINNET};
    use reth_stages::{sets::OnlineStages, test_utils::TestStages};
    use tokio::sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        watch,
    };

    fn setup_controller() -> (
        UnboundedSender<SyncControllerMessage>,
        SyncController<Env<WriteMap>, NoopSyncStateUpdate, TestConsensus, TestExecutorFactory>,
    ) {
        let db = create_test_rw_db();
        let consensus = TestConsensus::default();
        let chain_spec = Arc::new(
            ChainSpecBuilder::default()
                .chain(MAINNET.chain)
                .genesis(MAINNET.genesis.clone())
                .shanghai_activated()
                .build(),
        );
        let executor_factory = TestExecutorFactory::new(chain_spec.clone());

        // Setup pipeline
        let (tip_tx, tip_rx) = watch::channel(H256::default());
        let pipeline =
            Pipeline::builder().add_stages(TestStages::default()).with_tip_sender(tip_tx).build();

        // Setup blockchain tree
        let tree =
            BlockchainTree::new(db.clone(), consensus, executor_factory, chain_spec, 1, 2, 3)
                .expect("failed to create tree");

        let (sync_tx, sync_rx) = unbounded_channel();
        (sync_tx, SyncController::new(db, pipeline, tree, sync_rx))
    }

    #[tokio::test]
    async fn pipeline_to_live_sync() {
        let (msg_tx, controller) = setup_controller();
        // TODO:
    }
}
