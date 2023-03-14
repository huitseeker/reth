#![warn(missing_docs, unreachable_pub)]
#![deny(unused_must_use, rust_2018_idioms)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))
))]

//! sync controller

use futures::{Future, FutureExt, StreamExt};
use reth_db::database::Database;
use reth_interfaces::{consensus::ForkchoiceState, sync::SyncStateUpdater};
use reth_primitives::{BlockHash, SealedBlock, H256};
use reth_stages::{Pipeline, PipelineFut};
use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;

enum PipelineState<DB: Database, U: SyncStateUpdater> {
    Idle(Pipeline<DB, U>),
    Running(PipelineFut<DB, U>),
}

// TODO:
#[allow(dead_code)]
enum SyncControllerMessage {
    ForkchoiceUpdated(ForkchoiceState),
    NewPayload(SealedBlock),
}

struct SyncController<DB: Database, U: SyncStateUpdater> {
    db: Arc<DB>,
    pipeline_state: Option<PipelineState<DB, U>>,
    message_rx: UnboundedReceiver<SyncControllerMessage>,
    forkchoice_state: Option<ForkchoiceState>,
    is_sync_needed: bool,
    // blockchain_tree: BlockchainTree<DB, C>,
}

impl<DB, U> SyncController<DB, U>
where
    DB: Database + Unpin + 'static,
    U: SyncStateUpdater + Unpin + 'static,
{
    pub fn new(
        db: Arc<DB>,
        pipeline: Pipeline<DB, U>,
        message_rx: UnboundedReceiver<SyncControllerMessage>,
    ) -> Self {
        Self {
            db,
            pipeline_state: Some(PipelineState::Idle(pipeline)),
            message_rx: UnboundedReceiverStream::new(message_rx),
            forkchoice_state: None,
            is_sync_needed: true,
        }
    }

    fn set_next_pipeline_state(&mut self, cx: &mut Context<'_>, sync_needed: bool) {
        // Lookup the forkchoice state. We can't launch the pipeline without the tip.
        let forckchoice_state = match &self.forkchoice_state {
            Some(state) => state,
            None => return (),
        };

        let next_state = match self.pipeline_state.take().expect("pipeline state is set") {
            PipelineState::Running(mut fut) => {
                match fut.poll_unpin(cx) {
                    Poll::Ready((pipeline, result)) => {
                        if let Err(_) = result {
                            // TODO: handle result
                        }
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
    }
}

/// On initialization, the controller will poll the message receiver and return [Poll::Pending]
/// until the first forkchoice update message is received.
///
/// As soon as the controller receives the first forkchoice updated message and updates the local
/// forkchoice state, it will launch the pipeline to sync to the head hash.
/// While the pipeline is syncing, the controller will keep processing messages from the receiver
/// and forwarding them to the blockchain tree.
impl<DB, U> Future for SyncController<DB, U>
where
    DB: Database + Unpin + 'static,
    U: SyncStateUpdater + Unpin + 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let pipeline_sync_needed = false;
        while let Poll::Ready(Some(msg)) = this.message_rx.poll_next_unpin(cx) {
            match msg {
                SyncControllerMessage::ForkchoiceUpdated(state) => {
                    // TODO:
                    this.forkchoice_state = Some(state);
                    if this.pipeline_state.is_idle() {
                        // this.blockchain_tree.make_canonical(state.head_block_hash);
                    }
                }
                SyncControllerMessage::NewPayload(_block) => {
                    // TODO:
                    // if pipeline is syncing TODO: buffer
                    // this.blockchain_tree.insert_block(block);
                }
            }
        }

        // TODO:
        this.set_next_pipeline_state(cx, pipeline_sync_needed);

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {}
