use futures::{Future, FutureExt, StreamExt};
use reth_db::database::Database;
use reth_executor::blockchain_tree::BlockchainTree;
use reth_interfaces::{
    consensus::{Consensus, ForkchoiceState},
    sync::SyncStateUpdater,
};
use reth_primitives::{SealedBlock, H256};
use reth_provider::ExecutorFactory;
use reth_stages::{Pipeline, PipelineError, PipelineFut};
use std::{
    default,
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

impl<DB: Database, U: SyncStateUpdater> PipelineState<DB, U> {
    fn is_idle(&self) -> bool {
        matches!(self, PipelineState::Idle(_))
    }
}

pub enum SyncControllerMessage {
    ForkchoiceUpdated(ForkchoiceState),
    NewPayload(SealedBlock), // TODO: add oneshot for sending result back
}

#[derive(Default)]
pub enum SyncControllerAction {
    #[default]
    None,
    RunPipeline,
}

impl SyncControllerAction {
    fn run_pipeline(&self) -> bool {
        matches!(self, SyncControllerAction::RunPipeline)
    }
}

#[must_use = "Future does nothing unless polled"]
pub struct SyncController<DB: Database, U: SyncStateUpdater, C: Consensus, EF: ExecutorFactory> {
    db: Arc<DB>,
    pipeline_state: Option<PipelineState<DB, U>>,
    blockchain_tree: BlockchainTree<DB, C, EF>,
    message_rx: UnboundedReceiverStream<SyncControllerMessage>,
    forkchoice_state: Option<ForkchoiceState>,
    next_action: SyncControllerAction,
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
            next_action: SyncControllerAction::RunPipeline,
        }
    }

    fn pipeline_is_idle(&self) -> bool {
        self.pipeline_state.as_ref().expect("pipeline state is set").is_idle()
    }

    fn pipeline_run_needed(&mut self) {
        self.next_action = SyncControllerAction::RunPipeline;
    }

    fn set_next_pipeline_state(&mut self, cx: &mut Context<'_>) -> Result<(), PipelineError> {
        // Lookup the forkchoice state. We can't launch the pipeline without the tip.
        let forckchoice_state = match &self.forkchoice_state {
            Some(state) => state,
            None => return Ok(()),
        };

        let tip = forckchoice_state.head_block_hash;
        let next_state = match self.pipeline_state.take().expect("pipeline state is set") {
            PipelineState::Running(mut fut) => {
                match fut.poll_unpin(cx) {
                    Poll::Ready((pipeline, result)) => {
                        // Any pipeline error at this point is fatal.
                        result?;
                        // Get next pipeline state.
                        self.next_pipeline_state(pipeline, tip)
                    }
                    Poll::Pending => PipelineState::Running(fut),
                }
            }
            PipelineState::Idle(pipeline) => self.next_pipeline_state(pipeline, tip),
        };
        self.pipeline_state = Some(next_state);
        Ok(())
    }

    fn next_pipeline_state(
        &mut self,
        pipeline: Pipeline<DB, U>,
        tip: H256,
    ) -> PipelineState<DB, U> {
        let next_action = std::mem::take(&mut self.next_action);
        if next_action.run_pipeline() {
            PipelineState::Running(pipeline.run_as_fut(self.db.clone(), tip))
        } else {
            PipelineState::Idle(pipeline)
        }
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

        let pipeline_is_idle = this.pipeline_is_idle();
        while let Poll::Ready(Some(msg)) = this.message_rx.poll_next_unpin(cx) {
            match msg {
                SyncControllerMessage::ForkchoiceUpdated(state) => {
                    if pipeline_is_idle {
                        let result = this.blockchain_tree.make_canonical(&state.head_block_hash);
                        // TODO: match error
                        if result.is_err() {
                            this.pipeline_run_needed();
                        }
                    }
                    this.forkchoice_state = Some(state);
                }
                SyncControllerMessage::NewPayload(block) => {
                    if pipeline_is_idle {
                        let result = this.blockchain_tree.insert_block(block);
                        // TODO: match error
                        if result.is_err() {
                            this.pipeline_run_needed();
                        }
                    }
                    // TODO: else put into a buffer
                }
            }
        }

        match this.set_next_pipeline_state(cx) {
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
