use crate::pipeline_state::PipelineState;
use futures::{Future, FutureExt, StreamExt};
use reth_db::{database::Database, tables, transaction::DbTx};
use reth_executor::blockchain_tree::BlockchainTree;
use reth_interfaces::{
    consensus::{Consensus, ForkchoiceState},
    sync::SyncStateUpdater,
};
use reth_primitives::{BlockHash, SealedBlock, H256};
use reth_provider::ExecutorFactory;
use reth_rpc_types::engine::PayloadStatusEnum;
use reth_stages::{Pipeline, PipelineError};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::{mpsc::UnboundedReceiver, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::*;

#[derive(Debug)]
pub enum SyncControllerMessage {
    ForkchoiceUpdated(ForkchoiceState, oneshot::Sender<PayloadStatusEnum>),
    NewPayload(SealedBlock, oneshot::Sender<PayloadStatusEnum>),
}

#[derive(Debug, Default)]
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

    /// Returns `true` if the pipeline is currently idle.
    fn pipeline_is_idle(&self) -> bool {
        self.pipeline_state.as_ref().expect("pipeline state is set").is_idle()
    }

    /// Set next action to [SyncControllerAction::RunPipeline] to indicate that
    /// controller needs to run the pipeline as soon as it becomes available.
    fn pipeline_run_needed(&mut self) {
        self.next_action = SyncControllerAction::RunPipeline;
    }

    /// Handle the forkchoice updated message.
    fn on_forkchoice_updated(&mut self, state: ForkchoiceState) -> PayloadStatusEnum {
        self.forkchoice_state = Some(state.clone());
        if self.pipeline_is_idle() {
            match self.blockchain_tree.make_canonical(&state.head_block_hash) {
                Ok(_) => PayloadStatusEnum::Valid,
                // TODO: handle/match error
                Err(_error) => {
                    self.pipeline_run_needed();
                    PayloadStatusEnum::Syncing
                }
            }
        } else {
            PayloadStatusEnum::Syncing
        }
    }

    /// Handle new payload message.
    fn on_new_payload(&mut self, block: SealedBlock) -> PayloadStatusEnum {
        if self.pipeline_is_idle() {
            match self.blockchain_tree.insert_block(block) {
                Ok(true) => PayloadStatusEnum::Valid,
                Ok(false) => PayloadStatusEnum::Syncing,
                Err(error) => PayloadStatusEnum::Invalid { validation_error: error.to_string() },
            }
        } else {
            PayloadStatusEnum::Syncing
        }
    }

    /// Returns the next pipeline state depending on the current value of the next action.
    /// Resets the next action to the default value.
    fn next_pipeline_state(
        &mut self,
        pipeline: Pipeline<DB, U>,
        tip: H256,
    ) -> PipelineState<DB, U> {
        let next_action = std::mem::take(&mut self.next_action);
        if next_action.run_pipeline() {
            trace!(target: "sync::controller", ?tip, "Starting the pipeline");
            PipelineState::Running(pipeline.run_as_fut(self.db.clone(), tip))
        } else {
            PipelineState::Idle(pipeline)
        }
    }

    /// Attempt to restore the tree with the finalized block number.
    /// If the finalized block is missing from the database, trigger the pipeline run.
    fn restore_tree_if_possible(
        &mut self,
        finalized_hash: BlockHash,
    ) -> Result<(), reth_interfaces::Error> {
        match self.db.view(|tx| tx.get::<tables::HeaderNumbers>(finalized_hash))?? {
            Some(number) => self.blockchain_tree.restore_canonical_hashes(number)?,
            None => self.pipeline_run_needed(),
        };
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

        // Process all incoming messages first.
        while let Poll::Ready(Some(msg)) = this.message_rx.poll_next_unpin(cx) {
            match msg {
                SyncControllerMessage::ForkchoiceUpdated(state, tx) => {
                    let response = this.on_forkchoice_updated(state);
                    let _ = tx.send(response);
                }
                SyncControllerMessage::NewPayload(block, tx) => {
                    let response = this.on_new_payload(block);
                    let _ = tx.send(response);
                }
            }
        }

        // Set the next pipeline state.
        loop {
            // Lookup the forkchoice state. We can't launch the pipeline without the tip.
            let forckchoice_state = match &this.forkchoice_state {
                Some(state) => state,
                None => return Poll::Pending,
            };

            let tip = forckchoice_state.head_block_hash;
            let next_state = match this.pipeline_state.take().expect("pipeline state is set") {
                PipelineState::Running(mut fut) => {
                    match fut.poll_unpin(cx) {
                        Poll::Ready((pipeline, result)) => {
                            // Any pipeline error at this point is fatal.
                            if let Err(error) = result {
                                return Poll::Ready(Err(error))
                            }

                            // Update the state and hashes of the blockchain tree if possible
                            if let Err(error) = this
                                .restore_tree_if_possible(forckchoice_state.finalized_block_hash)
                            {
                                return Poll::Ready(Err(PipelineError::Internal(Box::new(error))))
                            }

                            // Get next pipeline state.
                            this.next_pipeline_state(pipeline, tip)
                        }
                        Poll::Pending => {
                            this.pipeline_state = Some(PipelineState::Running(fut));
                            return Poll::Pending
                        }
                    }
                }
                PipelineState::Idle(pipeline) => this.next_pipeline_state(pipeline, tip),
            };
            this.pipeline_state = Some(next_state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use reth_db::mdbx::{test_utils::create_test_rw_db, Env, WriteMap};
    use reth_executor::{
        blockchain_tree::{config::BlockchainTreeConfig, externals::TreeExternals},
        test_utils::TestExecutorFactory,
    };
    use reth_interfaces::{sync::NoopSyncStateUpdate, test_utils::TestConsensus};
    use reth_primitives::{ChainSpecBuilder, H256, MAINNET};
    use reth_stages::{test_utils::TestStages, ExecOutput, StageError};
    use std::{collections::VecDeque, time::Duration};
    use tokio::sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        oneshot::error::TryRecvError,
        watch,
    };

    struct TestEnv {
        tip_rx: watch::Receiver<H256>,
        sync_tx: UnboundedSender<SyncControllerMessage>,
    }

    impl TestEnv {
        fn new(
            tip_rx: watch::Receiver<H256>,
            sync_tx: UnboundedSender<SyncControllerMessage>,
        ) -> Self {
            Self { tip_rx, sync_tx }
        }

        fn send_new_payload(&self, block: SealedBlock) -> oneshot::Receiver<PayloadStatusEnum> {
            let (tx, rx) = oneshot::channel();
            self.sync_tx
                .send(SyncControllerMessage::NewPayload(block, tx))
                .expect("failed to send msg");
            rx
        }

        fn send_forkchoice_updated(
            &self,
            state: ForkchoiceState,
        ) -> oneshot::Receiver<PayloadStatusEnum> {
            let (tx, rx) = oneshot::channel();
            self.sync_tx
                .send(SyncControllerMessage::ForkchoiceUpdated(state, tx))
                .expect("failed to send msg");
            rx
        }
    }

    fn setup_controller(
        exec_outputs: VecDeque<Result<ExecOutput, StageError>>,
    ) -> (
        SyncController<Env<WriteMap>, NoopSyncStateUpdate, TestConsensus, TestExecutorFactory>,
        TestEnv,
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
        let pipeline = Pipeline::builder()
            .add_stages(TestStages::new(exec_outputs, Default::default()))
            .with_tip_sender(tip_tx)
            .build();

        // Setup blockchain tree
        let externals = TreeExternals::new(db.clone(), consensus, executor_factory, chain_spec);
        let config = BlockchainTreeConfig::new(1, 2, 3);
        let tree = BlockchainTree::new(externals, config).expect("failed to create tree");

        let (sync_tx, sync_rx) = unbounded_channel();
        (SyncController::new(db, pipeline, tree, sync_rx), TestEnv::new(tip_rx, sync_tx))
    }

    #[tokio::test]
    async fn is_idle_until_forkchoice_is_set() {
        let (controller, env) = setup_controller(VecDeque::from([Err(StageError::ChannelClosed)]));

        let (tx, mut rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = controller.await;
            tx.send(result).expect("failed to forward controller result");
        });

        std::thread::sleep(Duration::from_millis(100));
        assert_matches!(rx.try_recv(), Err(TryRecvError::Empty));

        let fcu_rx = env.send_forkchoice_updated(ForkchoiceState::default());
        assert_matches!(fcu_rx.await, Ok(PayloadStatusEnum::Syncing));

        assert_matches!(rx.await, Ok(Err(PipelineError::Stage(StageError::ChannelClosed))));
    }

    #[tokio::test]
    async fn pipeline_error_is_propagated() {
        let (controller, env) = setup_controller(VecDeque::from([Err(StageError::ChannelClosed)]));

        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = controller.await;
            tx.send(result).expect("failed to forward controller result");
        });

        let _ = env.send_forkchoice_updated(ForkchoiceState::default());
        assert_matches!(rx.await, Ok(Err(PipelineError::Stage(StageError::ChannelClosed))));
    }
}
