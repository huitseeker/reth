use super::TestExecutor;
use parking_lot::Mutex;
use reth_primitives::ChainSpec;
use reth_provider::{execution_result::ExecutionResult, ExecutorFactory, StateProvider};
use std::sync::Arc;

/// Executor factory with pre-set execution results.
pub struct TestExecutorFactory {
    exec_results: Arc<Mutex<Vec<ExecutionResult>>>,
    chain_spec: Arc<ChainSpec>,
}

impl TestExecutorFactory {
    /// Create new instance of test factory.
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self { exec_results: Arc::new(Mutex::new(Vec::new())), chain_spec }
    }

    /// Extend the mocked execution results
    pub fn extend(&self, results: Vec<ExecutionResult>) {
        self.exec_results.lock().extend(results.into_iter());
    }
}

impl ExecutorFactory for TestExecutorFactory {
    type Executor<T: StateProvider> = TestExecutor;

    fn with_sp<SP: StateProvider>(&self, _sp: SP) -> Self::Executor<SP> {
        let exec_res = self.exec_results.lock().pop();
        TestExecutor(exec_res)
    }

    fn chain_spec(&self) -> &ChainSpec {
        self.chain_spec.as_ref()
    }
}
