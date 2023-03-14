use reth_interfaces::executor::Error as ExecError;
use reth_primitives::{Address, Block, U256};
use reth_provider::{execution_result::ExecutionResult, BlockExecutor, StateProvider};

/// Test executor with mocked result.
pub struct TestExecutor(pub Option<ExecutionResult>);

impl<SP: StateProvider> BlockExecutor<SP> for TestExecutor {
    fn execute(
        &mut self,
        _block: &Block,
        _total_difficulty: U256,
        _senders: Option<Vec<Address>>,
    ) -> Result<ExecutionResult, ExecError> {
        self.0.clone().ok_or(ExecError::VerificationFailed)
    }

    fn execute_and_verify_receipt(
        &mut self,
        _block: &Block,
        _total_difficulty: U256,
        _senders: Option<Vec<Address>>,
    ) -> Result<ExecutionResult, ExecError> {
        self.0.clone().ok_or(ExecError::VerificationFailed)
    }
}
