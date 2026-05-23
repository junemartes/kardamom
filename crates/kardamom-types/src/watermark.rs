// stub: real impl in Task 2.
use crate::position::BPosition;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FsyncWatermark {
    pub recorder_id: u8,
    pub position: BPosition,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QuorumWatermark {
    pub position: BPosition,
}
