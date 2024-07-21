pub mod coordinator;
pub mod inner;
pub mod signal;
pub mod task;

pub use coordinator::Coordinator;
pub use coordinator_derive::coordinator;
pub use signal::JoinHandle;
pub use task::{TaskPrefs, TaskProcessErr, TaskProcessor};
