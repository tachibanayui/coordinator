use std::{
    fmt::Debug,
    hash::Hash,
    panic::UnwindSafe,
    sync::{atomic::Ordering, Arc, OnceLock},
};

use crate::{
    inner::CoordinatorInner,
    signal::ImplJoinHandle,
    task::{TaskPrefs, TaskProcessErr, TaskProcessor},
};

#[derive(Debug)]
pub struct Coordinator<TIn, TOut, TIdent>(
    OnceLock<Arc<CoordinatorInner<TIn, TOut, TIdent>>>,
    usize,
)
where
    TIdent: Hash + Eq + Ord + Debug + Clone + Send + Sync + 'static,
    TIn: Send + UnwindSafe + 'static,
    TOut: Send + UnwindSafe + 'static;

impl<TIn, TOut, TIdent> Coordinator<TIn, TOut, TIdent>
where
    TIdent: Hash + Eq + Ord + Debug + Clone + Send + Sync + 'static,
    TIn: Send + UnwindSafe + 'static,
    TOut: Send + UnwindSafe + 'static,
{
    fn get_inner(&self) -> &Arc<CoordinatorInner<TIn, TOut, TIdent>> {
        self.0.get_or_init(|| CoordinatorInner::new(self.1))
    }

    /// Create a new coordinator
    ///
    /// # Arguments
    /// * `queue_threshold` - Specify the threshold when the number of pending tasks in a worker queue
    /// prevents [`TaskPrefs::Preferred`] from adding new task to this queue
    pub fn new(queue_threshold: usize) -> Self {
        Self(OnceLock::new(), queue_threshold)
    }

    pub async fn run(
        &self,
        input: TIn,
        prefs: TaskPrefs<TIdent>,
    ) -> Result<ImplJoinHandle<Result<(TOut, TIdent), TaskProcessErr>>, TIn> {
        self.get_inner().run(input, prefs).await
    }

    pub async fn add_worker<TP>(&self, ident: TIdent, worker: TP) -> bool
    where
        TP: TaskProcessor<TIn, Output = TOut> + UnwindSafe + Send + Sync + 'static,
        TIn: UnwindSafe,
        TOut: UnwindSafe,
        TIdent: Debug,
    {
        let arc = self.get_inner().clone();
        arc.add_worker(worker, ident).await
    }

    pub async fn remove_worker(&self, ident: &TIdent) -> bool {
        self.get_inner().remove_worker(ident).await
    }
}

impl<TIn, TOut, TIdent> Drop for Coordinator<TIn, TOut, TIdent>
where
    TIdent: Hash + Eq + Ord + Debug + Clone + Send + Sync + 'static,
    TIn: Send + UnwindSafe + 'static,
    TOut: Send + UnwindSafe + 'static,
{
    fn drop(&mut self) {
        let prev = self
            .get_inner()
            .consumer_count
            .fetch_sub(1, Ordering::Release);
        if prev == 1 {
            self.get_inner().event_coordinator_abort.notify(usize::MAX);
        }
    }
}

impl<TIn, TOut, TIdent> Clone for Coordinator<TIn, TOut, TIdent>
where
    TIdent: Hash + Eq + Ord + Debug + Clone + Send + Sync + 'static,
    TIn: Send + UnwindSafe + 'static,
    TOut: Send + UnwindSafe + 'static,
{
    /// Create another consumer of this [`Coordinator`]. Note that this only increase the reference count
    /// on a internal structure, not creating a new coordinator
    fn clone(&self) -> Self {
        self.get_inner()
            .consumer_count
            .fetch_add(1, Ordering::Release);
        Self(self.0.clone(), self.1)
    }
}
