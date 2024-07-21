use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    hash::Hash,
    panic::{AssertUnwindSafe, UnwindSafe},
    pin::pin,
    ptr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use crate::{
    signal::{signal, ImplJoinHandle, JoinSignal},
    task::{TaskPrefs, TaskProcessErr, TaskProcessor},
};
use async_channel::{unbounded, Receiver, Sender};
use concurrent_queue::ConcurrentQueue;
use derivative::Derivative;
use event_listener::{listener, Event};
use futures::{
    future::{select, Either},
    lock::Mutex,
    FutureExt,
};
use tokio::sync::RwLock;
use tracing::{debug, error};

fn ewma(old: u64, new: u64, alpha: f64) -> u64 {
    let old = old as f64;
    let new = new as f64;
    let rs = alpha * new + (1.0 - alpha) * old;
    rs as u64
}

#[derive(Debug)]
pub(crate) struct Task<I, P, R, TIdent> {
    input: Option<I>, // Option None means this task has been processed
    prefs: TaskPrefs<P>,
    signal: JoinSignal<Result<(R, TIdent), TaskProcessErr>>,
    task_id: u64,
}

impl<I, P, R, TIdent> Task<I, P, R, TIdent> {
    pub(crate) fn new(
        input: I,
        prefs: TaskPrefs<P>,
        task_id: u64,
    ) -> (Self, ImplJoinHandle<Result<(R, TIdent), TaskProcessErr>>) {
        let (sig, hdl) = signal();
        let task = Self {
            input: Some(input),
            prefs,
            signal: sig,
            task_id,
        };

        (task, hdl)
    }
}

#[derive(Derivative, Clone)]
#[derivative(PartialEq, Eq, PartialOrd, Ord)]
struct WorkerRank<T>(u64, T);

#[derive(Debug)]
pub struct TaskWorker<TIdent, TIn, TOut> {
    ident: TIdent,
    queue: Sender<Task<TIn, TIdent, TOut, TIdent>>,
    /// Arc<(Cancel, AvgRespTime)>
    task_handle: Arc<(Event, AtomicU64, f64)>,
}

impl<TIdent: Debug, TIn, TOut> TaskWorker<TIdent, TIn, TOut> {
    #[tracing::instrument(skip(self, task))]
    async fn queue_task(&self, task: Task<TIn, TIdent, TOut, TIdent>) -> u64 {
        let id = task.task_id;
        if let Err(_) = self.queue.send(task).await {
            // Log queue error
        }

        let ql = self.queue.len() as u64;
        let ewma = self.task_handle.1.load(Ordering::Acquire);
        let tps = (ql + 1) * ewma;
        debug!(
            "Task id {} enqueued! queue len: {} priority score for `{:?}`: {}!",
            id,
            self.queue.len(),
            self.ident,
            tps
        );
        tps
    }
}

// Note: Wrap in Arc<Balancer> to share the balancer
#[derive(Debug)]
pub struct CoordinatorInner<TIn, TOut, TIdent> {
    buf: ConcurrentQueue<Task<TIn, TIdent, TOut, TIdent>>,
    workers: RwLock<HashMap<TIdent, TaskWorker<TIdent, TIn, TOut>>>,
    queue_threshold: usize,
    task_id_gen: AtomicU64,

    event_update_queue: Event,
    // update_queue_processed: AtomicBool,
    pub(crate) consumer_count: AtomicU64,
    pub(crate) event_coordinator_abort: Event,
    task_priority: Mutex<BTreeMap<WorkerRank<TIdent>, TIdent>>,
}

impl<TIn, TOut, TIdent> CoordinatorInner<TIn, TOut, TIdent>
where
    TIdent: Hash + Eq + Ord + Debug + Clone + Send + Sync + 'static,
    TIn: Send + UnwindSafe + 'static,
    TOut: Send + UnwindSafe + 'static,
{
    fn create_task(
        &self,
        input: TIn,
        prefs: TaskPrefs<TIdent>,
    ) -> (
        Task<TIn, TIdent, TOut, TIdent>,
        ImplJoinHandle<Result<(TOut, TIdent), TaskProcessErr>>,
    ) {
        let id = self.task_id_gen.fetch_add(1, Ordering::Relaxed);
        Task::new(input, prefs, id)
    }

    pub(crate) fn new(queue_threshold: usize) -> Arc<Self> {
        let slf = Self {
            buf: ConcurrentQueue::unbounded(),
            workers: RwLock::new(HashMap::new()),
            queue_threshold,
            task_id_gen: AtomicU64::new(0),
            event_update_queue: Event::new(),
            // update_queue_processed: AtomicBool::new(true),
            consumer_count: AtomicU64::new(0),
            event_coordinator_abort: Event::new(),
            task_priority: Mutex::new(BTreeMap::new()),
        };

        let slf = Arc::new(slf);
        tokio::spawn(slf.clone().coordinator_loop());
        slf
    }

    pub async fn run(
        &self,
        input: TIn,
        prefs: TaskPrefs<TIdent>,
    ) -> Result<ImplJoinHandle<Result<(TOut, TIdent), TaskProcessErr>>, TIn> {
        let (task, handle) = self.create_task(input, prefs);
        self.buf
            .push(task)
            .map_err(|x| x.into_inner().input.unwrap())?;

        self.event_update_queue.notify(usize::MAX);
        Ok(handle)
    }

    pub async fn add_worker<TP>(self: Arc<Self>, worker: TP, ident: TIdent) -> bool
    where
        TP: TaskProcessor<TIn, Output = TOut> + UnwindSafe + Send + Sync + 'static,
    {
        let (send, recv) = unbounded();
        let handle = Arc::new((Event::new(), AtomicU64::new(1), 0.5)); // TODO: Config or constant

        let worker_info = TaskWorker {
            ident: ident.clone(),
            queue: send,
            task_handle: handle,
        };

        let prev = {
            let mut workers = self.workers.write().await;
            workers.insert(ident.clone(), worker_info)
        };

        tokio::spawn(self.task_processor_loop(worker, ident, recv));
        prev.is_some()
    }

    pub async fn remove_worker(&self, ident: &TIdent) -> bool {
        let mut workers = self.workers.write().await;
        let Some(info) = workers.remove(ident) else {
            return false;
        };

        info.task_handle.0.notify(usize::MAX);
        true
    }

    #[tracing::instrument(target = "lb", skip(self))]
    async fn coordinator_loop(self: Arc<Self>) {
        let cancel_ev = &self.event_coordinator_abort;
        listener!(cancel_ev => lis);

        let update_ev = &self.event_update_queue;

        loop {
            self.process_queue().await;
            listener!(update_ev => update_lis);
            debug!("Waiting...");
            let Either::Left((_, lis_fut)) = select(update_lis, lis).await else {
                return;
            };
            debug!("Notification received!");
            lis = lis_fut;
        }
    }

    #[tracing::instrument(target = "lb", skip(self))]
    async fn process_queue(&self) {
        let update_rank = |priority: &mut BTreeMap<WorkerRank<TIdent>, TIdent>, tps, pref| {
            let mut iter = priority.iter().skip_while(|x| x.1 != &pref);
            let Some(cur_key) = iter.next().map(|x| x.0.clone()) else {
                return;
            };

            if let Some(next) = iter.next() {
                if tps > next.0 .0 {
                    priority.remove(&cur_key);
                    priority.insert(WorkerRank(tps, pref.clone()), pref);
                }
            }
        };

        let update_rank_any =
            |priority: &mut BTreeMap<WorkerRank<TIdent>, TIdent>, tps, ident: TIdent| {
                let Some((nps, _)) = priority.first_key_value() else {
                    return ident;
                };

                if nps.0 < tps {
                    priority.insert(WorkerRank(tps, ident.clone()), ident);
                    priority.pop_first().unwrap().1
                } else {
                    ident
                }
            };

        let mut priority = self.task_priority.lock().await;
        priority.clear();

        let workers = self.workers.read().await;
        for (ident, info) in workers.iter() {
            let ql = info.queue.len() as u64;
            let ewma = info.task_handle.1.load(Ordering::Acquire);
            let score = (ql + 1) * ewma;
            priority.insert(WorkerRank(score, ident.clone()), ident.clone());
        }

        let Some((_, mut ident)) = priority.pop_first() else {
            error!("Load balancer currently has no worker!");
            return;
        };

        let mut any_worker_info = workers.get(&ident).unwrap();
        debug!("Start sending task to task processor");
        for task in self.buf.try_iter() {
            use TaskPrefs::*;
            match &task.prefs {
                Any => {
                    let tps = any_worker_info.queue_task(task).await;
                    ident = update_rank_any(&mut priority, tps, ident);
                    any_worker_info = workers.get(&ident).unwrap();
                }
                Preferred(pref) => {
                    let worker = workers
                        .get(pref)
                        .filter(|x| x.queue.len() < self.queue_threshold)
                        .unwrap_or(any_worker_info);
                    let pref = pref.clone();
                    let tps = worker.queue_task(task).await;
                    if ptr::eq(worker, any_worker_info) {
                        ident = update_rank_any(&mut priority, tps, ident);
                        any_worker_info = workers.get(&ident).unwrap();
                    } else {
                        update_rank(&mut priority, tps, pref);
                    }
                }
                Required(req) => {
                    let Some(worker) = workers.get(req) else {
                        // Receive a task that no task worker can process, dropping
                        continue;
                    };

                    let req = req.clone();
                    let tps = worker.queue_task(task).await;
                    update_rank(&mut priority, tps, req);
                }
            }

            if any_worker_info.queue.len() >= self.queue_threshold {
                let Some((_, wr)) = priority.pop_first() else {
                    break;
                };

                ident = wr;
                any_worker_info = workers.get(&ident).unwrap();
            }
        }

        let item_left = self.buf.len();
        if item_left > 0 {
            debug!("Process queue done! Work item left: {}", item_left);
        }
    }

    #[tracing::instrument(skip(self, processor, queue))]
    async fn task_processor_loop<TP>(
        self: Arc<Self>,
        mut processor: TP,
        ident: TIdent,
        queue: Receiver<Task<TIn, TIdent, TOut, TIdent>>,
    ) where
        TP: TaskProcessor<TIn, Output = TOut> + UnwindSafe,
    {
        let workers = self.workers.read().await;
        let Some(task_handle) = workers.get(&ident).map(|x| x.task_handle.clone()) else {
            return;
        };
        drop(workers);

        let (cancel, avg, ewma_alpha) = &*task_handle;
        listener!(cancel => lis);

        loop {
            let pin_recv = pin!(queue.recv());
            debug!("Waiting...");
            let Either::Left((Ok(task), lis_fut)) = select(pin_recv, lis).await else {
                return;
            };
            debug!("Received!");

            lis = lis_fut;

            if let Some(input) = task.input {
                let proc = &mut processor;
                let timer = Instant::now();
                let work_rs = AssertUnwindSafe(proc.do_work(input))
                    .catch_unwind()
                    .await
                    .map_err(|_| TaskProcessErr)
                    .map(|x| (x, ident.clone()));
                let elapsed = timer.elapsed().as_millis() as u64;
                let old = avg.load(Ordering::Acquire);
                let new = ewma(old, elapsed, *ewma_alpha);
                debug!(
                    "Task id={} success={}! New ewma val for `{:?}`={}",
                    task.task_id,
                    work_rs.is_ok(),
                    &ident,
                    new
                );
                avg.store(new, Ordering::Release);
                task.signal.set(work_rs).await;
            };

            // self.update_queue_processed.store(false, Ordering::Release);
            self.event_update_queue.notify(usize::MAX);
        }
    }
}
