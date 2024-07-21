use std::sync::{Arc, Weak};

use event_listener::{listener, Event};
use futures::{lock::Mutex, Future};

type JoinInner<T> = (Event, Mutex<Option<T>>);

#[derive(Debug)]
pub struct JoinSignal<T> {
    signal: Weak<JoinInner<T>>,
}

impl<T> JoinSignal<T> {
    /// Return back the owned object if JoinHandle is already consummed
    pub async fn set(self, val: T) -> Option<T> {
        let Some(data) = self.signal.upgrade() else {
            return Some(val);
        };

        data.1.lock().await.replace(val);
        data.0.notify(usize::MAX);
        return None;
    }
}

pub trait JoinHandle {
    type Output;
    fn join(self) -> impl Future<Output = Self::Output> + Send;
    fn map<O>(self, tf: impl FnOnce(Self::Output) -> O + Send) -> impl JoinHandle<Output = O>
    where
        Self: Sized + Send,
    {
        MappedJoinHandle { handle: self, tf }
    }
}

pub struct ImplJoinHandle<T> {
    signal: Arc<JoinInner<T>>,
}

impl<T: Send> JoinHandle for ImplJoinHandle<T> {
    type Output = T;

    async fn join(self) -> T {
        listener!(self.signal.0 => lis);
        lis.await;

        let val = self
            .signal
            .1
            .lock()
            .await
            .take()
            .expect("Value must be set before notify event!");

        return val;
    }
}

pub struct MappedJoinHandle<H, F, O>
where
    H: JoinHandle,
    F: FnOnce(H::Output) -> O,
{
    handle: H,
    tf: F,
}

impl<H, F, O> JoinHandle for MappedJoinHandle<H, F, O>
where
    H: JoinHandle + Send,
    F: FnOnce(H::Output) -> O + Send,
{
    type Output = O;

    async fn join(self) -> Self::Output {
        let rs = self.handle.join().await;
        let tf = self.tf;
        tf(rs)
    }
}

pub fn signal<T>() -> (JoinSignal<T>, ImplJoinHandle<T>) {
    let container = Mutex::new(None);
    let ev = Event::new();
    let arc = Arc::new((ev, container));
    let signal = JoinSignal {
        signal: Arc::downgrade(&arc),
    };

    let handle = ImplJoinHandle { signal: arc };

    (signal, handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn sig_test() {
        let (signal, handle) = signal();
        dbg!("WC");

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            signal.set("Yooo").await;
        });

        println!("Waiting for signal");
        let val = handle.join().await;
        println!("retured {val}");
    }
}
