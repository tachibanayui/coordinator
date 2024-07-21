use core::panic;
use std::{
    error::Error,
    fmt::{Debug, Display},
    panic::RefUnwindSafe,
    time::{Duration, Instant},
};

use ::coordinator::{Coordinator, JoinHandle};
use coordinator::coordinator;

#[coordinator]
pub trait MyBalancer<T: ::std::fmt::Debug> {
    fn wait(duration: Duration) -> Instant;
    fn ping() -> Duration;
    fn post_msg<M: ToString>(msg: M);
    fn broadcast<BI: Display>(item: BI);
    fn drain_broadcast<I: Display + RefUnwindSafe>(vec: Vec<I>) -> usize
    where
        Self: Send,
    {
        async move {
            let mut num = 0;
            for i in vec.into_iter() {
                self.broadcast(i).await;
                num += 1;
            }

            num
        }
    }
}

struct Worker(String);

impl<T: Debug> MyBalancerProcessor<T> for Worker {
    async fn wait(&mut self, duration: Duration) -> Instant {
        tokio::time::sleep(duration).await;
        println!("{} has sleep for {:?}", self.0, duration);
        Instant::now()
    }

    async fn ping(&mut self) -> Duration {
        println!("{} pinging...", self.0);
        Duration::from_secs(10)
    }

    async fn post_msg<M: ToString + Send>(&mut self, msg: M) {
        println!("{} pinging with {}...", self.0, msg.to_string());
    }

    async fn broadcast<BI: Display>(&mut self, item: BI) {
        println!("{} broadcast with {}...", self.0, item);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let c: MyBalancer<i32, &str, i32, i32, &str> = Coordinator::new(10).into();
    c.add_worker("F0", Worker(String::from("F0"))).await;
    let (time, worker) = c.any().ping().await?.join().await?;
    println!("{} pinged, wait time {:?}", worker, time);
    c.require(&worker).wait(time).await?.join().await?;
    c.require(&worker).ping().await?.join().await?;

    let ident = c.any().post_msg("Hello").await?.join().await?.1;

    // No need to join if you dont care about the result
    c.prefer(&ident).post_msg("Goodbye from ident").await?;
    c.prefer(&ident)
        .drain_broadcast(vec![1, 2, 3, 4, 5])
        .await?;

    Ok(())
}
