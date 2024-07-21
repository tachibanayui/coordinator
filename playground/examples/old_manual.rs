use std::{
    error::Error,
    fmt::{Debug, Display},
    panic::RefUnwindSafe,
    time::{Duration, Instant},
};

use coordinator::{Coordinator, JoinHandle, TaskPrefs, TaskProcessor};

#[derive(Debug)]
pub enum MyBalancerInput<M, BI, I> {
    Wait(Duration),
    Ping(),
    PostMsg(M),
    Broadcast(BI),
    DrainBroadcast(Vec<I>),
}

impl<M, BI, I> Error for MyBalancerInput<M, BI, I> where MyBalancerInput<M, BI, I>: Debug {}

impl<M, BI, I> Display for MyBalancerInput<M, BI, I>
where
    MyBalancerInput<M, BI, I>: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self, f)
    }
}

#[derive(Debug)]
pub enum MyBalancerOutput {
    Wait(Instant),
    Ping(Duration),
    PostMsg(()),
    Broadcast(()),
    DrainBroadcast(usize),
}

struct Worker(String);

fn broadcast<T: Display>(s: &mut Worker, item: T) -> MyBalancerOutput {
    println!("{} broadcast with {}...", s.0, item);
    MyBalancerOutput::Broadcast(())
}

impl<M, BI, I> TaskProcessor<MyBalancerInput<M, BI, I>> for Worker
where
    M: Send + ToString,
    BI: Send + Display,
    I: Send + Display + RefUnwindSafe,
{
    type Output = MyBalancerOutput;

    async fn do_work(&mut self, task: MyBalancerInput<M, BI, I>) -> Self::Output {
        match task {
            MyBalancerInput::Wait(duration) => {
                tokio::time::sleep(duration).await;
                println!("{} has sleep for {:?}", self.0, duration);
                MyBalancerOutput::Wait(Instant::now())
            }
            MyBalancerInput::Ping() => {
                println!("{} pinging...", self.0);
                MyBalancerOutput::Ping(Duration::from_secs(10))
            }
            MyBalancerInput::PostMsg(msg) => {
                println!("{} pinging with {}...", self.0, msg.to_string());
                MyBalancerOutput::PostMsg(())
            }
            MyBalancerInput::Broadcast(item) => broadcast(self, item),
            MyBalancerInput::DrainBroadcast(vec) => {
                let mut num = 0;
                for i in vec.into_iter() {
                    broadcast(self, i);
                    num += 1;
                }

                return MyBalancerOutput::DrainBroadcast(num);
            }
        }
    }
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let c: Coordinator<MyBalancerInput<&str, i32, i32>, MyBalancerOutput, &str> =
        Coordinator::new(10);
    c.add_worker("F0", Worker("F0".to_owned())).await;

    let (output, worker) = c
        .run(MyBalancerInput::Ping(), TaskPrefs::Any)
        .await?
        .join()
        .await?;

    let time = match output {
        MyBalancerOutput::Ping(x) => x,
        _ => panic!(),
    };

    println!("{} pinged, wait time {:?}", worker, time);

    c.run(MyBalancerInput::Wait(time), TaskPrefs::Required(worker))
        .await?
        .join()
        .await?;

    c.run(MyBalancerInput::Ping(), TaskPrefs::Required(worker))
        .await?
        .join()
        .await?;

    let ident = c
        .run(MyBalancerInput::PostMsg("Hello"), TaskPrefs::Any)
        .await?
        .join()
        .await?
        .1;

    c.run(
        MyBalancerInput::PostMsg("Goodbye from ident"),
        TaskPrefs::Preferred(ident),
    )
    .await?;

    c.run(
        MyBalancerInput::DrainBroadcast(vec![1, 2, 3, 4, 5]),
        TaskPrefs::Preferred(ident),
    )
    .await?;

    Ok(())
}
