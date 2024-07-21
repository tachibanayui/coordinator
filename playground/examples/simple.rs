use std::time::Duration;

use coordinator::{Coordinator, JoinHandle, TaskPrefs, TaskProcessor};

// Create a worker that sleeps for 1 sec and return a number that double the input
struct Doubler(String);
impl TaskProcessor<i32> for Doubler {
    type Output = i32;
    async fn do_work(&mut self, task: i32) -> Self::Output {
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!("Task {} computed {}", self.0, task * 2);
        task * 2
    }
}

#[tokio::main]
async fn main() {
    // the queue thershold of a single queue, if the number of task item in queue exceeded the thershold
    // any `TaskPref::Preferred(x)` will be processed by a different task processor.
    let queue_len = 3;
    let b = Coordinator::new(queue_len);

    // Add `Doubler` as task processor
    b.add_worker("Doubler 1st", Doubler("Doubler 1st".to_string()))
        .await;

    // Add a closure as a task processor. Any `FnMut` closure can be used as task processor!
    b.add_worker("Doubler 5th", |x| async move { x * 2 }).await;

    // Schedule a task for processing. The task will be polled to completion in the worker future
    // and not the current future. The `join_handle` can be used to retrieve the returned value
    let join_handle = b.run(2, TaskPrefs::Any).await.unwrap();
    println!("Task scheduled!");

    // Do other works.....

    // Wait for the task result
    let rs = join_handle.join().await.unwrap().0;
    println!("Task result: {}", rs);
}
