use coordinator::{Coordinator, JoinHandle, TaskPrefs, TaskProcessor};
use std::time::Duration;
use tracing::{debug, level_filters::LevelFilter};

struct Worker(String);
impl TaskProcessor<i32> for Worker {
    type Output = i32;
    async fn do_work(&mut self, task: i32) -> Self::Output {
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!("Task {} computed {}", self.0, task * 2);
        task * 2
    }
}

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .with_max_level(LevelFilter::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let b = Coordinator::new(3);
    b.add_worker("Worker 1st", Worker("Worker 1st".to_string()))
        .await;
    b.add_worker("Worker 5th", |x| async move { x * 2 }).await;
    debug!("wakldna");
    let _ = b.run(2, TaskPrefs::Any).await.unwrap();
    let x = b.run(4, TaskPrefs::Any).await.unwrap();
    let _ = b.run(6, TaskPrefs::Any).await.unwrap();
    let rs = x.join().await.unwrap();
    dbg!(rs);

    for x in 10..20 {
        b.run(x, TaskPrefs::Preferred("Worker 4th")).await.unwrap();
    }

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
