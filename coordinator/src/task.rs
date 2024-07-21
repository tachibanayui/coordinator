use std::{error::Error, fmt::Display, future::Future};

#[derive(Debug)]
pub struct TaskProcessErr;

impl Error for TaskProcessErr {}

impl Display for TaskProcessErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Error occurred while processing the current task!")
    }
}

#[derive(Debug)]
pub enum TaskPrefs<T> {
    Any,
    Preferred(T),
    Required(T),
}

pub trait TaskProcessor<I> {
    type Output;
    fn do_work(&mut self, task: I) -> impl Future<Output = Self::Output> + Send;
}

impl<F, I, O, Fut> TaskProcessor<I> for F
where
    F: FnMut(I) -> Fut,
    Fut: Future<Output = O> + Send,
{
    type Output = O;

    fn do_work(&mut self, task: I) -> impl Future<Output = Self::Output> + Send {
        self(task)
    }
}
