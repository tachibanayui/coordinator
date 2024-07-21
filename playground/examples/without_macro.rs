use core::fmt;
use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
    panic::AssertUnwindSafe,
    sync::Arc,
    time::Duration,
};

use ::coordinator::{Coordinator, JoinHandle, TaskPrefs, TaskProcessor};
use tokio::sync::Mutex;

pub trait InteractableObject {
    fn size(&self) -> [f32; 3];
    fn weight(&self) -> f32;
    fn set_weight(&mut self, val: f32);
}

#[derive(Debug)]
pub struct Ball {
    size: [f32; 3],
    weight: f32,
    bounciness: f32,
}

impl Clone for Ball {
    fn clone(&self) -> Self {
        Self {
            size: self.size.clone(),
            weight: self.weight.clone(),
            bounciness: self.bounciness.clone(),
        }
    }
}

impl InteractableObject for Ball {
    fn size(&self) -> [f32; 3] {
        self.size
    }

    fn weight(&self) -> f32 {
        self.weight
    }

    fn set_weight(&mut self, val: f32) {
        self.weight = val;
    }
}

#[derive(Debug)]
pub struct Crystal {
    size: [f32; 3],
    weight: f32,
    #[allow(dead_code)]
    purity: f32,
}

impl InteractableObject for Crystal {
    fn size(&self) -> [f32; 3] {
        self.size
    }

    fn weight(&self) -> f32 {
        self.weight
    }

    fn set_weight(&mut self, val: f32) {
        self.weight = val;
    }
}

// Type alias for not having to type out this long type every time we use it
type ArcMut<T> = Arc<AssertUnwindSafe<Mutex<T>>>;

#[derive(Debug, Clone)]
pub enum CatFamilyInput<I, O> {
    LocateObject(ArcMut<I>),
    Upgrade(ArcMut<I>, O),
    Meow(),
    MeowRepeatedly(usize),
}

impl<I, O> Error for CatFamilyInput<I, O> where Self: Debug {}

impl<I, O> Display for CatFamilyInput<I, O>
where
    Self: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self, f)
    }
}

#[derive(Debug)]
pub enum CatFamilyOutput {
    LocateObject(Option<[f32; 3]>),
    Upgrade(()),
    Meow(bool),
    MeowRepeatedly(()),
}

pub struct RobotCat {
    name: String,
}

impl RobotCat {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl<I, O> TaskProcessor<CatFamilyInput<I, O>> for RobotCat
where
    I: InteractableObject + Send,
    O: InteractableObject + Send,
{
    type Output = CatFamilyOutput;

    async fn do_work(&mut self, task: CatFamilyInput<I, O>) -> Self::Output {
        match task {
            CatFamilyInput::LocateObject(obj) => {
                tokio::time::sleep(Duration::from_secs(3)).await;
                let obj = obj.lock().await;
                let x_coord = obj.size()[0] + 2.3;
                let y_coord = obj.size()[1] + 3.4;
                let z_coord = obj.size()[2] + 4.5;
                let rs = Some([x_coord, y_coord, z_coord]);
                CatFamilyOutput::LocateObject(rs)
            }
            CatFamilyInput::Upgrade(obj, material) => {
                tokio::time::sleep(Duration::from_secs(3)).await;
                let mut obj = obj.lock().await;
                let new_weight = obj.weight() + material.weight();
                obj.set_weight(new_weight);
                CatFamilyOutput::Upgrade(())
            }
            CatFamilyInput::Meow() => {
                tokio::time::sleep(Duration::from_secs(3)).await;
                println!("Robot cat `{}` say meow!", self.name);
                CatFamilyOutput::Meow(true)
            }
            CatFamilyInput::MeowRepeatedly(times) => {
                // There's no easy way to provide default implementation without creating a trait
                for _ in 0..times {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    println!("Robot cat `{}` say meow!", self.name);
                }
                CatFamilyOutput::MeowRepeatedly(())
            }
        }
    }
}

pub struct DomesticatedCat {
    name: String,
    exp: usize,
}

impl DomesticatedCat {
    pub fn new(name: String) -> Self {
        Self { name, exp: 0 }
    }
}

impl<I, O> TaskProcessor<CatFamilyInput<I, O>> for DomesticatedCat
where
    I: InteractableObject + Send,
    O: InteractableObject + Send,
{
    type Output = CatFamilyOutput;

    async fn do_work(&mut self, task: CatFamilyInput<I, O>) -> Self::Output {
        match task {
            CatFamilyInput::LocateObject(obj) => {
                let wait_time = 3000u64.checked_sub(self.exp as u64).unwrap_or(0);
                tokio::time::sleep(Duration::from_millis(wait_time)).await;
                let obj = obj.lock().await;
                let x_coord = obj.size()[0] + 2.3;
                let y_coord = obj.size()[1] + 3.4;
                let z_coord = obj.size()[2] + 4.5;
                let rs = Some([x_coord, y_coord, z_coord]);
                println!("Cat {} has gained 10 exp! Total: {}", self.name, self.exp);
                CatFamilyOutput::LocateObject(rs)
            }
            CatFamilyInput::Upgrade(obj, material) => {
                let wait_time = 3000u64.checked_sub(self.exp as u64).unwrap_or(0);
                tokio::time::sleep(Duration::from_millis(wait_time)).await;
                let mut obj = obj.lock().await;
                let new_weight = obj.weight() + material.weight();
                obj.set_weight(new_weight);
                println!("Cat {} has gained 10 exp! Total: {}", self.name, self.exp);
                CatFamilyOutput::Upgrade(())
            }
            CatFamilyInput::Meow() => {
                let wait_time = 3000u64.checked_sub(self.exp as u64).unwrap_or(0);
                tokio::time::sleep(Duration::from_millis(wait_time)).await;
                println!("Robot cat `{}` say meow!", self.name);
                println!("Cat {} has gained 10 exp! Total: {}", self.name, self.exp);
                CatFamilyOutput::Meow(true)
            }
            CatFamilyInput::MeowRepeatedly(times) => {
                // There's no easy way to provide default implementation without creating a trait
                for _ in 0..times {
                    let wait_time = 3000u64.checked_sub(self.exp as u64).unwrap_or(0);
                    tokio::time::sleep(Duration::from_millis(wait_time)).await;
                    println!("Robot cat `{}` say meow!", self.name);
                    println!("Cat {} has gained 10 exp! Total: {}", self.name, self.exp);
                }
                CatFamilyOutput::MeowRepeatedly(())
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let cat_family: Coordinator<CatFamilyInput<Ball, Crystal>, CatFamilyOutput, &str> =
        Coordinator::new(3);
    cat_family
        .add_worker("Maple", DomesticatedCat::new("Maple".to_owned()))
        .await;

    cat_family
        .add_worker("Suduko", DomesticatedCat::new("Suduko".to_owned()))
        .await;

    cat_family
        .add_worker("Autocat", RobotCat::new("Autocat".to_owned()))
        .await;

    cat_family
        .add_worker("Oktocat", RobotCat::new("Oktocat".to_owned()))
        .await;

    for _ in 0..10 {
        // Cloning here is only cloning the `Arc` under the hood, not creating a new `Coordinator`
        let cat_family = cat_family.clone();
        tokio::spawn(async move {
            let balls = Arc::new(AssertUnwindSafe(Mutex::new(Ball {
                size: [2.2, 3.3, 4.4],
                weight: 5.9,
                bounciness: 10.2,
            })));

            let crystal = Crystal {
                size: [5.2, 3.1, 6.4],
                weight: 15.9,
                purity: 0.9,
            };

            let (pos, cat) = cat_family
                .run(CatFamilyInput::LocateObject(balls.clone()), TaskPrefs::Any)
                .await?
                .join()
                .await?;

            let CatFamilyOutput::LocateObject(Some(pos)) = pos else {
                println!("Cat {} cannot find the object!", cat);
                return Ok(());
            };

            println!("Cat {} has found the ball at {:?}", cat, pos);

            let (_, cat) = cat_family
                .run(
                    CatFamilyInput::Upgrade(balls.clone(), crystal),
                    TaskPrefs::Preferred(cat),
                )
                .await?
                .join()
                .await?;

            println!(
                "Cat {} has upgrade ball to {}",
                cat,
                balls.0.lock().await.weight
            );

            // We don't care about the result here so no need to join
            cat_family
                .run(CatFamilyInput::MeowRepeatedly(3), TaskPrefs::Required(cat))
                .await?;
            return Ok::<(), Box<dyn Error + Send + Sync + 'static>>(());
        });
    }

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
