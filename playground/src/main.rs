use std::{
    error::Error,
    panic::{AssertUnwindSafe, RefUnwindSafe},
    sync::Arc,
    time::Duration,
};

use ::coordinator::{Coordinator, JoinHandle};
use coordinator::coordinator;
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
            size: self.size,
            weight: self.weight,
            bounciness: self.bounciness,
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

#[coordinator]
pub trait CatFamily<I>
where
    I: InteractableObject + RefUnwindSafe,
{
    fn locate_object(obj: ArcMut<I>) -> Option<[f32; 3]>;
    fn upgrade<O: InteractableObject>(obj: ArcMut<I>, material: O);
    fn meow() -> bool;
    fn meow_repeatedly(times: usize)
    where
        Self: Send,
    {
        async move {
            for _ in 0..times {
                self.meow().await;
            }
        }
    }
}

pub struct RobotCat {
    name: String,
}

impl RobotCat {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

// Instead of implementing the [`TaskProcessor`]  trait, we implement the trait generated by
// `#[coordinator]` instead, this way we don't have to enum dispatch ourself. The trait name
// will always be `[Name]Processor`
impl<I> CatFamilyProcessor<I> for RobotCat
where
    I: InteractableObject + RefUnwindSafe + Send + Sync + 'static,
{
    async fn locate_object(&mut self, obj: ArcMut<I>) -> Option<[f32; 3]> {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let obj = obj.lock().await;
        let x_coord = obj.size()[0] + 2.3;
        let y_coord = obj.size()[1] + 3.4;
        let z_coord = obj.size()[2] + 4.5;
        Some([x_coord, y_coord, z_coord])
    }

    async fn upgrade<O: InteractableObject>(&mut self, obj: ArcMut<I>, material: O) {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let mut obj = obj.lock().await;
        let new_weight = obj.weight() + material.weight();
        obj.set_weight(new_weight);
    }

    async fn meow(&mut self) -> bool {
        tokio::time::sleep(Duration::from_secs(3)).await;
        println!("Robot cat `{}` say meow!", self.name);
        true
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

impl<I> CatFamilyProcessor<I> for DomesticatedCat
where
    I: InteractableObject + RefUnwindSafe + Send + Sync + 'static,
{
    async fn locate_object(&mut self, obj: ArcMut<I>) -> Option<[f32; 3]> {
        let wait_time = 3000u64.saturating_sub(self.exp as u64);
        tokio::time::sleep(Duration::from_millis(wait_time)).await;
        let obj = obj.lock().await;

        let x_coord = obj.size()[0] + 2.3;
        let y_coord = obj.size()[1] + 3.4;
        let z_coord = obj.size()[2] + 4.5;
        self.exp += 10;
        println!("Cat {} has gained 10 exp!", self.name);
        Some([x_coord, y_coord, z_coord])
    }

    async fn upgrade<O: InteractableObject>(&mut self, obj: ArcMut<I>, material: O) {
        let wait_time = 3000u64.saturating_sub(self.exp as u64);
        tokio::time::sleep(Duration::from_millis(wait_time)).await;
        let mut obj = obj.lock().await;
        let new_weight = obj.weight() + material.weight();
        obj.set_weight(new_weight);
        self.exp += 10;
        println!("Cat {} has gained 10 exp!", self.name);
    }

    async fn meow(&mut self) -> bool {
        let wait_time = 3000u64.saturating_sub(self.exp as u64);
        tokio::time::sleep(Duration::from_millis(wait_time)).await;
        println!("Robot cat `{}` say meow!", self.name);
        self.exp += 10;
        println!("Cat {} has gained 10 exp!", self.name);
        true
    }
}

#[tokio::main]
async fn main() {
    // The `CatFamily` struct is generated automatically, with `From<Coordinator>` impl
    // so you can convert any `Coordinator` into it using `into()`
    let cat_family: CatFamily<Ball, Crystal, &str> = Coordinator::new(3).into();
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
                .any()
                .locate_object(balls.clone())
                .await?
                .join()
                .await?;

            let Some(pos) = pos else {
                println!("Cat {} cannot find the object!", cat);
                return Ok(());
            };

            println!("Cat {} has found the ball at {:?}", cat, pos);

            let (_, cat) = cat_family
                .prefer(&cat)
                .upgrade(balls.clone(), crystal)
                .await?
                .join()
                .await?;

            println!(
                "Cat {} has upgrade ball to {}",
                cat,
                balls.0.lock().await.weight
            );

            // We don't care about the result here so no need to join
            cat_family.require(&cat).meow_repeatedly(3).await?;
            Ok::<(), Box<dyn Error + Send + Sync + 'static>>(())
        });
    }

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
