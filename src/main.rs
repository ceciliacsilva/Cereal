mod application;
mod database;
mod messages;
mod operations;
mod repository;
mod runtime;

use std::time::Duration;

use actix::prelude::*;
use application::single_repository_transaction;
use operations::Operation;
use repository::Repository;

use crate::{application::{indep_repository_transaction, coord_repository_transaction}, runtime::Runtime};

#[actix_rt::main]
async fn main() {
    let mut runtime = Runtime::new();

    let customer = Repository::new("customer".to_string()).start();
    let operations = vec![
        Operation::Create(1, Box::new(Operation::Value((30, 0)))),
        Operation::Create(2, Box::new(Operation::Value((100, 20)))),
        Operation::Create(3, Box::new(Operation::Value((1, 2)))),
    ];

    let cust = single_repository_transaction(customer.clone(), operations, &mut runtime).await;
    println!("customer single transaction: {:?}", cust);

    let prod = Repository::new("product".to_string()).start();
    let operations = vec![
        Operation::Create(0, Box::new(Operation::Value((4, 3)))),
        Operation::Create(1, Box::new(Operation::Value((5, 6)))),
        Operation::Create(2, Box::new(Operation::Value((8, 2)))),
    ];
    let resp = single_repository_transaction(prod.clone(), operations, &mut runtime).await;
    println!("prod single transaction: {:?}", resp);

    let operations = vec![
        Operation::Read(1)
    ];

    // FIXME: this should get different operations for each repository.
    let res = coord_repository_transaction(vec![customer, prod], operations, &mut runtime).await;
    println!("vote order: {:?}", res);

    let _ = actix_rt::time::sleep(Duration::from_secs(2)).await;
}
