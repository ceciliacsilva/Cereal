mod application;
mod database;
mod messages;
mod operations;
mod repository;
mod runtime;

use actix::prelude::*;
use operations::Operation;
use repository::Repository;

use crate::{
    application::{single_repository_transaction, indep_repository_transaction},
    operations::{Expr, Statement},
    runtime::Runtime,
};

#[actix_rt::main]
async fn main() -> anyhow::Result<()> {
    let mut runtime = Runtime::new();

    let customer = Repository::new("customer".to_string()).start();
    let operations = vec![
        Operation::Statement(Statement::Create(0, Box::new(Expr::Value((1, 1))))),
        Operation::Statement(Statement::Create(1, Box::new(Expr::Value((2, 2))))),
        Operation::Statement(Statement::Create(2, Box::new(Expr::Value((3, 3))))),
    ];

    let cust = single_repository_transaction(&customer, operations, &mut runtime).await;
    println!("Adding customer info. Result: {:?}", cust);

    let product = Repository::new("product".to_string()).start();
    let operations = vec![
        Operation::Statement(Statement::Create(0, Box::new(Expr::Value((4, 4))))),
        Operation::Statement(Statement::Create(1, Box::new(Expr::Value((5, 5))))),
        Operation::Statement(Statement::Create(2, Box::new(Expr::Value((6, 6))))),
    ];

    let resp = single_repository_transaction(&product, operations, &mut runtime).await;
    println!("Adding product info. Result: {:?}", resp);

    let operations = vec![
        vec![Operation::Expr(Expr::Read(0))],
        vec![Operation::Expr(Expr::Read(0))],
    ];

    let reads = indep_repository_transaction(vec![customer, product], operations, &mut runtime).await;
    println!("reading repositories: {:?}", reads);

    Ok(())
}
