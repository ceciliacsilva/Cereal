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

use crate::{
    application::{coord_repository_transaction, indep_repository_transaction},
    operations::{Expr, Statement},
    runtime::Runtime,
};

async fn create_customer_product_tables(
    runtime: &mut Runtime,
) -> (Addr<Repository>, Addr<Repository>) {
    let customer = Repository::new("customer".to_string()).start();
    let operations = vec![
        Operation::Statement(Statement::Create(1, Box::new(Expr::Value((1, 1))))),
        Operation::Statement(Statement::Create(2, Box::new(Expr::Value((2, 2))))),
        Operation::Statement(Statement::Create(3, Box::new(Expr::Value((3, 3))))),
    ];

    let cust = single_repository_transaction(customer.clone(), operations, runtime).await;
    println!("Adding customer info. Result: {:?}", cust);

    let prod = Repository::new("product".to_string()).start();
    let operations = vec![
        Operation::Statement(Statement::Create(0, Box::new(Expr::Value((4, 4))))),
        Operation::Statement(Statement::Create(1, Box::new(Expr::Value((5, 5))))),
        Operation::Statement(Statement::Create(2, Box::new(Expr::Value((6, 6))))),
    ];
    let resp = single_repository_transaction(prod.clone(), operations, runtime).await;
    println!("Adding product info. Result: {:?}", resp);

    (customer, prod)
}

async fn test_indep_ok(
    customer: Addr<Repository>,
    product: Addr<Repository>,
    runtime: &mut Runtime,
) {
    let operations = vec![
        vec![Operation::Expr(Expr::Read(1))],
        vec![Operation::Expr(Expr::Read(1))],
    ];

    let res = indep_repository_transaction(vec![customer, product], operations, runtime).await;

    assert!(res.is_ok());
    let _ = res.unwrap().iter().map(|r| assert!(r.is_some()));
    println!("Reading a key that exists on both. Should be ok.");
}

async fn test_indep_not_valid_key_product(
    customer: Addr<Repository>,
    product: Addr<Repository>,
    runtime: &mut Runtime,
) {
    let operations = vec![
        vec![Operation::Expr(Expr::Read(1))],
        vec![Operation::Expr(Expr::Read(4))],
    ];

    let res = indep_repository_transaction(vec![customer, product], operations, runtime).await;
    assert!(res.is_err());
    println!("Read transaction failed because of a primary key violation. Should be err.");
}

async fn test_indep_update_failed_because_of_primary_key_violation(
    customer: Addr<Repository>,
    product: Addr<Repository>,
    runtime: &mut Runtime,
) {
    let operations = vec![
        Operation::Expr(Expr::Read(1)),
    ];

    let cust_before = single_repository_transaction(customer.clone(), operations, runtime).await;

    let operations = vec![
        vec![Operation::Statement(Statement::Update(
            1,
            Box::new(Expr::Value((1000, 1000))),
        ))],
        vec![Operation::Expr(Expr::Read(4))],
    ];

    let res = indep_repository_transaction(vec![customer.clone(), product], operations, runtime).await;
    assert!(res.is_err());

    let operations = vec![
        Operation::Expr(Expr::Read(1)),
    ];

    let cust = single_repository_transaction(customer, operations, runtime).await;

    assert!(cust.is_ok());
    assert_eq!(cust_before.unwrap(), cust.unwrap());
    println!("An `abort` transaction has not effect on a repo");
}

async fn test_coord_ok(
    customer: Addr<Repository>,
    product: Addr<Repository>,
    runtime: &mut Runtime,
) {
    let operations = vec![
        vec![Operation::Statement(Statement::Update(
            1,
            Box::new(Expr::Value((10, 10))),
        ))],
        vec![Operation::Statement(Statement::Update(
            1,
            Box::new(Expr::Value((40, 40))),
        ))],
    ];

    let res = coord_repository_transaction(vec![customer.clone(), product.clone()], operations, runtime).await;
    assert!(res.is_ok());

    let operations = vec![
        Operation::Expr(Expr::Read(1)),
    ];
    let cust = single_repository_transaction(customer, operations, runtime).await;

    let operations = vec![
        Operation::Expr(Expr::Read(1)),
    ];
    let prod = single_repository_transaction(product, operations, runtime).await;

    assert!(cust.is_ok());
    assert!(prod.is_ok());
    assert_eq!(cust.unwrap(), Some((10, 10)));
    assert_eq!(prod.unwrap(), Some((40, 40)));
    println!("Coordinated update succeeded.");
}

async fn test_coord_should_fail(
    customer: Addr<Repository>,
    product: Addr<Repository>,
    runtime: &mut Runtime,
) {
    let operations = vec![
        vec![Operation::Statement(Statement::Update(
            1,
            Box::new(Expr::Value((100, 100))),
        ))],
        vec![Operation::Statement(Statement::Update(
            1,
            Box::new(Expr::Value((400, 400))),
        )),
             // Should fail.
             Operation::Expr(Expr::Read(10)),
        ],
    ];

    let res = coord_repository_transaction(vec![customer.clone(), product.clone()], operations, runtime).await;
    assert!(res.is_err());

    let operations = vec![
        Operation::Expr(Expr::Read(1)),
    ];
    let cust = single_repository_transaction(customer, operations, runtime).await;

    let operations = vec![
        Operation::Expr(Expr::Read(1)),
    ];
    let prod = single_repository_transaction(product, operations, runtime).await;

    assert!(cust.is_ok());
    assert!(prod.is_ok());
    assert_eq!(cust.unwrap(), Some((10, 10)));
    assert_eq!(prod.unwrap(), Some((40, 40)));
    println!("Coordinated fail to update due to primary key violation.");
}

#[actix_rt::main]
async fn main() {
    let mut runtime = Runtime::new();
    let (customer, product) = create_customer_product_tables(&mut runtime).await;

    test_indep_ok(customer.clone(), product.clone(), &mut runtime).await;
    test_indep_not_valid_key_product(customer.clone(), product.clone(), &mut runtime).await;
    test_indep_update_failed_because_of_primary_key_violation(customer.clone(), product.clone(), &mut runtime).await;
    test_coord_ok(customer.clone(), product.clone(), &mut runtime).await;
    test_coord_should_fail(customer, product, &mut runtime).await;

    let _ = actix_rt::time::sleep(Duration::from_secs(2)).await;
}
