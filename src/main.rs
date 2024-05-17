mod application;
mod customer;
mod database;
mod messages;
mod operations;
mod order;
mod product;
mod repository;
mod runtime;

use std::time::Duration;

use actix::prelude::*;
use application::single_repository_transaction;
use customer::Customer;
use operations::Operation;
use order::Order;
use product::Product;

use crate::{runtime::Runtime, application::indep_repository_transaction};

#[actix_rt::main]
async fn main() {
    let mut runtime = Runtime::new();

    let customer = Customer::new().start();
    let operations = vec![Operation::Create(0, 0), Operation::Read(0)];

    let cust = single_repository_transaction(customer.clone(), operations, &mut runtime).await;
    println!("customer single transaction: {:?}", cust);

    let order = Order::new().start();
    let operations = vec![Operation::Create(1, 3), Operation::Read(1)];
    let ord = single_repository_transaction(order.clone(), operations, &mut runtime).await;
    println!("order single transaction: {:?}", ord);

    let operations = vec![Operation::Read(1)];
    // let a = vec![order];
    // let res = indep_repository_transaction(a, operations, &mut runtime).await;
    // println!("indep xaction: {:?}", res);

    let tid = uuid::Uuid::new_v4();
    let args = crate::operations::Arguments {
        timestamp: 0,
        operations,
    };
    let vote_customer = customer
        .send(crate::messages::MessagePrepare::<Order>::Indep(tid, args.clone(), 2))
        .await
        .unwrap()
        .unwrap();

    println!("vote cus: {:?}", vote_customer);

    let vote_order = order
        .send(crate::messages::MessagePrepare::<Order>::Indep(tid, args.clone(), 2))
        .await;

    println!("vote order: {:?}", vote_order);

    let prod = Product::new().start();
    let _a = customer
        .send(crate::messages::MessagePrepare::IndepParticipants(
            tid,
            vote_customer,
            vec![order.clone().into()],
        ))
        .await;
    // let _a = order
    //     .send(MessagePrepare::IndepParticipants(
    //         tid,
    //         vote_order.unwrap().unwrap(),
    //         vec![customer.clone().into()],
    //     ))
    //     .await;

    // let b = customer.send(GetResult(tid)).await;
    // println!("b: {:?}", b);

    // let b = order.send(GetResult(tid)).await;
    // println!("b: {:?}", b);


    let _ = actix_rt::time::sleep(Duration::from_secs(2)).await;
}
