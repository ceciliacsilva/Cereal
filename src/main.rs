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
use customer::Customer;
use messages::{GetResult, MessagePrepare};
use operations::{Arguments, Operation};
use order::Order;
use product::Product;
use uuid::Uuid;

use crate::runtime::Runtime;

#[actix_rt::main]
async fn main() {
    let customer = Customer::new().start();
    let tid = Uuid::new_v4();
    let operations = vec![Operation::Create(0, 0), Operation::Read(0)];
    let args = Arguments {
        timestamp: 0,
        operations,
    };
    let _a = customer
        .send(MessagePrepare::Single(tid, args.clone()))
        .await;

    let b = customer.send(GetResult(tid)).await;
    println!("b: {:?}", b);

    let runtime = Runtime::new();
    let order = Order::new().start();
    let tid = Uuid::new_v4();
    let operations = vec![Operation::Create(1, 3), Operation::Read(1)];
    let args = Arguments {
        timestamp: 0,
        operations,
    };
    let _a = order
        .send(MessagePrepare::Single(tid, args.clone()))
        .await;

    let b = order.send(GetResult(tid)).await;
    println!("b: {:?}", b);

    let tid = Uuid::new_v4();
    let operations = vec![Operation::Read(1)];
    let args = Arguments {
        timestamp: 0,
        operations,
    };
    let vote_customer = customer
        .send(MessagePrepare::Indep(tid, args.clone(), 1))
        .await
        .unwrap()
        .unwrap();

    println!("vote cus: {:?}", vote_customer);

    let vote_order = order
        .send(MessagePrepare::Indep(tid, args.clone(), 1))
        .await;

    println!("vote order: {:?}", vote_order);

    let _a = customer
        .send(MessagePrepare::IndepParticipants(
            tid,
            vote_customer,
            vec![order.clone().into()],
        ))
        .await;
    let _a = order
        .send(MessagePrepare::IndepParticipants(
            tid,
            vote_order.unwrap().unwrap(),
            vec![customer.clone().into()],
        ))
        .await;

    let b = customer.send(GetResult(tid)).await;
    println!("b: {:?}", b);

    let b = order.send(GetResult(tid)).await;
    println!("b: {:?}", b);

    let _ = Product::new().start();

    let _ = actix_rt::time::sleep(Duration::from_secs(2)).await;
}
