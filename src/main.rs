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

#[actix_rt::main]
async fn main() {
    let _ = actix_rt::time::sleep(Duration::from_secs(2)).await;
}
