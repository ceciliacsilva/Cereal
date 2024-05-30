// TODO: change all ws communications to `Binary` instead of `Text`.
use std::{net::Ipv4Addr, time::Duration};

use actix::prelude::*;
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use clap::{Parser, Subcommand};

use cereal_core::{
    operations::{Expr, Operation, Statement, Table},
    repository::Repository,
};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

mod client;
mod macros;
mod repositoryws;

use crate::{
    client::*,
    macros::{add, create, op, read, sub, update, value},
    repositoryws::*,
};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum GetResultResponse {
    Ok(Option<Table>),
    Err(String),
}

async fn index(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
    let repo = req.app_data::<web::Data<Addr<Repository>>>().unwrap();
    let repows = RepositoryWs::new(repo.clone());
    ws::start(repows, &req, stream)
}

fn to_io_error(error: anyhow::Error) -> std::io::Error {
    let context = format!("failed to send operations. {}", error);
    std::io::Error::new(std::io::ErrorKind::Other, context)
}

async fn populate_customer_and_product(
    customer: &mut Client,
    product: &mut Client,
) -> anyhow::Result<()> {
    let operations = vec![
        create!(1, value!(Table(10, 10))),
        create!(2, value!(Table(20, 20))),
        create!(3, value!(Table(30, 30))),
        create!(4, value!(Table(40, 40))),
        create!(5, value!(Table(50, 50))),
        create!(6, value!(Table(60, 60))),
    ];
    let _result = customer
        .send_single(operations.clone())
        .await
        .map_err(to_io_error)?;

    let _result = product.send_single(operations).await.map_err(to_io_error)?;

    Ok(())
}

async fn check_invariant(
    customer: &mut Client,
    _order: &mut Client,
    product: &mut Client,
) -> anyhow::Result<()> {
    loop {
        let mut rng = thread_rng();
        // TODO: have a dynamic generated repo and keys. So the limits are not
        // hardcoded.
        let key: usize = rng.gen_range(1..=6);

        let operation_customer = vec![op!(read!(key))];
        let operation_product = vec![op!(read!(key))];

        let mut clients = Clients {
            participants: vec![customer, product],
        };

        let results = clients
            .send_indep(vec![operation_customer, operation_product])
            .await?;

        log::info!("for key = {key}, indep result: {:?}", results);

        // TODO: make this less horrible
        if let (Some(Some(result_customer)), Some(Some(result_product))) =
            (results.get(0), results.get(1))
        {
            let key: i64 = i64::try_from(key)?;
            assert_eq!(
                result_customer.clone() + result_product.clone(),
                Table(key * 10 * 2, key * 10 * 2)
            )
        }

        actix::clock::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

// TODO: handle the loop better
async fn create_order(
    customer: &mut Client,
    order: &mut Client,
    product: &mut Client,
) -> anyhow::Result<()> {
    loop {
        let mut rng = thread_rng();
        // TODO: have a dynamic generated repo and keys. So the limits are not
        // hardcoded.
        let key: usize = rng.gen_range(1..=6);

        // SAFETY: this is won't work for very large keys.
        let operation_order = vec![create!(
            key,
            value!(Table(i64::try_from(key)?, i64::try_from(key)?))
        )];
        log::debug!("Choosen key: {key}");
        // XXX: Needs to add! and sub! the same amount.
        let operation_customer = vec![update!(key, add!(read!(key), value!(Table(1, 1))))];
        let operation_product = vec![update!(key, sub!(read!(key), value!(Table(1, 1))))];

        let mut clients = Clients {
            participants: vec![customer, order, product],
        };

        let results = clients
            .send_coord(vec![operation_customer, operation_order, operation_product])
            .await;

        log::info!("coord result: {:?}", results);

        let op_read = vec![op!(read!(key))];
        let result = order.send_single(op_read.clone()).await?;
        println!("order result: {:?}", result);

        let result = customer.send_single(op_read.clone()).await?;
        println!("customer result: {:?}", result);

        let result = product.send_single(op_read.clone()).await?;
        println!("product result: {:?}", result);

        actix::clock::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// start a new `Repository`
    Repository {
        #[arg(short, long)]
        port: u16,
    },
    /// start a loosely inspired TPC-like testing.
    TPCFake {
        #[command(subcommand)]
        tpc_command: TPCFakeCommand,
        #[arg(short, long, required(true))]
        customer_port: u16,
        #[arg(short, long, required(true))]
        order_port: u16,
        #[arg(short, long, required(true))]
        product_port: u16,
    },
}

#[derive(Subcommand, Debug)]
enum TPCFakeCommand {
    Start,
    Management,
    Buyer,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let cli: Cli = Cli::parse();

    match cli.command {
        Commands::Repository { port } => {
            let repo_actor: web::Data<Addr<Repository>> =
                web::Data::new(Repository::new(format!("repository-{port}")).start());
            return HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::clone(&repo_actor))
                    .route("/ws/", web::get().to(index))
            })
            .bind(("127.0.0.1", port))?
            .run()
            .await;
        }
        Commands::TPCFake {
            tpc_command,
            customer_port,
            order_port,
            product_port,
        } => {
            let customer_builder = ClientBuilder::new(Ipv4Addr::new(127, 0, 0, 1), customer_port);
            let mut customer = customer_builder.build().await;

            let product_builder = ClientBuilder::new(Ipv4Addr::new(127, 0, 0, 1), product_port);
            let mut product = product_builder.build().await;

            let order_builder = ClientBuilder::new(Ipv4Addr::new(127, 0, 0, 1), order_port);
            let mut order = order_builder.build().await;

            match tpc_command {
                TPCFakeCommand::Start => populate_customer_and_product(&mut customer, &mut product)
                    .await
                    .map_err(to_io_error)?,
                TPCFakeCommand::Management => {
                    check_invariant(&mut customer, &mut order, &mut product)
                        .await
                        .map_err(to_io_error)?
                }
                TPCFakeCommand::Buyer => create_order(&mut customer, &mut order, &mut product)
                    .await
                    .map_err(to_io_error)?,
            };
        }
    }

    Ok(())
}
