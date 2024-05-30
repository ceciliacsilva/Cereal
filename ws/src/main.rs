// TODO: change all ws communications to `Binary` instead of `Text`.
use std::net::Ipv4Addr;

use actix::prelude::*;
use actix_web::{
    web, App, Error, HttpRequest, HttpResponse, HttpServer,
};
use actix_web_actors::ws::{self, WebsocketContext};
use clap::{Parser, Subcommand};
use futures_util::{SinkExt as _, StreamExt as _};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cereal_core::{
    messages::{CommitVote, GetProposedTs, GetResult, MessageAccept, MessagePrepare},
    operations::{Arguments, Expr, Operation, Statement, Table},
    repository::Repository,
};

mod client;
mod macros;
mod repositoryws;

use crate::{
    client::*,
    macros::{add, create, op, read, sub, update, value},
    repositoryws::*,
};


async fn index(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
    println!("to aqui");

    let repo = req.app_data::<web::Data<Addr<Repository>>>().unwrap();
    let repows = RepositoryWs::new(repo.clone());
    ws::start(repows, &req, stream)
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
        #[arg(short, long)]
        customer_port: u16,
        #[arg(short, long)]
        order_port: u16,
        #[arg(short, long)]
        product_port: u16,
    },
}

fn to_io_error(error: anyhow::Error) -> std::io::Error {
    let context = format!("failed to send operations. {}", error);
    std::io::Error::new(std::io::ErrorKind::Other, context)
}

async fn create_order(
    customer: &mut Client,
    order: &mut Client,
    product: &mut Client,
) -> anyhow::Result<()> {
    //loop {
    let tid = Uuid::new_v4();

    let operation_order = vec![create!(0, value!(Table(5, 5)))];
    let operation_customer = vec![update!(0, add!(read!(0), value!(Table(1, 1))))];
    let operation_product = vec![update!(0, sub!(read!(0), value!(Table(1, 1))))];

    let mut clients = Clients {
        participants: vec![customer, order, product],
    };

    let results = clients
        .send_coord(
            &tid,
            vec![operation_customer, operation_order, operation_product],
        )
        .await;

    log::info!("coord result: {:?}", results);

    let op_read = vec![op!(read!(0))];
    let tid = order.send_single(op_read.clone()).await?;
    let result = order.get_result(&tid).await?;
    println!("order result: {:?}", result);

    let tid = customer.send_single(op_read.clone()).await?;
    let result = customer.get_result(&tid).await?;
    println!("customer result: {:?}", result);

    let tid = product.send_single(op_read.clone()).await?;
    let result = product.get_result(&tid).await?;
    println!("product result: {:?}", result);

    Ok(())
    //}
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
            customer_port,
            order_port,
            product_port,
        } => {
            let operations = vec![
                create!(0, value!(Table(10, 10))),
                create!(1, value!(Table(20, 20))),
                create!(2, value!(Table(30, 30))),
                create!(3, value!(Table(40, 40))),
            ];
            let customer_builder = ClientBuilder::new(Ipv4Addr::new(127, 0, 0, 1), customer_port);
            let mut customer = customer_builder.build().await;

            let _tid = customer
                .send_single(operations.clone())
                .await
                .map_err(to_io_error)?;

            let product_builder = ClientBuilder::new(Ipv4Addr::new(127, 0, 0, 1), product_port);
            let mut product = product_builder.build().await;

            let _tid = product.send_single(operations).await.map_err(to_io_error)?;

            let order_builder = ClientBuilder::new(Ipv4Addr::new(127, 0, 0, 1), order_port);
            let mut order = order_builder.build().await;

            let _ = create_order(&mut customer, &mut order, &mut product).await;
        }
    }

    Ok(())
}
