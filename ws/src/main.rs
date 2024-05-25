use actix::prelude::*;
use actix_web::{http::Uri, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws::{self, WebsocketContext, Frame};
use clap::{Parser, Subcommand};
use futures_util::{SinkExt as _, StreamExt as _};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cereal_core::{
    messages::{CommitVote, GetProposedTs, GetResult, MessageAccept, MessagePrepare},
    operations::{Arguments, Expr, Operation, Statement, Table},
    repository::Repository,
    runtime::Runtime,
};

#[derive(Message, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[rtype(result = "Result<CommitVote, anyhow::Error>")]
pub enum MessageWs {
    Single {
        tid: Uuid,
        args: Arguments,
    },
    // tid, ops, participants.length()
    Indep {
        tid: Uuid,
        args: Arguments,
        participants_size: usize,
    },
    // TODO: change Vec<String> to something better?
    IndepParticipants {
        tid: Uuid,
        vote: CommitVote,
        participants: Vec<String>,
    },
    // TODO: change to be actors instead of u64.
    Coord {
        tid: Uuid,
        args: Arguments,
        participants_size: usize,
    },
    CoordParticipants {
        tid: Uuid,
        vote: CommitVote,
        participants: Vec<String>,
    },
    AcceptIndep {
        tid: Uuid,
        proposed_ts: usize,
        vote: CommitVote,
    },
    AcceptCoord {
        tid: Uuid,
        proposed_ts: usize,
        vote: CommitVote,
    },
    GetResult {
        tid: Uuid,
    },
}

/// A Ws Wrapper of `Repository`.
struct RepositoryWs {
    repo_actor: Addr<Repository>,
}

impl RepositoryWs {
    fn new(name: String) -> Self {
        RepositoryWs {
            repo_actor: Repository::new(name).start(),
        }
    }
}

impl Actor for RepositoryWs {
    type Context = ws::WebsocketContext<Self>;
}

impl RepositoryWs {
    fn send_get_result(&self, tid: Uuid, ctx: &mut WebsocketContext<Self>) {
        self.repo_actor
            .send(GetResult(tid))
            .into_actor(self)
            .then(|res, _, ctx| {
                // FIXME: error handling
                let res: Option<Table> = res.unwrap().unwrap();
                info!("response from tid: {:?}", res);
                let response = serde_json::to_string(&res)
                    .expect("Actor response is typed. So should never happend");
                ctx.text(response);
                fut::ready(())
            })
            .wait(ctx);
    }

    fn send_prepare_single(&self, tid: Uuid, args: Arguments, ctx: &mut WebsocketContext<Self>) {
        self.repo_actor
            .send(MessagePrepare::Single(tid, args))
            .into_actor(self)
            .then(|res, _, ctx| {
                let res: CommitVote = res.unwrap().unwrap();
                info!("response single: {:?}", res);
                let response = serde_json::to_string(&res)
                    .expect("Actor response is typed. So should never happend");
                ctx.text(response);
                fut::ready(())
            })
            .wait(ctx);
    }

    fn send_prepare_indep(
        &self,
        tid: Uuid,
        args: Arguments,
        participants_size: usize,
        ctx: &mut WebsocketContext<Self>,
    ) {
        self.repo_actor
            .send(MessagePrepare::Indep(tid, args, participants_size))
            .into_actor(self)
            .then(|res, _, ctx| {
                let res: CommitVote = res.unwrap().unwrap();
                info!("response indep: {:?}", res);
                let response = serde_json::to_string(&res)
                    .expect("Actor response is typed. So should never happend");
                ctx.text(response);
                fut::ready(())
            })
            .wait(ctx);
    }

    fn send_prepare_coord(
        &self,
        tid: Uuid,
        args: Arguments,
        participants_size: usize,
        ctx: &mut WebsocketContext<Self>,
    ) {
        self.repo_actor
            .send(MessagePrepare::Coord(tid, args, participants_size))
            .into_actor(self)
            .then(|res, _, ctx| {
                let res: CommitVote = res.unwrap().unwrap();
                info!("response indep: {:?}", res);
                let response = serde_json::to_string(&res)
                    .expect("Actor response is typed. So should never happend");
                ctx.text(response);
                fut::ready(())
            })
            .wait(ctx);
    }

    // TODO: don't like this.
    fn send_prepare_indep_accept(
        &self,
        tid: Uuid,
        vote: CommitVote,
        participants: Vec<String>,
        ctx: &mut WebsocketContext<Self>,
    ) {
        self.repo_actor
            .send(GetProposedTs(tid))
            .into_actor(self)
            .then(move |res, this, ctx| {
                let proposed_ts: usize = res.unwrap();
                info!("proposed ts: {:?}", proposed_ts);
                async move {
                    for participant in participants.iter() {
                        let uri = Uri::builder()
                            .authority(participant.clone())
                            .scheme("http")
                            .path_and_query("/ws/")
                            .build()
                            .unwrap();
                        // TODO: loop over all participants
                        let (_resp, mut connection) =
                            awc::Client::new().ws(uri).connect().await.unwrap();

                        let msg = serde_json::to_string(&MessageWs::AcceptIndep {
                            tid,
                            proposed_ts,
                            vote: vote.clone(),
                        })
                        .expect("This can be serialized");
                        connection
                            .send(ws::Message::Text(msg.into()))
                            .await
                            .unwrap();

                        let _ = connection.next().await.unwrap().unwrap();
                    }
                    fut::ready(())
                }
                .into_actor(this)
                .map(|res, _, _| println!("{:?}", res))
                .wait(ctx);

                ctx.text("Message sent to participants".to_string());
                fut::ready(())
            })
            .wait(ctx);
    }

    fn send_prepare_coord_accept(
        &self,
        tid: Uuid,
        vote: CommitVote,
        participants: Vec<String>,
        ctx: &mut WebsocketContext<Self>,
    ) {
        self.repo_actor
            .send(GetProposedTs(tid))
            .into_actor(self)
            .then(move |res, this, ctx| {
                let proposed_ts: usize = res.unwrap();
                info!("proposed ts: {:?}", proposed_ts);
                async move {
                    for participant in participants.iter() {
                        let uri = Uri::builder()
                            .authority(participant.clone())
                            .scheme("http")
                            .path_and_query("/ws/")
                            .build()
                            .unwrap();
                        // TODO: loop over all participants
                        let (_resp, mut connection) =
                            awc::Client::new().ws(uri).connect().await.unwrap();

                        let msg = serde_json::to_string(&MessageWs::AcceptCoord {
                            tid,
                            proposed_ts,
                            vote: vote.clone(),
                        })
                        .expect("This can be serialized");
                        connection
                            .send(ws::Message::Text(msg.into()))
                            .await
                            .unwrap();

                        info!("send message accept");
                        let _ = connection.next().await.unwrap().unwrap();
                    }
                    fut::ready(())
                }
                .into_actor(this)
                .map(|res, _, _| println!("{:?}", res))
                .wait(ctx);

                ctx.text("Message sent to participants".to_string());
                fut::ready(())
            })
            .wait(ctx);
    }

    fn send_accept_indep(
        &self,
        tid: Uuid,
        proposed_ts: usize,
        vote: CommitVote,
        ctx: &mut WebsocketContext<Self>,
    ) {
        self.repo_actor
            .send(MessageAccept::Indep(tid, proposed_ts, vote))
            .into_actor(self)
            .then(|res, _, ctx| {
                let res: CommitVote = res.unwrap().unwrap();
                info!("response indep: {:?}", res);
                let response = serde_json::to_string(&res)
                    .expect("Actor response is typed. So should never happend");
                ctx.text(response);
                fut::ready(())
            })
            .wait(ctx);
    }

    fn send_accept_coord(
        &self,
        tid: Uuid,
        proposed_ts: usize,
        vote: CommitVote,
        ctx: &mut WebsocketContext<Self>,
    ) {
        self.repo_actor
            .send(MessageAccept::Coord(tid, proposed_ts, vote))
            .into_actor(self)
            .then(|res, _, ctx| {
                let res: CommitVote = res.unwrap().unwrap();
                info!("response indep: {:?}", res);
                let response = serde_json::to_string(&res)
                    .expect("Actor response is typed. So should never happend");
                ctx.text(response);
                fut::ready(())
            })
            .wait(ctx);
    }
}

/// Handler for ws::Message message
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for RepositoryWs {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Text(text)) => {
                info!("Ws message got: {text}");
                // FIXME: can be MessageAccept also.
                let message_deserialized = serde_json::from_str::<MessageWs>(&text);
                info!("{:?}", message_deserialized);
                if let Ok(message) = message_deserialized {
                    match message {
                        MessageWs::Single { tid, args } => {
                            info!("Ws deserialized single: {:?}, {:?}", tid, args);
                            self.send_prepare_single(tid, args, ctx);
                        }
                        MessageWs::Indep {
                            tid,
                            args,
                            participants_size,
                        } => {
                            info!(
                                "Ws deserialized indep: {:?}, {:?}, {:?}",
                                tid, args, participants_size
                            );
                            self.send_prepare_indep(tid, args, participants_size, ctx);
                        }
                        MessageWs::IndepParticipants {
                            tid,
                            vote,
                            participants,
                        } => {
                            info!(
                                "Ws deserialized indep participants: {:?}, {:?}, {:?}",
                                tid, vote, participants
                            );
                            self.send_prepare_indep_accept(tid, vote, participants, ctx);
                        }
                        MessageWs::Coord {
                            tid,
                            args,
                            participants_size,
                        } => {
                            info!(
                                "Ws deserialized coord: {:?}, {:?}, {:?}",
                                tid, args, participants_size
                            );
                            self.send_prepare_coord(tid, args, participants_size, ctx);
                        }
                        MessageWs::CoordParticipants {
                            tid,
                            vote,
                            participants,
                        } => {
                            info!(
                                "Ws deserialized coord participants: {:?}, {:?}, {:?}",
                                tid, vote, participants
                            );
                            self.send_prepare_coord_accept(tid, vote, participants, ctx);
                        }
                        MessageWs::AcceptIndep {
                            tid,
                            proposed_ts,
                            vote,
                        } => {
                            info!(
                                "Ws deserialized accept indep: {:?}, {:?}, {:?}",
                                tid, proposed_ts, vote
                            );
                            self.send_accept_indep(tid, proposed_ts, vote, ctx);
                        }
                        MessageWs::AcceptCoord {
                            tid,
                            proposed_ts,
                            vote,
                        } => {
                            info!(
                                "Ws deserialized accept coord: {:?}, {:?}, {:?}",
                                tid, proposed_ts, vote
                            );
                            self.send_accept_coord(tid, proposed_ts, vote, ctx);
                        }
                        MessageWs::GetResult { tid } => {
                            info!("Ws deserialized get result: {:?}", tid,);
                            self.send_get_result(tid, ctx);
                        }
                    }
                } else {
                    warn!("Error deserialize ws message, {:?}", message_deserialized);
                    ctx.text("Error");
                }
            }
            Ok(ws::Message::Binary(bin)) => ctx.binary(bin),
            _ => (),
        }
    }
}

async fn index(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
    ws::start(RepositoryWs::new("customer".to_string()), &req, stream)
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
    /// start a new client.
    Client {
        #[arg(short, long)]
        customer_port: u16,
        #[arg(short, long)]
        order_port: u16,
        #[arg(short, long)]
        product_port: u16,
    },
}

struct Client {}
impl Client {
    async fn send_order(self, order_port: u16, runtime: &mut Runtime) -> anyhow::Result<()> {
        let uri = Uri::builder()
            .authority(format!("127.0.0.1:{order_port}"))
            .scheme("http")
            .path_and_query("/ws/")
            .build()
            .unwrap();

        let (_resp, connection) = awc::Client::new().ws(uri).connect().await.unwrap();
        let mut connection = connection.fuse();

        let operations = vec![
            Operation::Statement(Statement::Create(0, Box::new(Expr::Value((1, 1))))),
            Operation::Statement(Statement::Create(1, Box::new(Expr::Value((2, 2))))),
            Operation::Statement(Statement::Create(2, Box::new(Expr::Value((3, 3))))),
        ];

        let tid = Uuid::new_v4();
        let args = Arguments {
            timestamp: runtime.now(),
            operations,
        };

        let msg = serde_json::to_string(&MessageWs::Single { tid, args })
            .expect("this can be serialized");

        connection
            .send(ws::Message::Text(msg.into()))
            .await
            .unwrap();

        let res = connection.next().await.unwrap()?;

        print!("Result: {:?}", res);

        let operations = vec![
            Operation::Expr(Expr::Read(0)),
        ];

        let tid = Uuid::new_v4();
        let args = Arguments {
            timestamp: runtime.now(),
            operations,
        };

        let msg = serde_json::to_string(&MessageWs::Single { tid, args })
            .expect("this can be serialized");

        connection
            .send(ws::Message::Text(msg.into()))
            .await
            .unwrap();
        let res = connection.next().await.unwrap()?;

        print!("Result: {:?}", res);

        let msg = serde_json::to_string(&MessageWs::GetResult { tid } )
            .expect("this can be serialized");

        connection
            .send(ws::Message::Text(msg.into()))
            .await
            .unwrap();
        print!("Result: {:?}", res);

        Ok(())
    }

    async fn send_customer(self, customer_port: u16, runtime: &mut Runtime) -> anyhow::Result<()> {
        let uri = Uri::builder()
            .authority(format!("127.0.0.1:{customer_port}"))
            .scheme("http")
            .path_and_query("/ws/")
            .build()
            .unwrap();

        let (_resp, connection) = awc::Client::new().ws(uri).connect().await.unwrap();
        let mut connection = connection.fuse();

        let operations = vec![
            Operation::Statement(Statement::Create(0, Box::new(Expr::Value((1, 1))))),
            Operation::Statement(Statement::Create(1, Box::new(Expr::Value((2, 2))))),
            Operation::Statement(Statement::Create(2, Box::new(Expr::Value((3, 3))))),
        ];

        let tid = Uuid::new_v4();
        let args = Arguments {
            timestamp: runtime.now(),
            operations,
        };

        let msg = serde_json::to_string(&MessageWs::Single { tid, args })
            .expect("this can be serialized");

        connection
            .send(ws::Message::Text(msg.into()))
            .await
            .unwrap();

        let res = connection.next().await.unwrap()?;

        print!("Result: {:?}", res);

        let operations = vec![
            Operation::Expr(Expr::Read(0)),
        ];

        let tid = Uuid::new_v4();
        let args = Arguments {
            timestamp: runtime.now(),
            operations,
        };

        let msg = serde_json::to_string(&MessageWs::Single { tid, args })
            .expect("this can be serialized");

        connection
            .send(ws::Message::Text(msg.into()))
            .await
            .unwrap();
        let res = connection.next().await.unwrap()?;

        print!("Result: {:?}", res);

        let msg = serde_json::to_string(&MessageWs::GetResult { tid } )
            .expect("this can be serialized");

        connection
            .send(ws::Message::Text(msg.into()))
            .await
            .unwrap();
        print!("Result: {:?}", res);

        Ok(())
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let cli: Cli = Cli::parse();

    match cli.command {
        Commands::Repository { port } => {
            return HttpServer::new(|| App::new().route("/ws/", web::get().to(index)))
                .bind(("127.0.0.1", port))?
                .run()
                .await
        }
        Commands::Client {
            customer_port,
            order_port,
            product_port,
        } => {
            let mut runtime = Runtime::new();
            let _ = Client{}.send_customer(customer_port, &mut runtime).await;
            let _ = Client{}.send_order(order_port, &mut runtime).await;
        }
    }

    Ok(())
}
