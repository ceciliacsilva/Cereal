use actix::prelude::*;
use actix_web::web;
use actix_web_actors::ws::{self, WebsocketContext};
use cereal_core::{
    messages::{CommitVote, GetProposedTs, GetResult, MessageAccept, MessagePrepare},
    operations::{Arguments, Table},
    repository::Repository,
};
use futures_util::{SinkExt as _, StreamExt as _};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::GetResultResponse;

/// A `network` wrap over [cereal_core::message].
#[derive(Message, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[rtype(result = "Result<CommitVote, anyhow::Error>")]
pub(crate) enum MessageWs {
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
#[derive(Clone)]
pub(crate) struct RepositoryWs {
    repo_actor: web::Data<Addr<Repository>>,
}

impl RepositoryWs {
    /// Create a new `Repository` using a `Arc` of `Repository`.
    pub(crate) fn new(repo_actor: web::Data<Addr<Repository>>) -> Self {
        RepositoryWs { repo_actor }
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
                let xaction_result: anyhow::Result<Option<Table>, _> = res.unwrap();
                let response = match xaction_result {
                    Ok(table) => GetResultResponse::Ok(table),
                    Err(e) => GetResultResponse::Err(e.to_string()),
                };

                log::info!("response from tid: {:?}", response);
                let response = serde_json::to_string(&response)
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
                log::info!("response single: {:?}", res);
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
                log::info!("response indep: {:?}", res);
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
                log::info!("response coord: {:?}", res);
                let response = serde_json::to_string(&res)
                    .expect("Actor response is typed. So should never happend");
                ctx.text(response);
                fut::ready(())
            })
            .wait(ctx);
    }

    // TODO: Ideally this should be better because it leaks details over the protocol.
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
                log::info!("proposed ts: {:?}", proposed_ts);
                async move {
                    for participant in participants.iter() {
                        let uri = participant;
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

                        let _ = connection.next().await.unwrap();
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
                log::info!("proposed ts: {:?}", proposed_ts);
                async move {
                    for participant in participants.iter() {
                        log::info!("URI: {:?}", participant);
                        let uri = participant;
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

                        log::info!("send message accept");
                        let _ = connection.next().await.unwrap();
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
                log::info!("response indep: {:?}", res);
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
                log::info!("response indep: {:?}", res);
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
                log::info!("Ws message got: {text}");
                let message_deserialized = serde_json::from_str::<MessageWs>(&text);
                log::info!("{:?}", message_deserialized);
                if let Ok(message) = message_deserialized {
                    match message {
                        MessageWs::Single { tid, args } => {
                            log::info!("Ws deserialized single: {:?}, {:?}", tid, args);
                            self.send_prepare_single(tid, args, ctx);
                        }
                        MessageWs::Indep {
                            tid,
                            args,
                            participants_size,
                        } => {
                            log::info!(
                                "Ws deserialized indep: {:?}, {:?}, {:?}",
                                tid,
                                args,
                                participants_size
                            );
                            self.send_prepare_indep(tid, args, participants_size, ctx);
                        }
                        MessageWs::IndepParticipants {
                            tid,
                            vote,
                            participants,
                        } => {
                            log::info!(
                                "Ws deserialized indep participants: {:?}, {:?}, {:?}",
                                tid,
                                vote,
                                participants
                            );
                            self.send_prepare_indep_accept(tid, vote, participants, ctx);
                        }
                        MessageWs::Coord {
                            tid,
                            args,
                            participants_size,
                        } => {
                            log::info!(
                                "Ws deserialized coord: {:?}, {:?}, {:?}",
                                tid,
                                args,
                                participants_size
                            );
                            self.send_prepare_coord(tid, args, participants_size, ctx);
                        }
                        MessageWs::CoordParticipants {
                            tid,
                            vote,
                            participants,
                        } => {
                            log::info!(
                                "Ws deserialized coord participants: {:?}, {:?}, {:?}",
                                tid,
                                vote,
                                participants
                            );
                            self.send_prepare_coord_accept(tid, vote, participants, ctx);
                        }
                        MessageWs::AcceptIndep {
                            tid,
                            proposed_ts,
                            vote,
                        } => {
                            log::info!(
                                "Ws deserialized accept indep: {:?}, {:?}, {:?}",
                                tid,
                                proposed_ts,
                                vote
                            );
                            self.send_accept_indep(tid, proposed_ts, vote, ctx);
                        }
                        MessageWs::AcceptCoord {
                            tid,
                            proposed_ts,
                            vote,
                        } => {
                            log::info!(
                                "Ws deserialized accept coord: {:?}, {:?}, {:?}",
                                tid,
                                proposed_ts,
                                vote
                            );
                            self.send_accept_coord(tid, proposed_ts, vote, ctx);
                        }
                        MessageWs::GetResult { tid } => {
                            log::info!("Ws deserialized get result: {:?}", tid,);
                            self.send_get_result(tid, ctx);
                        }
                    }
                } else {
                    log::warn!("Error deserialize ws message, {:?}", message_deserialized);
                    ctx.text("Error");
                }
            }
            Ok(ws::Message::Binary(bin)) => ctx.binary(bin),
            _ => (),
        }
    }
}
