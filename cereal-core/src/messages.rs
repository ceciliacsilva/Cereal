use crate::{operations::*, repository::Repository};
use actix::{
    dev::{MessageResponse, OneshotSender},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Message, Debug)]
#[rtype(result = "Result<CommitVote, anyhow::Error>")]
pub enum MessagePrepare {
    Single(Uuid, Arguments),
    // tid, ops, participants.length()
    Indep(Uuid, Arguments, usize),
    IndepParticipants(Uuid, CommitVote, Vec<Addr<Repository>>),
    // TODO: change to be actors instead of u64.
    Coord(Uuid, Arguments, usize),
    CoordParticipants(Uuid, CommitVote, Vec<Addr<Repository>>),
}

#[derive(Message, Debug)]
#[rtype(result = "Result<CommitVote, anyhow::Error>")]
pub enum MessageAccept {
    // tid, proposed_ts
    Indep(Uuid, usize, CommitVote),
    // TODO: fix types
    Coord(Uuid, usize, CommitVote),
}

#[derive(Message, Debug)]
#[rtype(result = "Result<Option<Table>, anyhow::Error>")]
pub struct GetResult(pub Uuid);

#[derive(Message, Debug)]
#[rtype(result = "usize")]
pub struct GetProposedTs(pub Uuid);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum CommitVote {
    Commit(Option<usize>),
    Abort,
    Conflict,
    InProgress,
}

impl<A, M> MessageResponse<A, M> for CommitVote
where
    A: Actor,
    M: Message<Result = CommitVote>,
{
    fn handle(
        self,
        _ctx: &mut A::Context,
        tx: Option<OneshotSender<<M as actix::Message>::Result>>,
    ) {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
    }
}
