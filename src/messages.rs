use crate::{operations::*, repository::Repository};
use actix::{
    dev::{MessageResponse, OneshotSender},
    prelude::*,
};
use uuid::Uuid;

#[derive(Message)]
#[rtype(result = "Result<CommitVote, anyhow::Error>")]
pub(crate) enum MessagePrepare {
    Single(Uuid, Arguments),
    // tid, ops, participants.length()
    Indep(Uuid, Arguments, usize),
    IndepParticipants(Uuid, CommitVote, Vec<Addr<Repository>>),
    // TODO: change to be actors instead of u64.
    Coord(Uuid, Arguments, usize),
    CoordParticipants(Uuid, CommitVote, Vec<Addr<Repository>>),
}

#[derive(Message)]
#[rtype(result = "Result<CommitVote, anyhow::Error>")]
pub(crate) enum MessageAccept {
    // tid, proposed_ts
    Indep(Uuid, usize, CommitVote),
    // TODO: fix types
    Coord(Uuid, usize, CommitVote),
}

#[derive(Message)]
#[rtype(result = "Result<Option<Table>, anyhow::Error>")]
pub(crate) struct GetResult(pub Uuid);

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum CommitVote {
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
