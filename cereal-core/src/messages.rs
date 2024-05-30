use crate::{operations::*, repository::Repository};
use actix::{
    dev::{MessageResponse, OneshotSender},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// [actix::Message] for the the first `half` of the `2PhaseProtocol`.
#[derive(Message, Debug)]
#[rtype(result = "Result<CommitVote, anyhow::Error>")]
pub enum MessagePrepare {
    /// For `single` repository transaction. Only one phase is needed.
    Single(Uuid, Arguments),
    /// For Indep repositories transaction.
    // tid, ops, participants.length()
    Indep(Uuid, Arguments, usize),
    /// For Indep repositories transaction. To be used after [Indep].
    IndepParticipants(Uuid, CommitVote, Vec<Addr<Repository>>),

    /// For Coordinated repositories transaction.
    Coord(Uuid, Arguments, usize),
    /// For Coordinated repositories transaction. To be used after [Coord].
    CoordParticipants(Uuid, CommitVote, Vec<Addr<Repository>>),
}

/// [actix::Message] for the second `half` of the `2PhaseProtocol`.
#[derive(Message, Debug)]
#[rtype(result = "Result<CommitVote, anyhow::Error>")]
pub enum MessageAccept {
    /// For independent repositories transactions.
    // tid, proposed_ts
    Indep(Uuid, usize, CommitVote),
    /// For Coordinated repositories transactions.
    Coord(Uuid, usize, CommitVote),
}

/// [actix::Message] to `get` the result for a given `tid`.
#[derive(Message, Debug)]
#[rtype(result = "Result<Option<Table>, anyhow::Error>")]
pub struct GetResult(pub Uuid);

/// [actix::Message] to `get` current proposed timestamp for a given `tid`.
/// Only needed for `RepositoryWs`.
#[derive(Message, Debug)]
#[rtype(result = "usize")]
pub struct GetProposedTs(pub Uuid);

/// Result of a transaction.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum CommitVote {
    // XXX: what this should hold??
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
