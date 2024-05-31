use std::collections::HashMap;

use uuid::Uuid;

use crate::{
    database::Database,
    messages::{CommitVote, GetProposedTs, GetResult, MessageAccept, MessagePrepare},
    operations::{Arguments, Table},
    runtime::Runtime,
};
use actix::prelude::*;

/// A `Repository`, represents a generic Database application.
///
/// Here is just a very toy-ish implementation, meant to be used inside a
/// `Actix-actor`.
pub struct Repository {
    /// A [`Database`] that holds the data.
    pub(crate) database: Database,
    /// A [`Runtime`] that controls time and persistency.
    pub(crate) runtime: Runtime,
    /// Last used timestamp.
    pub(crate) last_timestamp: usize,
    /// Map from `tid` to a transaction result.
    pub(crate) done_xactions: HashMap<Uuid, anyhow::Result<Option<Table>>>,
    /// Filename for durability.
    pub(crate) filename: String,
}

impl Repository {
    /// Create a new `Repository`.
    ///
    /// # Example:
    /// ```
    /// use cereal_core::repository::Repository;
    ///
    /// let repo = Repository::new("db.txt".to_string());
    /// ```
    pub fn new(filename: String) -> Self {
        Repository {
            database: Database::new(),
            runtime: Runtime::new(),
            last_timestamp: 0,
            done_xactions: HashMap::new(),
            filename,
        }
    }
}

macro_rules! find_max {
    ($x:expr) => ($x);
    ($x:expr, $($y:expr),+) => (
        std::cmp::max($x, find_max!($($y),+))
    )
}

impl Repository {
    /// Single-Repository Transactions.
    ///
    /// Section 4.3. https://pmg.csail.mit.edu/papers/granola-usenix12.pdf
    fn handle_single(
        &mut self,
        tid: Uuid,
        args: Arguments,
    ) -> anyhow::Result<crate::messages::CommitVote, anyhow::Error> {
        let runtime = &mut self.runtime;
        let current_time = runtime.now();
        let proposed_ts = find_max!(args.timestamp, current_time, self.last_timestamp) + 1;

        let operations_str = format!("{:?}", args.operations);
        runtime.write_to_durable(&self.filename, &operations_str, proposed_ts)?;

        self.database
            .add_xaction(&tid, proposed_ts, args.operations, 0);

        self.last_timestamp = proposed_ts;

        let result = self.database.run_nexts();

        for r in result {
            self.done_xactions.insert(r.0, Ok(r.1));
        }

        Ok(CommitVote::InProgress)
    }

    /// Independent Distributed Transactions
    ///
    /// Section 4.4. https://pmg.csail.mit.edu/papers/granola-usenix12.pdf
    /// Steps 1 and 2.
    fn handle_indep_prepare(
        &mut self,
        tid: Uuid,
        args: Arguments,
        participants_len: usize,
    ) -> anyhow::Result<crate::messages::CommitVote, anyhow::Error> {
        let runtime = &mut self.runtime;
        let current_time = runtime.now();
        let proposed_ts = find_max!(args.timestamp, current_time, self.last_timestamp) + 1;

        self.database
            .add_xaction(&tid, proposed_ts, args.operations.clone(), participants_len);

        let vote = if self.database.check_for_conflicts_and_primary_key(&tid) {
            self.database.finalize(&tid, proposed_ts);
            Ok(CommitVote::Conflict)
        } else {
            Ok(CommitVote::Commit(None))
        };

        let operations_str = format!("{:?}, {:?}", args.operations, vote);
        runtime.write_to_durable(&self.filename, &operations_str, proposed_ts)?;

        vote
    }

    /// Independent Distributed Transactions
    ///
    /// Section 4.4. https://pmg.csail.mit.edu/papers/granola-usenix12.pdf
    /// Step 3.
    fn send_message_accept_indep_to_participants(
        &mut self,
        tid: Uuid,
        vote: CommitVote,
        other_participants: &Vec<Addr<Repository>>,
    ) -> anyhow::Result<crate::messages::CommitVote, anyhow::Error> {
        let proposed_ts = self.database.get_proposed_ts_for_tid(&tid);
        for participant in other_participants {
            participant.do_send(MessageAccept::Indep(tid, proposed_ts, vote.clone()));
        }

        self.last_timestamp = proposed_ts;
        Ok(CommitVote::InProgress)
    }

    /// Independent Distributed Transactions
    ///
    /// Section 4.4. https://pmg.csail.mit.edu/papers/granola-usenix12.pdf
    /// Step 4 to end.
    fn handle_indep_accept(
        &mut self,
        tid: Uuid,
        proposed_ts: usize,
        vote: CommitVote,
    ) -> anyhow::Result<CommitVote, anyhow::Error> {
        // XXX: can it be abort??
        if vote == CommitVote::Conflict {
            self.database.finalize(&tid, proposed_ts);
            self.done_xactions
                .insert(tid, Err(anyhow::anyhow!("Problem at another repository")));
            return Ok(CommitVote::Abort);
        }
        // A conflict happened locally and the transaction should be aborted.
        if let Some(_) = self.database.tid_to_ts_end_xaction_ends.get(&tid) {
            self.done_xactions.insert(
                tid,
                Err(anyhow::anyhow!(
                    "Local problem, locked key or missing primary key"
                )),
            );
            return Ok(CommitVote::Abort);
        }

        if self.database.active_transactions.is_empty() {
            log::error!("Should not be here");
            return Ok(CommitVote::Abort);
        }

        self.database.decrement_reply_count(&tid);
        self.database
            .update_proposed_ts_to_highest(&tid, proposed_ts);
        let result = self.database.run_nexts();

        for r in result {
            self.done_xactions.insert(r.0, Ok(r.1));
        }

        Ok(CommitVote::InProgress)
    }

    /// Coordinated Distributed Transactions
    ///
    /// Section 4.5. https://pmg.csail.mit.edu/papers/granola-usenix12.pdf
    /// Steps 1, 2, 3.
    fn handle_coord_prepare(
        &mut self,
        tid: Uuid,
        args: Arguments,
        participants_len: usize,
    ) -> anyhow::Result<crate::messages::CommitVote, anyhow::Error> {
        let runtime = &mut self.runtime;
        let current_time = runtime.now();
        let proposed_ts = find_max!(args.timestamp, current_time, self.last_timestamp) + 1;

        let operations_str = format!("{:?}", args.operations);
        runtime.write_to_durable(&self.filename, &operations_str, proposed_ts)?;

        self.database
            .add_xaction(&tid, proposed_ts, args.operations.clone(), participants_len);

        log::error!(
            "check for conflicts: {:?}",
            self.database.check_for_conflicts_and_primary_key(&tid)
        );
        let vote = if self.database.check_for_conflicts_and_primary_key(&tid) {
            log::error!("================TO AQUI {:?}", tid);
            self.database.finalize(&tid, proposed_ts);
            Ok(CommitVote::Conflict)
        } else {
            self.database.get_all_locks(&tid);
            Ok(CommitVote::Commit(None))
        };

        let operations_str = format!("{:?}, {:?}", args.operations, vote);
        runtime.write_to_durable(&self.filename, &operations_str, proposed_ts)?;

        vote
    }

    /// Coordinated Distributed Transactions
    ///
    /// Section 4.5. https://pmg.csail.mit.edu/papers/granola-usenix12.pdf
    /// Step 4.
    fn send_message_accept_coord_to_participants(
        &mut self,
        tid: Uuid,
        vote: CommitVote,
        other_participants: &Vec<Addr<Repository>>,
    ) -> anyhow::Result<crate::messages::CommitVote, anyhow::Error> {
        let proposed_ts = self.database.get_proposed_ts_for_tid(&tid);
        for participant in other_participants {
            participant.do_send(MessageAccept::Coord(tid, proposed_ts, vote.clone()));
        }

        self.last_timestamp = proposed_ts;
        Ok(CommitVote::InProgress)
    }

    /// Coordinated Distributed Transactions
    ///
    /// Section 4.5. https://pmg.csail.mit.edu/papers/granola-usenix12.pdf
    /// Steps 5 to end.
    fn handle_coord_accept(
        &mut self,
        tid: Uuid,
        proposed_ts: usize,
        vote: CommitVote,
    ) -> anyhow::Result<CommitVote, anyhow::Error> {
        // XXX: can it be abort??
        if vote == CommitVote::Conflict {
            self.database.finalize(&tid, proposed_ts);
            self.done_xactions
                .insert(tid, Err(anyhow::anyhow!("Problem at another repository")));
            return Ok(CommitVote::Abort);
        }
        // A conflict happened locally and the transaction should be aborted.
        if let Some(_) = self.database.tid_to_ts_end_xaction_ends.get(&tid) {
            self.done_xactions.insert(
                tid,
                Err(anyhow::anyhow!(
                    "Local problem, locked key or missing primary key"
                )),
            );
            return Ok(CommitVote::Abort);
        }

        if self.database.active_transactions.is_empty() {
            log::error!("Should not be here");
            return Ok(CommitVote::Abort);
        }

        self.database.decrement_reply_count(&tid);
        self.database
            .update_proposed_ts_to_highest(&tid, proposed_ts);
        let result = self.database.run_nexts();

        self.database.release_locks();
        for r in result {
            self.done_xactions.insert(r.0, Ok(r.1));
        }

        Ok(CommitVote::InProgress)
    }
}

impl Actor for Repository {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        let actor_name = format!("{}", self.filename);
        println!("Starting actor: {actor_name}.");
    }
}

impl Handler<MessagePrepare> for Repository {
    type Result = anyhow::Result<CommitVote, anyhow::Error>;

    /// Handle for [`MessagePrepare`] for [`Repository`].
    fn handle(&mut self, msg: MessagePrepare, _ctx: &mut Self::Context) -> Self::Result {
        match msg {
            MessagePrepare::Single(tid, args) => self.handle_single(tid, args),
            MessagePrepare::Indep(tid, args, participants_len) => {
                self.handle_indep_prepare(tid, args, participants_len)
            }
            MessagePrepare::IndepParticipants(tid, vote, other_participants) => {
                self.send_message_accept_indep_to_participants(tid, vote, &other_participants)
            }
            MessagePrepare::Coord(tid, args, participants_len) => {
                self.handle_coord_prepare(tid, args, participants_len)
            }
            MessagePrepare::CoordParticipants(tid, vote, other_participants) => {
                self.send_message_accept_coord_to_participants(tid, vote, &other_participants)
            }
        }
    }
}

impl Handler<MessageAccept> for Repository {
    type Result = anyhow::Result<CommitVote, anyhow::Error>;

    /// Handle for [`MessageAccept`] for [`Repository`].
    fn handle(&mut self, msg: MessageAccept, _ctx: &mut Self::Context) -> Self::Result {
        match msg {
            MessageAccept::Indep(tid, proposed_ts, vote) => {
                self.handle_indep_accept(tid, proposed_ts, vote)
            }
            MessageAccept::Coord(tid, proposed_ts, vote) => {
                self.handle_coord_accept(tid, proposed_ts, vote)
            }
        }
    }
}

impl Handler<GetResult> for Repository {
    type Result = ResponseFuture<anyhow::Result<Option<Table>, anyhow::Error>>;

    /// Handle for [`GetResult`] for [`Repository`].
    /// If a result for the given `tid` is already in [`Repository::done_xaction`],
    /// remove it and return the value. If not, send a `GetResult` for the actor
    /// to try to get a result.
    fn handle(&mut self, msg: GetResult, ctx: &mut Self::Context) -> Self::Result {
        let tid = msg.0;
        if let Some(result) = self.done_xactions.remove(&tid) {
            return Box::pin(async move { result });
        } else {
            let request = ctx.address().send(GetResult(tid));
            Box::pin(async move { request.await.unwrap() })
        }
    }
}

impl Handler<GetProposedTs> for Repository {
    type Result = usize;

    /// Handle for [`GetProposedTs`] for [`Repository`].
    /// Needed for [`RepositoryWs`].
    fn handle(&mut self, msg: GetProposedTs, _ctx: &mut Self::Context) -> Self::Result {
        self.database.get_proposed_ts_for_tid(&msg.0)
    }
}
