macro_rules! find_max {
    ($x:expr) => ($x);
    ($x:expr, $($y:expr),+) => (
        std::cmp::max($x, find_max!($($y),+))
    )
}
pub(crate) use find_max;

macro_rules! repository {
    ($repo_name:ident, $filename:expr) => {
        use actix::dev::ToEnvelope;

        impl $repo_name {
            fn handle_single(
                &mut self,
                tid: Uuid,
                args: Arguments,
            ) -> anyhow::Result<crate::messages::CommitVote, anyhow::Error> {
                let runtime = &mut self.runtime;
                let current_time = runtime.now();
                let proposed_ts = find_max!(args.timestamp, current_time, self.last_timestamp) + 1;

                let operations_str = format!("{:?}", args.operations);
                let filename = format!("{}.txt", $filename);
                runtime.write_to_durable(&filename, &operations_str, proposed_ts)?;

                self.database
                    .add_xaction(&tid, proposed_ts, args.operations, 0);

                self.last_timestamp = proposed_ts;

                let result = self.database.run_nexts();

                for r in result {
                    self.done_xactions.insert(r.0, r.1?);
                }

                Ok(CommitVote::InProgress)
            }

            fn handle_indep_prepare(
                &mut self,
                tid: Uuid,
                args: Arguments,
                participants_len: usize,
            ) -> anyhow::Result<crate::messages::CommitVote, anyhow::Error> {
                let runtime = &mut self.runtime;
                let current_time = runtime.now();
                let proposed_ts = find_max!(args.timestamp, current_time, self.last_timestamp) + 1;

                let operations_str = format!("{:?}", args.operations);
                let filename = format!("{}.txt", $filename);
                runtime.write_to_durable(&filename, &operations_str, proposed_ts)?;

                self.database
                    .add_xaction(&tid, proposed_ts, args.operations, participants_len);

                if self.database.has_conflict(&tid) {
                    Ok(CommitVote::Conflict)
                } else {
                    Ok(CommitVote::Commit(None))
                }
            }

            fn send_message_accept_indep_to_participants<T>(
                &mut self,
                tid: Uuid,
                vote: CommitVote,
                other_participants: &Vec<Addr<T>>,
            ) -> anyhow::Result<crate::messages::CommitVote, anyhow::Error>
            where
                T: Actor + Handler<MessageAccept>,
                <T as actix::Actor>::Context: ToEnvelope<T, MessageAccept>,
            {
                let proposed_ts = self.database.get_proposed_ts_for_tid(&tid);
                for participant in other_participants {
                    participant.do_send(MessageAccept::Indep(tid, proposed_ts, vote.clone()));
                }

                self.last_timestamp = proposed_ts;
                Ok(CommitVote::InProgress)
            }

            fn handle_indep_accept(
                &mut self,
                tid: Uuid,
                proposed_ts: usize,
                vote: CommitVote,
            ) -> anyhow::Result<CommitVote, anyhow::Error> {
                // XXX: can it be abort??
                if vote == CommitVote::Conflict {
                    self.database.finalize(&tid, proposed_ts);
                    return Ok(CommitVote::Abort);
                }

                // Didn't get the first message `MessagePrepare`, so try later.
                if self.database.active_transactions.is_empty() {
                    println!("Should not be here");
                    return Ok(CommitVote::InProgress);
                }

                self.database.decrement_reply_count(&tid);
                self.database.update_proposed_ts(&tid, proposed_ts);
                let result = self.database.run_nexts();

                for r in result {
                    self.done_xactions.insert(r.0, r.1?);
                }

                Ok(CommitVote::InProgress)
            }

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
                let filename = format!("{}.txt", $filename);
                runtime.write_to_durable(&filename, &operations_str, proposed_ts)?;

                self.database
                    .add_xaction(&tid, proposed_ts, args.operations, participants_len);

                if self.database.has_conflict(&tid) {
                    Ok(CommitVote::Conflict)
                } else {
                    self.database.get_all_locks(&tid);
                    Ok(CommitVote::Commit(None))
                }
            }

            fn send_message_accept_coord_to_participants<T>(
                &mut self,
                tid: Uuid,
                vote: CommitVote,
                other_participants: &Vec<Addr<T>>,
            ) -> anyhow::Result<crate::messages::CommitVote, anyhow::Error>
            where
                T: Actor + Handler<MessageAccept>,
                <T as actix::Actor>::Context: ToEnvelope<T, MessageAccept>,
            {
                let proposed_ts = self.database.get_proposed_ts_for_tid(&tid);
                for participant in other_participants {
                    participant.do_send(MessageAccept::Coord(tid, proposed_ts, vote.clone()));
                }

                self.last_timestamp = proposed_ts;
                Ok(CommitVote::InProgress)
            }

            fn handle_coord_accept(
                &mut self,
                tid: Uuid,
                proposed_ts: usize,
                vote: CommitVote,
            ) -> anyhow::Result<CommitVote, anyhow::Error> {
                // XXX: can it be abort??
                if vote == CommitVote::Conflict {
                    self.database.finalize(&tid, proposed_ts);
                    return Ok(CommitVote::Abort);
                }

                if self.database.active_transactions.is_empty() {
                    println!("Should not be here");
                    return Ok(CommitVote::InProgress);
                }

                self.database.decrement_reply_count(&tid);
                self.database.update_proposed_ts(&tid, proposed_ts);
                let result = self.database.run_nexts();

                self.database.release_locks();
                for r in result {
                    self.done_xactions.insert(r.0, r.1?);
                }

                Ok(CommitVote::InProgress)
            }
        }

        impl Actor for $repo_name {
            type Context = Context<Self>;

            fn started(&mut self, _ctx: &mut Context<Self>) {
                let actor_name = format!("{}", $filename);
                println!("{actor_name} stated");
            }
        }

        impl<T> Handler<MessagePrepare<T>> for $repo_name
        where
            T: Actor + Handler<MessageAccept>,
            <T as actix::Actor>::Context: ToEnvelope<T, MessageAccept>,
        {
            type Result = anyhow::Result<CommitVote, anyhow::Error>;

            fn handle(&mut self, msg: MessagePrepare<T>, _ctx: &mut Self::Context) -> Self::Result {
                match msg {
                    MessagePrepare::Single(tid, args) => self.handle_single(tid, args),
                    MessagePrepare::Indep(tid, args, participants_len) => {
                        self.handle_indep_prepare(tid, args, participants_len)
                    }
                    MessagePrepare::IndepParticipants(tid, vote, other_participants) => self
                        .send_message_accept_indep_to_participants(tid, vote, &other_participants),
                    MessagePrepare::Coord(tid, args, participants_len) => {
                        self.handle_coord_prepare(tid, args, participants_len)
                    }
                    MessagePrepare::CoordParticipants(tid, vote, other_participants) => self
                        .send_message_accept_coord_to_participants(tid, vote, &other_participants),
                }
            }
        }

        impl Handler<MessageAccept> for $repo_name {
            type Result = anyhow::Result<CommitVote, anyhow::Error>;

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

        impl Handler<GetResult> for $repo_name {
            type Result = ResponseFuture<anyhow::Result<Option<usize>, anyhow::Error>>;

            fn handle(&mut self, msg: GetResult, ctx: &mut Self::Context) -> Self::Result {
                let tid = msg.0;
                if let Some(result) = self.done_xactions.remove(&tid) {
                    return Box::pin(async move { Ok(result) });
                } else {
                    let request = ctx.address().send(GetResult(tid));
                    Box::pin(async move { request.await.unwrap() })
                }
            }
        }
    };
}

pub(crate) use repository;
