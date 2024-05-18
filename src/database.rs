use std::collections::{BTreeMap, HashMap, HashSet};
use uuid::Uuid;

use crate::operations::{Expr, Operation, PrimaryKey, Statement, Table};

#[derive(Debug, Clone)]
pub(crate) struct Database {
    pub(crate) data_structure: BTreeMap<PrimaryKey, Table>,
    pub(crate) active_transactions: BTreeMap<Uuid, Transaction>,
    pub(crate) locked_keys: HashSet<PrimaryKey>,
    pub(crate) tid_to_ts_end_xaction_ends: HashMap<Uuid, usize>,
}

impl Database {
    pub(crate) fn new() -> Self {
        Database {
            data_structure: BTreeMap::new(),
            active_transactions: BTreeMap::new(),
            locked_keys: HashSet::new(),
            tid_to_ts_end_xaction_ends: HashMap::new(),
        }
    }

    pub(crate) fn add_xaction(
        &mut self,
        tid: &Uuid,
        proposed_ts: usize,
        operation: Vec<Operation>,
        participants_len: usize,
    ) {
        let xaction = Transaction::new(proposed_ts, participants_len, true, operation);
        self.active_transactions.insert(*tid, xaction);
    }

    pub(crate) fn decrement_reply_count(&mut self, tid: &Uuid) {
        self.active_transactions
            .entry(*tid)
            .and_modify(|xaction| xaction.waiting_for -= 1);
    }

    // XXX: This could have a better naming...
    //
    // The ideia here is to keep the highest proposed_ts between the current
    // one and the new one.
    pub(crate) fn update_proposed_ts_to_highest(&mut self, tid: &Uuid, ts: usize) {
        self.active_transactions.entry(*tid).and_modify(|xaction| {
            xaction.proposed_ts = if xaction.proposed_ts > ts {
                xaction.proposed_ts
            } else {
                ts
            }
        });
    }

    pub(crate) fn finalize(&mut self, tid: &Uuid, ts: usize) {
        self.active_transactions.remove(tid);
        self.tid_to_ts_end_xaction_ends.insert(*tid, ts);
    }

    pub(crate) fn get_proposed_ts_for_tid(&self, tid: &Uuid) -> usize {
        if let Some(xaction) = self.active_transactions.get(tid) {
            return xaction.proposed_ts;
        }
        if let Some(ts) = self.tid_to_ts_end_xaction_ends.get(tid) {
            return *ts;
        }
        unreachable!()
    }

    pub(crate) fn set_next_to_run(&mut self) -> Option<Uuid> {
        let mut next_to_run_tid = None;
        let mut smaller_ts = usize::max_value();
        for xaction in &mut self.active_transactions {
            xaction.1.next_to_run = false;
            if xaction.1.proposed_ts < smaller_ts && xaction.1.waiting_for == 0 {
                next_to_run_tid = Some(xaction.0.clone());
                smaller_ts = xaction.1.proposed_ts.clone();
            }
        }

        next_to_run_tid.and_then(|next_tid| {
            self.active_transactions
                .entry(next_tid)
                .and_modify(|xaction| xaction.next_to_run = true);
            Some(next_tid.clone())
        })
    }

    /// Check that all needed keys exist and are `free` (not held by a `Coord` transaction).
    pub(crate) fn check_for_conflicts_and_primary_key(&self, tid: &Uuid) -> bool {
        let mut has_problem = false;
        if let Some(xaction) = self.active_transactions.get(tid) {
            let operations = &xaction.operations;
            for op in operations {
                match op {
                    Operation::Statement(Statement::Create(key, _)) => {
                        if self.locked_keys.contains(key) {
                            has_problem = true;
                            break;
                        }
                    }
                    Operation::Expr(Expr::Read(key)) => {
                        if self.locked_keys.contains(key) {
                            has_problem = true;
                            break;
                        }
                        if !self.data_structure.contains_key(key) {
                            has_problem = true;
                            break;
                        }
                    }
                    Operation::Statement(Statement::Update(key, _)) => {
                        if self.locked_keys.contains(key) {
                            has_problem = true;
                            break;
                        }
                    }
                    Operation::Expr(Expr::Delete(key)) => {
                        if self.locked_keys.contains(key) {
                            has_problem = true;
                            break;
                        }
                        if !self.data_structure.contains_key(key) {
                            has_problem = true;
                            break;
                        }
                    }
                    Operation::Expr(Expr::Value(_)) => (),
                }
            }
        }
        has_problem
    }

    pub(crate) fn get_all_locks(&mut self, tid: &Uuid) {
        if let Some(xaction) = self.active_transactions.get(tid) {
            let operations = &xaction.operations;
            for op in operations {
                match op {
                    Operation::Statement(Statement::Create(key, _)) => {
                        self.locked_keys.insert(*key);
                    }
                    Operation::Expr(Expr::Read(key)) => {
                        self.locked_keys.insert(*key);
                    }
                    Operation::Statement(Statement::Update(key, _)) => {
                        self.locked_keys.insert(*key);
                    }
                    Operation::Expr(Expr::Delete(key)) => {
                        self.locked_keys.insert(*key);
                    }
                    Operation::Expr(Expr::Value(_)) => (),
                }
            }
        }
    }

    pub(crate) fn release_locks(&mut self) {
        self.locked_keys.clear();
    }

    fn eval_operation(database: &mut BTreeMap<PrimaryKey, Table>, op: &Operation) -> Option<Table> {
        match op {
            Operation::Statement(Statement::Create(key, expr)) => {
                let value = Self::eval_operation(database, &Operation::Expr(*expr.to_owned()));
                database.insert(
                    key.to_owned(),
                    value.expect("eval_operation didn't return a valid `Table`."),
                )
            }
            // TODO: can I remove the cloned?
            Operation::Expr(Expr::Read(key)) => database.get(key).cloned(),
            Operation::Statement(Statement::Update(key, expr)) => {
                let value = Self::eval_operation(database, &Operation::Expr(*expr.to_owned()));
                database
                    .entry(key.to_owned())
                    .and_modify(|v| {
                        *v = value.expect("eval_operation didn't return a valid `Table`.")
                    })
                    .or_insert(value?);
                value
            }
            Operation::Expr(Expr::Delete(key)) => database.remove(key),
            Operation::Expr(Expr::Value(value)) => Some(*value),
        }
    }

    pub(crate) fn run_operations(&mut self, tid: &Uuid) -> Option<Table> {
        let mut result = vec![];
        if let Some(xaction) = self.active_transactions.get(tid) {
            let operations = &xaction.operations;
            for op in operations {
                result.push(Self::eval_operation(&mut self.data_structure, op));
            }
            self.finalize(tid, xaction.proposed_ts);
        }
        // TODO: handle unwrap()
        *result.last().unwrap()
    }

    /// Run all `next available to run` transactions, by lowest timestamp and got all needed responses back.
    pub(crate) fn run_nexts(&mut self) -> HashMap<Uuid, Option<Table>> {
        let mut result = HashMap::new();
        while let Some(tid) = self.set_next_to_run() {
            if self.check_for_conflicts_and_primary_key(&tid) {
                result.insert(tid.clone(), None);
                return result;
            }
            result.insert(tid.clone(), self.run_operations(&tid));
        }

        result
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Transaction {
    pub(crate) proposed_ts: usize,
    pub(crate) waiting_for: usize,
    pub(crate) next_to_run: bool,
    pub(crate) operations: Vec<Operation>,
}

impl Transaction {
    pub(crate) fn new(
        proposed_ts: usize,
        waiting_for: usize,
        next_to_run: bool,
        operations: Vec<Operation>,
    ) -> Self {
        Transaction {
            proposed_ts,
            waiting_for,
            next_to_run,
            operations,
        }
    }
}
