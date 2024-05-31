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

    fn check_for_problems_per_operation(&self, op: &Operation) -> bool {
        match op {
            Operation::Statement(Statement::Create(key, expr)) => {
                if self.locked_keys.contains(key) {
                    return true;
                }
                return self.check_for_problems_per_operation(&Operation::Expr((**expr).clone()));
            }
            Operation::Expr(Expr::Read(key)) => {
                if self.locked_keys.contains(key) {
                    return true;
                }
                if !self.data_structure.contains_key(key) {
                    return true;
                }
            }
            Operation::Statement(Statement::Update(key, expr)) => {
                if self.locked_keys.contains(key) {
                    return true;
                }

                return self.check_for_problems_per_operation(&Operation::Expr((**expr).clone()));
            }
            Operation::Expr(Expr::Delete(key)) => {
                if self.locked_keys.contains(key) {
                    return true;
                }
                if !self.data_structure.contains_key(key) {
                    return true;
                }
            }
            Operation::Expr(Expr::Value(_)) => (),
            Operation::Expr(Expr::Add(e1, e2)) | Operation::Expr(Expr::Sub(e1, e2)) => {
                return self.check_for_problems_per_operation(&Operation::Expr((**e1).clone()))
                    || self.check_for_problems_per_operation(&Operation::Expr((**e2).clone()));
            }
        }
        false
    }

    /// Check that all needed keys exist and are `free` (not held by a `Coord` transaction).
    pub(crate) fn check_for_conflicts_and_primary_key(&self, tid: &Uuid) -> bool {
        if let Some(xaction) = self.active_transactions.get(tid) {
            let operations = &xaction.operations;
            for op in operations {
                if self.check_for_problems_per_operation(op) {
                    return true;
                }
            }
        }
        false
    }

    pub fn get_lock_per_operation(&mut self, op: &Operation) {
        match op {
            Operation::Statement(Statement::Create(key, expr)) => {
                self.locked_keys.insert(*key);
                self.get_lock_per_operation(&Operation::Expr((**expr).clone()));
            }
            Operation::Expr(Expr::Read(key)) => {
                self.locked_keys.insert(*key);
            }
            Operation::Statement(Statement::Update(key, expr)) => {
                self.locked_keys.insert(*key);
                self.get_lock_per_operation(&Operation::Expr((**expr).clone()));
            }
            Operation::Expr(Expr::Delete(key)) => {
                self.locked_keys.insert(*key);
            }
            Operation::Expr(Expr::Value(_)) => (),
            Operation::Expr(Expr::Add(e1, e2)) | Operation::Expr(Expr::Sub(e1, e2)) => {
                self.get_lock_per_operation(&Operation::Expr((**e1).clone()));
                self.get_lock_per_operation(&Operation::Expr((**e2).clone()));
            }
        }
    }

    pub(crate) fn get_all_locks(&mut self, tid: &Uuid) {
        if let Some(xaction) = self.active_transactions.get(tid) {
            // TODO: Do I really need to clone here?
            let operations = &xaction.operations.clone();
            for op in operations {
                self.get_lock_per_operation(op);
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
                log::info!("{:?}", value);
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
                        *v = value
                            .clone()
                            .expect("eval_operation didn't return a valid `Table`.")
                    })
                    .or_insert(value.clone()?);
                value
            }
            Operation::Expr(Expr::Delete(key)) => database.remove(key),
            Operation::Expr(Expr::Value(value)) => Some(value.clone()),
            Operation::Expr(Expr::Add(expr, rhs)) => Some(
                Self::eval_operation(database, &Operation::Expr(*expr.to_owned()))?
                    + Self::eval_operation(database, &Operation::Expr(*rhs.to_owned()))?,
            ),
            Operation::Expr(Expr::Sub(expr, rhs)) => Some(
                Self::eval_operation(database, &Operation::Expr(*expr.to_owned()))?
                    - Self::eval_operation(database, &Operation::Expr(*rhs.to_owned()))?,
            ),
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
        result.last()?.clone()
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

mod tests {
    use super::*;

    #[test]
    fn test_add_xaction_and_decrement_xaction() {
        let mut database = Database::new();
        let tid = Uuid::new_v4();
        let participants_len = 2;
        database.add_xaction(&tid, 0, vec![], participants_len);
        database.decrement_reply_count(&tid);
        if let Some(xaction) = database.active_transactions.get(&tid) {
            assert_eq!(xaction.waiting_for, participants_len - 1);
        }
    }

    #[test]
    fn test_set_next_to_run_waiting_0() {
        let mut database = Database::new();
        let tid = Uuid::new_v4();
        let participants_len = 1;
        database.add_xaction(&tid, 0, vec![], participants_len);
        database.decrement_reply_count(&tid);
        let tid_next = database.set_next_to_run();
        assert_eq!(Some(tid), tid_next);
    }

    #[test]
    fn test_not_next_to_run_if_still_waiting() {
        let mut database = Database::new();
        let tid = Uuid::new_v4();
        let participants_len = 2;
        database.add_xaction(&tid, 0, vec![], participants_len);
        database.decrement_reply_count(&tid);
        let tid_next = database.set_next_to_run();
        assert_eq!(None, tid_next);
    }

    #[test]
    fn test_next_to_run_smallest_ts() {
        let mut database = Database::new();
        let participants_len = 0;

        let tid_ts_bigger = Uuid::new_v4();
        database.add_xaction(&tid_ts_bigger, 10, vec![], participants_len);
        let tid_ts_smaller = Uuid::new_v4();
        database.add_xaction(&tid_ts_smaller, 4, vec![], participants_len);

        assert_eq!(Some(tid_ts_smaller), database.set_next_to_run());
        database.run_operations(&tid_ts_smaller);
        assert_eq!(Some(tid_ts_bigger), database.set_next_to_run());
    }

    #[test]
    fn test_run_nexts() {
        let mut database = Database::new();
        database.data_structure.insert(0, Table(0, 0));
        database.data_structure.insert(1, Table(1, 1));
        database.data_structure.insert(2, Table(2, 2));

        let participants_len = 0;
        let tid_0 = Uuid::new_v4();
        database.add_xaction(
            &tid_0,
            0,
            vec![Operation::Expr(Expr::Read(0))],
            participants_len,
        );
        let tid_1 = Uuid::new_v4();
        database.add_xaction(
            &tid_1,
            1,
            vec![Operation::Expr(Expr::Read(1))],
            participants_len,
        );
        let tid_2 = Uuid::new_v4();
        database.add_xaction(
            &tid_2,
            2,
            vec![Operation::Expr(Expr::Read(2))],
            participants_len,
        );

        let result = database.run_nexts();
        assert_eq!(result.get(&tid_0), Some(&Some(Table(0, 0))));
        assert_eq!(result.get(&tid_1), Some(&Some(Table(1, 1))));
        assert_eq!(result.get(&tid_2), Some(&Some(Table(2, 2))));
    }

    #[test]
    fn get_all_locks() {
        let mut database = Database::new();
        database.data_structure.insert(0, Table(0, 0));

        let participants_len = 0;
        let tid = Uuid::new_v4();
        database.add_xaction(
            &tid,
            0,
            vec![Operation::Expr(Expr::Read(0))],
            participants_len,
        );

        database.get_all_locks(&tid);

        assert!(database.locked_keys.contains(&0));
    }

    #[test]
    fn test_update_ts() {
        let mut database = Database::new();

        let participants_len = 0;
        let tid = Uuid::new_v4();
        database.add_xaction(&tid, 0, vec![], participants_len);

        database.update_proposed_ts_to_highest(&tid, 4);
        assert_eq!(
            database.active_transactions.get(&tid).unwrap().proposed_ts,
            4
        );

        database.update_proposed_ts_to_highest(&tid, 2);
        assert_eq!(
            database.active_transactions.get(&tid).unwrap().proposed_ts,
            4
        );
    }

    #[test]
    fn test_finalize() {
        let mut database = Database::new();

        let participants_len = 0;
        let tid = Uuid::new_v4();
        database.add_xaction(&tid, 0, vec![], participants_len);

        database.finalize(&tid, 1);

        assert!(database.active_transactions.is_empty());
        assert_eq!(database.tid_to_ts_end_xaction_ends.get(&tid), Some(&1));
    }
}
