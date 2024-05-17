use std::collections::HashMap;

use actix::prelude::*;
use uuid::Uuid;

use crate::{
    database::Database,
    messages::{CommitVote, GetResult, MessageAccept, MessagePrepare},
    operations::Arguments,
    runtime::Runtime,
    repository::{repository, find_max},
};

pub(crate) struct Order {
    pub(crate) database: Database,
    pub(crate) runtime: Runtime,
    pub(crate) last_timestamp: usize,
    pub(crate) done_xactions: HashMap<Uuid, Option<usize>>,
}

impl Order {
    pub(crate) fn new() -> Self {
        Order {
            database: Database::new(),
            runtime: Runtime::new(),
            last_timestamp: 0,
            done_xactions: HashMap::new(),
        }
    }
}

repository!(Order, "Order");
