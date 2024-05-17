use std::collections::HashMap;

use actix::prelude::*;
use uuid::Uuid;

use crate::{
    database::Database,
    messages::{CommitVote, GetResult, MessageAccept, MessagePrepare},
    operations::Arguments,
    repository::{repository, find_max},
    runtime::Runtime,
};

pub(crate) struct Product {
    pub(crate) database: Database,
    pub(crate) runtime: Runtime,
    pub(crate) last_timestamp: usize,
    pub(crate) done_xactions: HashMap<Uuid, Option<usize>>,
}

impl Product {
    pub(crate) fn new() -> Self {
        Product {
            database: Database::new(),
            runtime: Runtime::new(),
            last_timestamp: 0,
            done_xactions: HashMap::new(),
        }
    }
}

repository!(Product, "Product");
