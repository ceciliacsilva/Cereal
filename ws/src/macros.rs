macro_rules! value {
    ($val:expr) => {
        Expr::Value($val)
    };
}

macro_rules! read {
    ($key:expr) => {
        Expr::Read($key)
    };
}

macro_rules! del {
    ($key:expr) => {
        Expr::Delete($key)
    };
}

macro_rules! op {
    ($exp:expr) => {
        Operation::Expr($exp)
    };
}

macro_rules! create {
    ($key:expr, $val:expr) => {
        Operation::Statement(Statement::Create($key, Box::new($val)))
    };
}

macro_rules! update {
    ($key:expr, $val:expr) => {
        Operation::Statement(Statement::Update($key, Box::new($val)))
    };
}

macro_rules! add {
    ($self:expr, $rhs:expr) => {
        Expr::Add(Box::new($self), Box::new($rhs))
    };
}

macro_rules! sub {
    ($self:expr, $rhs:expr) => {
        Expr::Sub(Box::new($self), Box::new($rhs))
    };
}

pub(crate) use {add, create, del, op, read, sub, update, value};
