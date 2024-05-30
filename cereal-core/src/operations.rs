use std::ops::{Add, Sub};

/// This should be a `blob` (bytes) like thing.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Table(pub i64, pub i64);
pub(crate) type PrimaryKey = usize;

/// This is a expression. Always returns something.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Expr {
    Value(Table),
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Read(PrimaryKey),
    Delete(PrimaryKey),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Statement {
    Create(PrimaryKey, Box<Expr>),
    Update(PrimaryKey, Box<Expr>),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Operation {
    Expr(Expr),
    Statement(Statement),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Arguments {
    pub timestamp: usize,
    pub operations: Vec<Operation>,
}

impl Add for Table {
    type Output = Table;

    fn add(self, rhs: Self) -> Self::Output {
        Table(self.0 + rhs.0, self.1 + rhs.1)
    }
}

impl Sub for Table {
    type Output = Table;

    fn sub(self, rhs: Self) -> Self::Output {
        Table(self.0 - rhs.0, self.1 - rhs.1)
    }
}
