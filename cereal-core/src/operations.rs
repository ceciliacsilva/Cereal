/// This should be a `blob` (bytes) like thing.
pub type Table = (usize, usize);
pub(crate) type PrimaryKey = usize;

/// This is a expression. Always returns something.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Expr {
    Value(Table),
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
