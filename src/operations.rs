pub(crate) type Table = (usize, usize);

/// This is a expression. Always returns something.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Operation {
    Value(Table),
    Create(usize, Box<Operation>),
    Read(usize),
    Update(usize, Box<Operation>),
    Delete(usize),
}

#[derive(Debug, Clone)]
pub(crate) struct Arguments {
    pub(crate) timestamp: usize,
    pub(crate) operations: Vec<Operation>,
}
