#[derive(Debug, Clone)]
pub(crate) enum Operation {
    Create(usize, usize),
    Read(usize),
    Update(usize, usize),
    Delete(usize),
}

#[derive(Debug, Clone)]
pub(crate) struct Arguments {
    pub(crate) timestamp: usize,
    pub(crate) operations: Vec<Operation>,
}
