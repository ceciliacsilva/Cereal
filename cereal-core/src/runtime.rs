use std::fs::File;
use std::io::Write;

use tempfile::TempDir;

#[derive(Debug)]
pub struct Runtime {
    dir: TempDir,
    current_time: usize,
}

impl Runtime {
    pub fn new() -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();

        let initial_time: usize = 10;

        Runtime {
            dir: tmp_dir,
            current_time: initial_time,
        }
    }

    // TODO: super bad that this need a `&mut self`.
    pub fn now(&mut self) -> usize {
        self.current_time += 1;
        self.current_time
    }

    pub(crate) fn write_to_durable(
        &self,
        filename: &str,
        request: &str,
        ts: usize,
    ) -> anyhow::Result<()> {
        let mut file = File::create(self.dir.path().join(filename))?;

        let _ = writeln!(file, "{request}, {ts}\n")?;
        Ok(())
    }
}
