use std::path::{Path, PathBuf};

pub struct Datalayer {
    domain_path: PathBuf,
}
impl Datalayer {
    /// Opens a new datalayer. You can have multiple datalayers. So we have to define the domain. This is basically a file system path
    fn open(domain: impl AsRef<Path>) -> Self {
        Self {
            domain_path: domain.as_ref().to_path_buf(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        Datalayer::open("test");
    }
}
