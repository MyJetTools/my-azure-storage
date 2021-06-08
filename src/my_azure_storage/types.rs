use hyper::Error;

#[derive(Debug)]
pub enum AzureStorageError {
    ContainerNotFound,
    BlobNotFound,
    BlobAlreadyExists,
    ContainerBeingDeleted,
    ContainerAlreadyExists,
    InvalidPageRange,
    UnknownError { msg: String },
    HyperError { err: hyper::Error },
}

impl From<Error> for AzureStorageError {
    fn from(err: Error) -> Self {
        AzureStorageError::HyperError { err }
    }
}

pub struct AzureItems<T> {
    pub next_marker: Option<String>,
    pub items: Vec<T>,
}
