use hyper::Error;

#[derive(Debug)]
pub enum AzureStorageError {
    ContainerNotFound,
    BlobNotFound,
    BlobAlreadyExists,
    UnknownError { msg: String },
    HyperError { err: hyper::Error },
}

impl From<Error> for AzureStorageError {
    fn from(err: Error) -> Self {
        AzureStorageError::HyperError { err }
    }
}
