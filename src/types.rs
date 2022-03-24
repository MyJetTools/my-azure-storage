#[derive(Debug)]
pub enum AzureStorageError {
    ContainerNotFound,
    BlobNotFound,
    BlobAlreadyExists,
    ContainerBeingDeleted,
    ContainerAlreadyExists,
    InvalidPageRange,
    RequestBodyTooLarge,
    IoError(std::io::Error),
    HyperError(hyper::Error),
    UnknownError { msg: String },
}

impl AzureStorageError {
    pub fn parse(str: &str) -> Self {
        match str {
            "ContainerNotFound" => AzureStorageError::ContainerNotFound,
            "ContainerBeingDeleted" => AzureStorageError::ContainerBeingDeleted,
            "BlobNotFound" => AzureStorageError::BlobNotFound,
            "ContainerAlreadyExists" => AzureStorageError::ContainerAlreadyExists,
            "InvalidPageRange" => AzureStorageError::InvalidPageRange,
            "RequestBodyTooLarge" => AzureStorageError::RequestBodyTooLarge,
            _ => {
                println!("Unknown error is found: {:?}", str);
                AzureStorageError::UnknownError {
                    msg: str.to_string(),
                }
            }
        }
    }
}

impl From<hyper::Error> for AzureStorageError {
    fn from(err: hyper::Error) -> Self {
        AzureStorageError::HyperError(err)
    }
}

impl From<std::io::Error> for AzureStorageError {
    fn from(src: std::io::Error) -> Self {
        AzureStorageError::IoError(src)
    }
}
