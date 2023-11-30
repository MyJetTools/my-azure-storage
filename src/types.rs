#[derive(Debug)]
pub enum AzureStorageError {
    ContainerNotFound,
    BlobNotFound,
    BlobAlreadyExists,
    ContainerBeingDeleted,
    ContainerAlreadyExists,
    InvalidPageRange,
    RequestBodyTooLarge,
    InvalidResourceName,
    IoError(std::io::Error),
    FlUrlError(flurl::FlUrlError),
    Timeout,
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
            "InvalidResourceName" => AzureStorageError::InvalidResourceName,
            _ => {
                println!("Unknown error is found: {:?}", str);
                AzureStorageError::UnknownError {
                    msg: str.to_string(),
                }
            }
        }
    }
}

impl From<flurl::FlUrlError> for AzureStorageError {
    fn from(err: flurl::FlUrlError) -> Self {
        Self::FlUrlError(err)
    }
}
