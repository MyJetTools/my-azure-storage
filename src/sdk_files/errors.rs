use crate::AzureStorageError;

impl From<std::io::Error> for AzureStorageError {
    fn from(src: std::io::Error) -> Self {
        AzureStorageError::IoError(src)
    }
}

pub fn handle_error_on_file_level<TOk>(
    src: Result<TOk, std::io::Error>,
) -> Result<TOk, AzureStorageError> {
    match src {
        Ok(ok) => Ok(ok),
        Err(err) => match err.kind() {
            std::io::ErrorKind::NotFound => {
                return Err(AzureStorageError::BlobNotFound);
            }
            _ => {
                return Err(AzureStorageError::UnknownError {
                    msg: err.to_string(),
                });
            }
        },
    }
}
