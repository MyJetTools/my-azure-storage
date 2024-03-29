use flurl::FlUrlError;

#[derive(Debug)]
pub enum TableStorageError {
    TableNotFound,
    EntityAlreadyExists,
    ResourceNotFound,
    TableAlreadyExists,
    FlUrlError(FlUrlError),
    Unknown(String),
}

impl From<FlUrlError> for TableStorageError {
    fn from(src: FlUrlError) -> Self {
        Self::FlUrlError(src)
    }
}
