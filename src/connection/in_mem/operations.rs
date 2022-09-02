use std::sync::Arc;

use crate::AzureStorageError;

use super::{ContainerInMem, MemStorageData};

pub async fn get_container(
    storage_data: &MemStorageData,
    container_name: &str,
) -> Result<Arc<ContainerInMem>, AzureStorageError> {
    match storage_data.get_container(container_name).await {
        Some(result) => Ok(result),
        None => Err(AzureStorageError::ContainerNotFound),
    }
}
