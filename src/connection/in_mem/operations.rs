use std::sync::Arc;

#[cfg(feature = "table-storage")]
use crate::table_storage::TableStorageError;
use crate::AzureStorageError;

#[cfg(feature = "table-storage")]
use super::TableStorageInMem;
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

#[cfg(feature = "table-storage")]
pub async fn get_table(
    storage_data: &MemStorageData,
    table_name: &str,
) -> Result<Arc<TableStorageInMem>, TableStorageError> {
    match storage_data.get_table(table_name).await {
        Some(result) => Ok(result),
        None => Err(TableStorageError::TableNotFound),
    }
}
