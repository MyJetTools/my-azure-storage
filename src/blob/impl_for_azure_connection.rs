use crate::{connection::AzureStorageConnection, types::AzureStorageError};
use async_trait::async_trait;

use super::{api::BlobApi, BlobProperties};

#[async_trait]
impl BlobApi for AzureStorageConnection {
    async fn get_blob_properties(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<BlobProperties, AzureStorageError> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                super::sdk::get_blob_properties(connection_data, container_name, blob_name).await
            }
            AzureStorageConnection::File(connection_data) => {
                let file_name = crate::file_utils::compile_blob_path(
                    connection_data,
                    container_name,
                    blob_name,
                );
                return crate::file_utils::get_blob_properties(file_name.as_str()).await;
            }
        }
    }

    async fn download_blob(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<Vec<u8>, AzureStorageError> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                super::sdk::download_blob(connection_data, container_name, blob_name).await
            }
            AzureStorageConnection::File(connection_data) => {
                let file_name = crate::file_utils::compile_blob_path(
                    connection_data,
                    container_name,
                    blob_name,
                );

                let mut file = tokio::fs::File::open(file_name).await?;

                let mut result = Vec::new();
                tokio::io::AsyncReadExt::read_to_end(&mut file, &mut result).await?;
                return Ok(result);
            }
        }
    }

    async fn delete_blob(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<(), AzureStorageError> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                super::sdk::delete_blob(connection_data, container_name, blob_name).await
            }
            AzureStorageConnection::File(connection_data) => {
                let file_name = crate::file_utils::compile_blob_path(
                    connection_data,
                    container_name,
                    blob_name,
                );

                match tokio::fs::metadata(file_name.as_str()).await {
                    Ok(_) => tokio::fs::remove_file(file_name).await?,
                    Err(_) => return Err(AzureStorageError::BlobNotFound),
                }

                Ok(())
            }
        }
    }

    async fn delete_blob_if_exists(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<(), AzureStorageError> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                super::sdk::delete_blob_if_exists(connection_data, container_name, blob_name).await
            }
            AzureStorageConnection::File(connection_data) => {
                let file_name = crate::file_utils::compile_blob_path(
                    connection_data,
                    container_name,
                    blob_name,
                );

                match tokio::fs::remove_file(file_name).await {
                    Ok(_) => Ok(()),
                    Err(_) => Ok(()),
                }
            }
        }
    }
}
