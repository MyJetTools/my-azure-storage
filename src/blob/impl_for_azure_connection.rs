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
                crate::sdk_azure::blobs::get_blob_properties(
                    connection_data,
                    container_name,
                    blob_name,
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                crate::sdk_files::blobs::get_blob_properties(
                    connection_data,
                    container_name,
                    blob_name,
                )
                .await
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
                crate::sdk_azure::blobs::download(connection_data, container_name, blob_name).await
            }
            AzureStorageConnection::File(connection_data) => {
                crate::sdk_files::blobs::download(connection_data, container_name, blob_name).await
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
                crate::sdk_azure::blobs::delete(connection_data, container_name, blob_name).await
            }
            AzureStorageConnection::File(connection_data) => {
                crate::sdk_files::blobs::delete(connection_data, container_name, blob_name).await
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
                crate::sdk_azure::blobs::delete_if_exists(
                    connection_data,
                    container_name,
                    blob_name,
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                crate::sdk_files::blobs::delete_if_exists(
                    connection_data,
                    container_name,
                    blob_name,
                )
                .await
            }
        }
    }
}
