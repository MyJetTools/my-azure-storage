use crate::{connection::AzureStorageConnection, AzureStorageError};

use super::api::BlobContainersApi;

use async_trait::async_trait;
use hyper::Error;

#[async_trait]
impl BlobContainersApi for AzureStorageConnection {
    async fn create_container_if_not_exist(
        &self,
        container_name: &str,
    ) -> Result<(), AzureStorageError> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::sdk_azure::containers::create_if_not_exists(connection_data, container_name)
                    .await
            }
            AzureStorageConnection::File(connection_data) => {
                crate::sdk_files::containers::create_if_not_exists(connection_data, container_name)
                    .await
            }
            AzureStorageConnection::InMemory(connection_data) => {
                connection_data.create(container_name).await;
                Ok(())
            }
        }
    }

    async fn delete_container(&self, container_name: &str) -> Result<(), AzureStorageError> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::sdk_azure::containers::delete(connection_data, container_name).await
            }
            AzureStorageConnection::File(connection_data) => {
                crate::sdk_files::containers::delete(connection_data, container_name).await
            }
            AzureStorageConnection::InMemory(connection_data) => {
                if connection_data.delete(container_name).await {
                    Ok(())
                } else {
                    Err(AzureStorageError::ContainerNotFound)
                }
            }
        }
    }

    async fn delete_container_if_exists(
        &self,
        container_name: &str,
    ) -> Result<(), AzureStorageError> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::sdk_azure::containers::delete_if_exists(connection_data, container_name)
                    .await
            }
            AzureStorageConnection::File(connection_data) => {
                crate::sdk_files::containers::delete_if_exists(connection_data, container_name)
                    .await
            }
            AzureStorageConnection::InMemory(connection_data) => {
                connection_data.delete(container_name).await;
                Ok(())
            }
        }
    }

    async fn get_list_of_blob_containers(&self) -> Result<Vec<String>, Error> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::sdk_azure::containers::get_list(connection_data).await
            }
            AzureStorageConnection::File(connection_data) => {
                crate::sdk_files::containers::get_list(connection_data).await
            }
            AzureStorageConnection::InMemory(connection_data) => {
                Ok(connection_data.get_list().await)
            }
        }
    }

    async fn get_list_of_blobs(
        &self,
        container_name: &str,
    ) -> Result<Vec<String>, AzureStorageError> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::sdk_azure::blobs::get_list(connection_data, container_name).await
            }
            AzureStorageConnection::File(connection_data) => {
                crate::sdk_files::blobs::get_list(connection_data, container_name).await
            }
            AzureStorageConnection::InMemory(connection_data) => {
                let container = crate::connection::in_mem::operations::get_container(
                    connection_data,
                    container_name,
                )
                .await?;

                Ok(container.get_list_of_blobs().await)
            }
        }
    }
}
