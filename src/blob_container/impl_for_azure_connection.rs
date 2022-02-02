use crate::{connection::AzureStorageConnection, types::AzureStorageError};

use super::api::BlobContainersApi;

use async_trait::async_trait;
use hyper::Error;

#[async_trait]
impl BlobContainersApi for AzureStorageConnection {
    async fn create_container_if_not_exist(
        &self,
        container_name: &str,
    ) -> Result<(), AzureStorageError> {
        super::sdk::create_container_if_not_exist(self, container_name).await
    }

    async fn delete_container(&self, container_name: &str) -> Result<(), AzureStorageError> {
        super::sdk::delete_container(self, container_name).await
    }

    async fn delete_container_if_exists(
        &self,
        container_name: &str,
    ) -> Result<(), AzureStorageError> {
        super::sdk::delete_container_if_exists(self, container_name).await
    }

    async fn get_list_of_blob_containers(&self) -> Result<Vec<String>, Error> {
        super::sdk::get_list_of_blob_containers(self).await
    }

    async fn get_list_of_blobs(
        &self,
        container_name: &str,
    ) -> Result<Vec<String>, AzureStorageError> {
        super::sdk::get_list_of_blobs(self, container_name).await
    }
}
