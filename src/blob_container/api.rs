use async_trait::async_trait;
use hyper::Error;

use crate::AzureStorageError;

#[async_trait]
pub trait BlobContainersApi {
    async fn create_container_if_not_exist(
        &self,
        container_name: &str,
    ) -> Result<(), AzureStorageError>;

    async fn delete_container(&self, container_name: &str) -> Result<(), AzureStorageError>;

    async fn delete_container_if_exists(
        &self,
        container_name: &str,
    ) -> Result<(), AzureStorageError>;

    async fn get_list_of_blob_containers(&self) -> Result<Vec<String>, Error>;

    async fn get_list_of_blobs(
        &self,
        container_name: &str,
    ) -> Result<Vec<String>, AzureStorageError>;
}
