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
        return super::sdk::get_blob_properties(self, container_name, blob_name).await;
    }

    async fn download_blob(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<Vec<u8>, AzureStorageError> {
        return super::sdk::download_blob(self, container_name, blob_name).await;
    }

    async fn delete_blob(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<(), AzureStorageError> {
        return super::sdk::delete_blob(self, container_name, blob_name).await;
    }

    async fn delete_blob_if_exists(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<(), AzureStorageError> {
        return super::sdk::delete_blob_if_exists(self, container_name, blob_name).await;
    }
}
