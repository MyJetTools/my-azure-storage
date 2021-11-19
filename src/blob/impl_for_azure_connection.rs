use crate::{
    connection::{AzureConnection, GetAzureConnectionInfo},
    types::AzureStorageError,
};
use async_trait::async_trait;

use super::{api::BlobApi, BlobProperties};

#[async_trait]
impl BlobApi for AzureConnection {
    async fn get_blob_properties(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<BlobProperties, AzureStorageError> {
        let connection = self.get_connection_info();

        return super::sdk::get_blob_properties(connection, container_name, blob_name).await;
    }

    async fn download_blob(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<Vec<u8>, AzureStorageError> {
        let connection = self.get_connection_info();
        return super::sdk::download_blob(connection, container_name, blob_name).await;
    }

    async fn delete_blob(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();
        return super::sdk::delete_blob(connection, container_name, blob_name).await;
    }

    async fn delete_blob_if_exists(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();
        return super::sdk::delete_blob_if_exists(connection, container_name, blob_name).await;
    }
}
