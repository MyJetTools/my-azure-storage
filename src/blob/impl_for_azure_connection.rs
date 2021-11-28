use crate::{connection::AzureStorageConnection, types::AzureStorageError};
use async_trait::async_trait;
use my_telemetry::MyTelemetryToConsole;

use super::{api::BlobApi, BlobProperties};

#[async_trait]
impl BlobApi for AzureStorageConnection {
    async fn get_blob_properties(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<BlobProperties, AzureStorageError> {
        let connection = self.get_connection_info();

        return super::sdk::get_blob_properties::<MyTelemetryToConsole>(
            connection,
            container_name,
            blob_name,
            None,
        )
        .await;
    }

    async fn download_blob(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<Vec<u8>, AzureStorageError> {
        let connection = self.get_connection_info();
        return super::sdk::download_blob::<MyTelemetryToConsole>(
            connection,
            container_name,
            blob_name,
            None,
        )
        .await;
    }

    async fn delete_blob(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();
        return super::sdk::delete_blob::<MyTelemetryToConsole>(
            connection,
            container_name,
            blob_name,
            None,
        )
        .await;
    }

    async fn delete_blob_if_exists(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();
        return super::sdk::delete_blob_if_exists::<MyTelemetryToConsole>(
            connection,
            container_name,
            blob_name,
            None,
        )
        .await;
    }
}
