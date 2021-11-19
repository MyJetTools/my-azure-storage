use crate::{
    connection::{AzureConnection, GetAzureConnectionInfo},
    types::AzureStorageError,
};

use super::api::BlobContainersApi;

use async_trait::async_trait;
use hyper::Error;
use my_telemetry::MyTelemetryToConsole;

#[async_trait]
impl BlobContainersApi for AzureConnection {
    async fn create_container_if_not_exist(
        &self,
        container_name: &str,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();
        super::sdk::create_container_if_not_exist::<MyTelemetryToConsole>(
            connection,
            container_name,
            None,
        )
        .await
    }

    async fn delete_container(&self, container_name: &str) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();
        super::sdk::delete_container::<MyTelemetryToConsole>(connection, container_name, None).await
    }

    async fn delete_container_if_exists(
        &self,
        container_name: &str,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();
        super::sdk::delete_container_if_exists::<MyTelemetryToConsole>(
            connection,
            container_name,
            None,
        )
        .await
    }

    async fn get_list_of_blob_containers(&self) -> Result<Vec<String>, Error> {
        let connection = self.get_connection_info();
        super::sdk::get_list_of_blob_containers::<MyTelemetryToConsole>(connection, None).await
    }

    async fn get_list_of_blobs(
        &self,
        container_name: &str,
    ) -> Result<Vec<String>, AzureStorageError> {
        let connection = self.get_connection_info();
        super::sdk::get_list_of_blobs::<MyTelemetryToConsole>(connection, container_name, None)
            .await
    }
}
