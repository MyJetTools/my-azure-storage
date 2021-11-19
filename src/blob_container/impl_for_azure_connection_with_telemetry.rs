use crate::{
    connection::{AzureConnectionWithTelemetry, GetAzureConnectionInfo},
    types::AzureStorageError,
};

use super::api::BlobContainersApi;

use async_trait::async_trait;
use hyper::Error;
use my_telemetry::MyTelemetry;

#[async_trait]
impl<TMyTelemetry: MyTelemetry + Send + Sync + 'static> BlobContainersApi
    for AzureConnectionWithTelemetry<TMyTelemetry>
{
    async fn create_container_if_not_exist(
        &self,
        container_name: &str,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();

        let result = super::sdk::create_container_if_not_exist(
            connection,
            container_name,
            self.telemetry.clone(),
        )
        .await;

        return result;
    }

    async fn delete_container(&self, container_name: &str) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();

        let result =
            super::sdk::delete_container(connection, container_name, self.telemetry.clone()).await;

        return result;
    }

    async fn delete_container_if_exists(
        &self,
        container_name: &str,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();

        let result = super::sdk::delete_container_if_exists(
            connection,
            container_name,
            self.telemetry.clone(),
        )
        .await;

        return result;
    }

    async fn get_list_of_blob_containers(&self) -> Result<Vec<String>, Error> {
        let connection = self.get_connection_info();

        let result =
            super::sdk::get_list_of_blob_containers(connection, self.telemetry.clone()).await;

        return result;
    }

    async fn get_list_of_blobs(
        &self,
        container_name: &str,
    ) -> Result<Vec<String>, AzureStorageError> {
        let connection = self.get_connection_info();

        let result =
            super::sdk::get_list_of_blobs(connection, container_name, self.telemetry.clone()).await;

        return result;
    }
}
