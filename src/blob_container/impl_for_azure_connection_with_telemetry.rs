use crate::{
    connection::{AzureConnectionWithTelemetry, GetAzureConnectionInfo},
    types::AzureStorageError,
};

use super::api::BlobContainersApi;

use async_trait::async_trait;
use hyper::Error;
use my_telemetry::MyTelemetry;
use rust_extensions::StopWatch;

#[async_trait]
impl<TMyTelemetry: MyTelemetry + Send + Sync + 'static> BlobContainersApi
    for AzureConnectionWithTelemetry<TMyTelemetry>
{
    async fn create_container_if_not_exist(
        &self,
        container_name: &str,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();

        let mut sw = StopWatch::new();
        sw.start();
        let result = super::sdk::create_container_if_not_exist(connection, container_name).await;

        sw.pause();

        let success = result.is_ok();

        self.track_dependency_duration(
            "blob_container/create_container_if_not_exist".to_string(),
            success,
            sw.duration(),
        );

        return result;
    }

    async fn delete_container(&self, container_name: &str) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();

        let mut sw = StopWatch::new();
        sw.start();
        let result = super::sdk::delete_container(connection, container_name).await;
        sw.pause();

        let success = result.is_ok();

        self.track_dependency_duration(
            "blob_container/delete_container".to_string(),
            success,
            sw.duration(),
        );

        return result;
    }

    async fn delete_container_if_exists(
        &self,
        container_name: &str,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();
        let mut sw = StopWatch::new();
        sw.start();
        let result = super::sdk::delete_container_if_exists(connection, container_name).await;
        sw.pause();

        let success = result.is_ok();

        self.track_dependency_duration(
            "blob_container/delete_container_if_exists".to_string(),
            success,
            sw.duration(),
        );

        return result;
    }

    async fn get_list_of_blob_containers(&self) -> Result<Vec<String>, Error> {
        let connection = self.get_connection_info();

        let mut sw = StopWatch::new();
        sw.start();
        let result = super::sdk::get_list_of_blob_containers(connection).await;

        sw.pause();

        let success = result.is_ok();

        self.track_dependency_duration(
            "blob_container/get_list_of_blob_containers".to_string(),
            success,
            sw.duration(),
        );

        return result;
    }

    async fn get_list_of_blobs(
        &self,
        container_name: &str,
    ) -> Result<Vec<String>, AzureStorageError> {
        let connection = self.get_connection_info();

        let mut sw = StopWatch::new();
        sw.start();
        let result = super::sdk::get_list_of_blobs(connection, container_name).await;

        sw.pause();

        let success = result.is_ok();

        self.track_dependency_duration(
            "blob_container/get_list_of_blobs".to_string(),
            success,
            sw.duration(),
        );

        return result;
    }
}
