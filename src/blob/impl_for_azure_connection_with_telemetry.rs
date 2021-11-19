use crate::{
    connection::{AzureConnectionWithTelemetry, GetAzureConnectionInfo},
    types::AzureStorageError,
};
use async_trait::async_trait;
use my_telemetry::MyTelemetry;
use rust_extensions::StopWatch;

use super::{api::BlobApi, BlobProperties};

#[async_trait]
impl<TMyTelemetry: MyTelemetry + Send + Sync + 'static> BlobApi
    for AzureConnectionWithTelemetry<TMyTelemetry>
{
    async fn get_blob_properties(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<BlobProperties, AzureStorageError> {
        let connection = self.get_connection_info();

        let mut sw = StopWatch::new();
        sw.start();
        let result = super::sdk::get_blob_properties(connection, container_name, blob_name).await;
        sw.pause();

        let success = result.is_ok();

        self.track_dependency_duration(
            "blob/get_blob_properties".to_string(),
            success,
            sw.duration(),
        );

        return result;
    }

    async fn download_blob(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<Vec<u8>, AzureStorageError> {
        let connection = self.get_connection_info();
        let mut sw = StopWatch::new();
        sw.start();
        let result = super::sdk::download_blob(connection, container_name, blob_name).await;
        sw.pause();

        let success = result.is_ok();

        self.track_dependency_duration("blob/download_blob".to_string(), success, sw.duration());

        return result;
    }

    async fn delete_blob(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();
        let mut sw = StopWatch::new();
        sw.start();
        let result = super::sdk::delete_blob(connection, container_name, blob_name).await;

        sw.pause();

        let success = result.is_ok();

        self.track_dependency_duration("blob/delete_blob".to_string(), success, sw.duration());

        return result;
    }

    async fn delete_blob_if_exists(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();

        let mut sw = StopWatch::new();
        sw.start();
        let result = super::sdk::delete_blob_if_exists(connection, container_name, blob_name).await;

        sw.pause();

        let success = result.is_ok();

        self.track_dependency_duration(
            "blob/delete_blob_if_exists".to_string(),
            success,
            sw.duration(),
        );

        return result;
    }
}
