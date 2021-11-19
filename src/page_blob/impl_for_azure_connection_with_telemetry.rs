use async_trait::async_trait;
use my_telemetry::MyTelemetry;
use rust_extensions::StopWatch;

use crate::{
    blob::BlobProperties,
    connection::{AzureConnectionWithTelemetry, GetAzureConnectionInfo},
    types::AzureStorageError,
};

use super::PageBlobApi;

#[async_trait]
impl<TMyTelemetry: MyTelemetry + Send + Sync + 'static> PageBlobApi
    for AzureConnectionWithTelemetry<TMyTelemetry>
{
    async fn create_page_blob(
        &self,
        container_name: &str,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();
        let mut sw = StopWatch::new();
        sw.start();
        let result =
            super::sdk::create_page_blob(connection, container_name, blob_name, pages_amount).await;

        let success = result.is_ok();

        self.track_dependency_duration(
            "page_blob/create_page_blob".to_string(),
            success,
            sw.duration(),
        );

        return result;
    }

    async fn create_page_blob_if_not_exists(
        &self,
        container_name: &str,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<BlobProperties, AzureStorageError> {
        let connection = self.get_connection_info();
        let mut sw = StopWatch::new();
        sw.start();
        let result = super::sdk::create_page_blob_if_not_exists(
            connection,
            container_name,
            blob_name,
            pages_amount,
        )
        .await;

        let success = result.is_ok();

        self.track_dependency_duration(
            "page_blob/create_page_blob_if_not_exists".to_string(),
            success,
            sw.duration(),
        );

        return result;
    }

    async fn resize_page_blob(
        &self,
        container_name: &str,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();
        let mut sw = StopWatch::new();
        sw.start();
        let result =
            super::sdk::resize_page_blob(connection, container_name, blob_name, pages_amount).await;

        let success = result.is_ok();

        self.track_dependency_duration(
            "page_blob/resize_page_blob".to_string(),
            success,
            sw.duration(),
        );

        return result;
    }

    async fn save_pages(
        &self,
        container_name: &str,
        blob_name: &str,
        start_page_no: usize,
        payload: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();
        let mut sw = StopWatch::new();
        sw.start();
        let result = super::sdk::save_pages(
            connection,
            container_name,
            blob_name,
            start_page_no,
            payload,
        )
        .await;

        let success = result.is_ok();

        self.track_dependency_duration("page_blob/save_pages".to_string(), success, sw.duration());

        return result;
    }

    async fn get_pages(
        &self,
        container_name: &str,
        blob_name: &str,
        start_page_no: usize,
        pages_to_read: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        let connection = self.get_connection_info();
        let mut sw = StopWatch::new();
        sw.start();
        let result = super::sdk::get_pages(
            connection,
            container_name,
            blob_name,
            start_page_no,
            pages_to_read,
        )
        .await;

        let success = result.is_ok();

        self.track_dependency_duration("page_blob/get_pages".to_string(), success, sw.duration());

        return result;
    }
}
