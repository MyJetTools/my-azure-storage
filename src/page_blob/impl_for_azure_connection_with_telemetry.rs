use async_trait::async_trait;
use my_telemetry::MyTelemetry;

use crate::{
    blob::BlobProperties, connection::AzureStorageConnectionWithTelemetry, types::AzureStorageError,
};

use super::PageBlobApi;

#[async_trait]
impl<TMyTelemetry: MyTelemetry + Send + Sync + 'static> PageBlobApi
    for AzureStorageConnectionWithTelemetry<TMyTelemetry>
{
    async fn create_page_blob(
        &self,
        container_name: &str,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();

        return super::sdk::create_page_blob(
            connection,
            container_name,
            blob_name,
            pages_amount,
            self.telemetry.clone(),
        )
        .await;
    }

    async fn create_page_blob_if_not_exists(
        &self,
        container_name: &str,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<BlobProperties, AzureStorageError> {
        let connection = self.get_connection_info();

        return super::sdk::create_page_blob_if_not_exists(
            connection,
            container_name,
            blob_name,
            pages_amount,
            self.telemetry.clone(),
        )
        .await;
    }

    async fn resize_page_blob(
        &self,
        container_name: &str,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();

        return super::sdk::resize_page_blob(
            connection,
            container_name,
            blob_name,
            pages_amount,
            self.telemetry.clone(),
        )
        .await;
    }

    async fn save_pages(
        &self,
        container_name: &str,
        blob_name: &str,
        start_page_no: usize,
        payload: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();

        return super::sdk::save_pages(
            connection,
            container_name,
            blob_name,
            start_page_no,
            payload,
            self.telemetry.clone(),
        )
        .await;
    }

    async fn get_pages(
        &self,
        container_name: &str,
        blob_name: &str,
        start_page_no: usize,
        pages_to_read: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        let connection = self.get_connection_info();

        return super::sdk::get_pages(
            connection,
            container_name,
            blob_name,
            start_page_no,
            pages_to_read,
            self.telemetry.clone(),
        )
        .await;
    }
}
