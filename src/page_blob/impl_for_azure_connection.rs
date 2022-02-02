use async_trait::async_trait;

use crate::{blob::BlobProperties, connection::AzureStorageConnection, types::AzureStorageError};

use super::PageBlobApi;

#[async_trait]
impl PageBlobApi for AzureStorageConnection {
    async fn create_page_blob(
        &self,
        container_name: &str,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        return super::sdk::create_page_blob(self, container_name, blob_name, pages_amount).await;
    }

    async fn create_page_blob_if_not_exists(
        &self,
        container_name: &str,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<BlobProperties, AzureStorageError> {
        return super::sdk::create_page_blob_if_not_exists(
            self,
            container_name,
            blob_name,
            pages_amount,
        )
        .await;
    }

    async fn resize_page_blob(
        &self,
        container_name: &str,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        return super::sdk::resize_page_blob(self, container_name, blob_name, pages_amount).await;
    }

    async fn save_pages(
        &self,
        container_name: &str,
        blob_name: &str,
        start_page_no: usize,
        payload: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        return super::sdk::save_pages(self, container_name, blob_name, start_page_no, payload)
            .await;
    }

    async fn get_pages(
        &self,
        container_name: &str,
        blob_name: &str,
        start_page_no: usize,
        pages_to_read: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        return super::sdk::get_pages(
            self,
            container_name,
            blob_name,
            start_page_no,
            pages_to_read,
        )
        .await;
    }
}
