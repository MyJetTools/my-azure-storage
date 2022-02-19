use async_trait::async_trait;

use crate::{blob::BlobProperties, AzureStorageError};

#[async_trait]
pub trait AzurePageBlobStorage {
    fn get_container_name(&self) -> &str;
    fn get_blob_name(&self) -> &str;

    async fn get_blob_properties(&self) -> Result<BlobProperties, AzureStorageError>;

    async fn create(&self, pages_amount: usize) -> Result<(), AzureStorageError>;
    async fn create_if_not_exists(&self, pages_amount: usize) -> Result<usize, AzureStorageError>;
    async fn get_available_pages_amount(&self) -> Result<usize, AzureStorageError>;
    async fn create_container_if_not_exist(&self) -> Result<(), AzureStorageError>;

    async fn resize(&self, pages_amount: usize) -> Result<(), AzureStorageError>;

    async fn delete(&self) -> Result<(), AzureStorageError>;

    async fn delete_if_exists(&self) -> Result<(), AzureStorageError>;

    async fn get(
        &self,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError>;

    async fn save_pages(
        &self,
        start_page_no: usize,
        payload: Vec<u8>,
    ) -> Result<(), AzureStorageError>;

    async fn download(&self) -> Result<Vec<u8>, AzureStorageError>;
}
