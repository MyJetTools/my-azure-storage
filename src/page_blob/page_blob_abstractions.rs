use crate::AzureStorageError;

use super::PageBlobProperties;

#[async_trait::async_trait]
pub trait PageBlobAbstractions {
    async fn create_container_if_not_exists(&self) -> Result<(), AzureStorageError>;
    async fn create_blob_if_not_exists(
        &self,
        init_pages_amounts: usize,
        auto_create_container: bool,
    ) -> Result<PageBlobProperties, AzureStorageError>;
}
