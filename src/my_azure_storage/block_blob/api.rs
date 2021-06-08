use async_trait::async_trait;

use crate::AzureStorageError;

#[async_trait]
pub trait BlockBlobApi {
    async fn upload(
        &self,
        container_name: &str,
        blob_name: &str,
        content: Vec<u8>,
    ) -> Result<(), AzureStorageError>;
}
