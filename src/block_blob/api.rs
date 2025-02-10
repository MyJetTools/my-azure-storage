use async_trait::async_trait;
use rust_extensions::SliceOrVec;

use crate::types::AzureStorageError;

#[async_trait]
pub trait BlockBlobApi {
    async fn upload_block_blob<'s>(
        &self,
        container_name: &str,
        blob_name: &str,
        content: impl Into<SliceOrVec<'s, u8>> + Send + Sync + 'static,
    ) -> Result<(), AzureStorageError>;
}
