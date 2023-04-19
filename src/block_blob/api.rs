use async_trait::async_trait;
use rust_extensions::AsSliceOrVec;

use crate::types::AzureStorageError;

#[async_trait]
pub trait BlockBlobApi {
    async fn upload_block_blob<'s>(
        &self,
        container_name: &str,
        blob_name: &str,
        content: impl Into<AsSliceOrVec<'s, u8>> + Send + Sync + 'static,
    ) -> Result<(), AzureStorageError>;
}
