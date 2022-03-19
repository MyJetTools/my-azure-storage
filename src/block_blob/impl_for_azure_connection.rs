use async_trait::async_trait;

use crate::{connection::AzureStorageConnection, types::AzureStorageError};

use super::api::BlockBlobApi;

#[async_trait]
impl BlockBlobApi for AzureStorageConnection {
    async fn upload(
        &self,
        container_name: &str,
        blob_name: &str,
        content: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                super::sdk::upload(connection_data, container_name, blob_name, content).await
            }
            AzureStorageConnection::File(connection_data) => {
                let file_name = crate::file_utils::compile_blob_path(
                    connection_data,
                    container_name,
                    blob_name,
                );

                let mut f = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(file_name.as_str())
                    .await?;
                tokio::io::AsyncWriteExt::write_all(&mut f, &content).await?;
                tokio::io::AsyncWriteExt::flush(&mut f).await?;
                Ok(())
            }
        }
    }
}
