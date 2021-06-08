use super::super::consts::AZURE_REST_VERSION;
use crate::{
    my_azure_storage::{azure_response_handler::ToAzureResponseHandler, FlUrlAzureExtensions},
    AzureConnection, AzureStorageError,
};
use async_trait::async_trait;
use flurl::FlUrl;

use super::api::BlockBlobApi;

#[async_trait]
impl BlockBlobApi for AzureConnection {
    async fn upload(
        &self,
        container_name: &str,
        blob_name: &str,
        content: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        FlUrl::new(self.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .with_header("x-ms-blob-type", "BlockBlob")
            .add_azure_headers(
                super::super::SignVerb::PUT,
                &self,
                Some(content.len()),
                None,
                AZURE_REST_VERSION,
            )
            .put(Some(content))
            .await?
            .to_azure_response_handler()
            .check_if_there_is_an_error()?;

        Ok(())
    }
}
