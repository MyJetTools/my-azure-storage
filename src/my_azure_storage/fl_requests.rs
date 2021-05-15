pub mod blobs {
    use flurl::FlUrl;

    use crate::{
        my_azure_storage::{
            azure_response_handler::{AzureResponseHandler, ToAzureResponseHandler},
            FlUrlAzureExtensions,
        },
        AzureConnection, AzureStorageError,
    };

    pub async fn get_blob_properties(
        connection: &AzureConnection,
        container_name: &str,
        blob_name: &str,
    ) -> Result<AzureResponseHandler, AzureStorageError> {
        let resp = FlUrl::new(connection.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .add_azure_headers(
                super::super::SignVerb::HEAD,
                connection,
                None,
                None,
                super::AZURE_REST_VERSION,
            )
            .head()
            .await?
            .to_azure_response_handler();

        Ok(resp)
    }
}

const AZURE_REST_VERSION: &str = "2017-07-29";
