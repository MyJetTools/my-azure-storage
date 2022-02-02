pub mod blobs {

    use flurl::FlUrl;

    use crate::{
        azure_response_handler::{AzureResponseHandler, ToAzureResponseHandler},
        flurl_ext::FlUrlAzureExtensions,
        sign_utils::SignVerb,
        types::AzureStorageError,
        AzureStorageConnection,
    };

    pub async fn get_blob_properties(
        connection: &AzureStorageConnection,
        container_name: &str,
        blob_name: &str,
    ) -> Result<AzureResponseHandler, AzureStorageError> {
        let fl_url: FlUrl = connection.into();

        let resp = fl_url
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .add_azure_headers(
                SignVerb::HEAD,
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
