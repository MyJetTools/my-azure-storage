pub mod blobs {
    use std::sync::Arc;

    use flurl::FlUrlWithTelemetry;
    use my_telemetry::MyTelemetry;

    use crate::{
        azure_response_handler::{AzureResponseHandler, ToAzureResponseHandler},
        connection::AzureConnectionInfo,
        flurl_ext::FlUrlAzureExtensions,
        sign_utils::SignVerb,
        types::AzureStorageError,
    };

    pub async fn get_blob_properties<TMyTelemetry: MyTelemetry>(
        connection: &AzureConnectionInfo,
        container_name: &str,
        blob_name: &str,
        telemetry: Option<Arc<TMyTelemetry>>,
    ) -> Result<AzureResponseHandler, AzureStorageError> {
        let resp = FlUrlWithTelemetry::new(connection.blobs_api_url.as_str(), telemetry)
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
