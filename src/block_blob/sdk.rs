use std::sync::Arc;

use crate::{
    azure_response_handler::ToAzureResponseHandler, connection::AzureConnectionInfo,
    consts::DEPENDENCY_TYPE, flurl_ext::FlUrlAzureExtensions, sign_utils::SignVerb,
    types::AzureStorageError,
};

use super::super::consts::AZURE_REST_VERSION;

use flurl::FlUrlWithTelemetry;
use my_telemetry::MyTelemetry;

pub async fn upload<TMyTelemetry: MyTelemetry>(
    connection: &AzureConnectionInfo,
    container_name: &str,
    blob_name: &str,
    content: Vec<u8>,
    telemetry: Option<Arc<TMyTelemetry>>,
) -> Result<(), AzureStorageError> {
    FlUrlWithTelemetry::new(
        connection.blobs_api_url.as_str(),
        telemetry,
        DEPENDENCY_TYPE.to_string(),
    )
    .append_path_segment(container_name)
    .append_path_segment(blob_name)
    .with_header("x-ms-blob-type", "BlockBlob")
    .add_azure_headers(
        SignVerb::PUT,
        connection,
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
