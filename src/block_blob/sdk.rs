use crate::sdk_azure::azure_response_handler::ToAzureResponseHandler;
use crate::sdk_azure::flurl_ext::FlUrlAzureExtensions;
use crate::sdk_azure::sign_utils::SignVerb;
use crate::{connection::AzureStorageConnectionData, types::AzureStorageError};

use crate::sdk_azure::consts::AZURE_REST_VERSION;

use flurl::FlUrl;
use rust_extensions::AsSliceOrVec;

pub async fn upload_block_blob<'s>(
    connection: &AzureStorageConnectionData,
    container_name: &str,
    blob_name: &str,
    content: impl Into<AsSliceOrVec<'s, u8>>,
) -> Result<(), AzureStorageError> {
    let fl_url: FlUrl = connection.into();

    let content = content.into().into_vec();

    fl_url
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
