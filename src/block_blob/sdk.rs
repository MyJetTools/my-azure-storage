use crate::{
    azure_response_handler::ToAzureResponseHandler, connection::AzureStorageConnectionData,
    flurl_ext::FlUrlAzureExtensions, sign_utils::SignVerb, types::AzureStorageError,
};

use super::super::consts::AZURE_REST_VERSION;

use flurl::FlUrl;

pub async fn upload(
    connection: &AzureStorageConnectionData,
    container_name: &str,
    blob_name: &str,
    content: Vec<u8>,
) -> Result<(), AzureStorageError> {
    let fl_url: FlUrl = connection.into();

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
