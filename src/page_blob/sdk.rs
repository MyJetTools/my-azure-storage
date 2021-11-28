use std::sync::Arc;

use flurl::FlUrlWithTelemetry;
use my_telemetry::MyTelemetry;

use crate::azure_response_handler::ToAzureResponseHandler;
use crate::blob::sdk::get_blob_properties;
use crate::blob::BlobProperties;
use crate::connection::AzureStorageConnectionInfo;
use crate::consts::{AZURE_REST_VERSION, DEPENDENCY_TYPE};
use crate::flurl_ext::FlUrlAzureExtensions;

use crate::sign_utils::SignVerb;
use crate::types::AzureStorageError;

use super::consts::BLOB_PAGE_SIZE;

pub async fn create_page_blob_if_not_exists<TMyTelemetry: MyTelemetry>(
    connection: &AzureStorageConnectionInfo,
    container_name: &str,
    blob_name: &str,
    pages_amount: usize,
    telemetry: Option<Arc<TMyTelemetry>>,
) -> Result<BlobProperties, AzureStorageError> {
    loop {
        let result =
            get_blob_properties(connection, container_name, blob_name, telemetry.clone()).await;

        match result {
            Ok(props) => return Ok(props),
            Err(err) => {
                if matches!(err, AzureStorageError::BlobNotFound) {
                    create_page_blob(
                        connection,
                        container_name,
                        blob_name,
                        pages_amount,
                        telemetry.clone(),
                    )
                    .await?;
                } else {
                    return Err(err);
                }
            }
        };
    }
}

pub async fn resize_page_blob<TMyTelemetry: MyTelemetry>(
    connection: &AzureStorageConnectionInfo,
    container_name: &str,
    blob_name: &str,
    pages_amount: usize,
    telemetry: Option<Arc<TMyTelemetry>>,
) -> Result<(), AzureStorageError> {
    let new_size = pages_amount * BLOB_PAGE_SIZE;

    FlUrlWithTelemetry::new(
        connection.blobs_api_url.as_str(),
        telemetry,
        DEPENDENCY_TYPE.to_string(),
    )
    .append_path_segment(container_name)
    .append_path_segment(blob_name)
    .append_query_param("comp", "properties")
    .with_header_val_string("x-ms-blob-content-length", new_size.to_string())
    .with_header("x-ms-blob-type", "PageBlob")
    .add_azure_headers(SignVerb::PUT, &connection, None, None, AZURE_REST_VERSION)
    .put(None)
    .await?
    .to_azure_response_handler()
    .check_if_there_is_an_error()?;

    Ok(())
}

pub async fn save_pages<TMyTelemetry: MyTelemetry>(
    connection: &AzureStorageConnectionInfo,
    container_name: &str,
    blob_name: &str,
    start_page_no: usize,
    payload: Vec<u8>,
    telemetry: Option<Arc<TMyTelemetry>>,
) -> Result<(), AzureStorageError> {
    let start_bytes = start_page_no * BLOB_PAGE_SIZE;

    let end_bytes = start_bytes + payload.len() - 1;

    let range_header = format!("bytes={}-{}", start_bytes, end_bytes);

    FlUrlWithTelemetry::new(
        connection.blobs_api_url.as_str(),
        telemetry,
        DEPENDENCY_TYPE.to_string(),
    )
    .append_path_segment(container_name)
    .append_path_segment(blob_name)
    .append_query_param("comp", "page")
    .with_header("x-ms-page-write", "update")
    .with_header_val_string("x-ms-range", range_header)
    .add_azure_headers(
        SignVerb::PUT,
        &connection,
        Some(payload.len()),
        None,
        AZURE_REST_VERSION,
    )
    .put(Some(payload))
    .await?
    .to_azure_response_handler()
    .check_if_there_is_an_error()?;

    Ok(())
}

pub async fn get_pages<TMyTelemetry: MyTelemetry>(
    connection: &AzureStorageConnectionInfo,
    container_name: &str,
    blob_name: &str,
    start_page_no: usize,
    pages_to_read: usize,
    telemetry: Option<Arc<TMyTelemetry>>,
) -> Result<Vec<u8>, AzureStorageError> {
    let start_bytes = start_page_no * BLOB_PAGE_SIZE;

    let size_to_read = pages_to_read * BLOB_PAGE_SIZE;

    let end_bytes = start_bytes + size_to_read - 1;

    let range_header = format!("bytes={}-{}", start_bytes, end_bytes);

    let response = FlUrlWithTelemetry::new(
        connection.blobs_api_url.as_str(),
        telemetry,
        DEPENDENCY_TYPE.to_string(),
    )
    .append_path_segment(container_name)
    .append_path_segment(blob_name)
    .with_header_val_string("x-ms-range", range_header)
    .add_azure_headers(SignVerb::GET, &connection, None, None, AZURE_REST_VERSION)
    .get()
    .await?
    .to_azure_response_handler()
    .check_if_there_is_an_error()?;

    Ok(response.get_body().await?)
}

pub async fn create_page_blob<TMyTelemetry: MyTelemetry>(
    connection: &AzureStorageConnectionInfo,
    container_name: &str,
    blob_name: &str,
    pages_amount: usize,
    telemetry: Option<Arc<TMyTelemetry>>,
) -> Result<(), AzureStorageError> {
    let new_size = pages_amount * BLOB_PAGE_SIZE;

    FlUrlWithTelemetry::new(
        connection.blobs_api_url.as_str(),
        telemetry,
        DEPENDENCY_TYPE.to_string(),
    )
    .append_path_segment(container_name)
    .append_path_segment(blob_name)
    .with_header_val_string("x-ms-blob-content-length", new_size.to_string())
    .with_header("x-ms-blob-type", "PageBlob")
    .add_azure_headers(SignVerb::PUT, &connection, None, None, AZURE_REST_VERSION)
    .put(None)
    .await?
    .to_azure_response_handler()
    .check_if_there_is_an_error()?;

    return Ok(());
}

#[cfg(test)]
mod tests {

    use my_telemetry::MyTelemetryToConsole;

    use crate::blob::sdk::get_blob_properties;
    use crate::blob_container::sdk::create_container_if_not_exist;

    use super::*;

    #[tokio::test]
    async fn test_page_blob() {
        let conn_string = env!("TEST_STORAGE_ACCOUNT");

        let connection = AzureStorageConnectionInfo::from_conn_string(conn_string);

        create_container_if_not_exist::<MyTelemetryToConsole>(&connection, "testtest", None)
            .await
            .unwrap();

        super::create_page_blob::<MyTelemetryToConsole>(&connection, "testtest", "test", 1, None)
            .await
            .unwrap();

        super::resize_page_blob::<MyTelemetryToConsole>(&connection, "testtest", "test", 4, None)
            .await
            .unwrap();

        let my_vec: Vec<u8> = vec![33; 512];

        super::save_pages::<MyTelemetryToConsole>(&connection, "testtest", "test", 1, my_vec, None)
            .await
            .unwrap();

        let blob_props =
            get_blob_properties::<MyTelemetryToConsole>(&connection, "testtest", "test", None)
                .await
                .unwrap();

        assert_eq!(512 * 4, blob_props.blob_size)

        //  let res = page_blob.get("testtest", "test", 2, 2).await.unwrap();

        //println!("{}", hex::encode(res));
    }
}
