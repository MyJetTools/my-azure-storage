use flurl::FlUrl;
use rust_extensions::AsSliceOrVec;

use super::azure_response_handler::ToAzureResponseHandler;
use super::consts::AZURE_REST_VERSION;
use super::flurl_ext::FlUrlAzureExtensions;
use crate::blob::BlobProperties;
use crate::connection::AzureStorageConnectionData;
use crate::AzureStorageError;

use super::sign_utils::SignVerb;

use crate::page_blob::consts::BLOB_PAGE_SIZE;

pub async fn create_page_blob_if_not_exists(
    connection: &AzureStorageConnectionData,
    container_name: &str,
    blob_name: &str,
    pages_amount: usize,
) -> Result<BlobProperties, AzureStorageError> {
    loop {
        let result =
            crate::sdk_azure::blobs::get_blob_properties(connection, container_name, blob_name)
                .await;

        match result {
            Ok(props) => return Ok(props),
            Err(err) => {
                if matches!(err, AzureStorageError::BlobNotFound) {
                    create_page_blob(connection, container_name, blob_name, pages_amount).await?;
                } else {
                    return Err(err);
                }
            }
        };
    }
}

pub async fn resize_page_blob(
    connection: &AzureStorageConnectionData,
    container_name: &str,
    blob_name: &str,
    pages_amount: usize,
) -> Result<(), AzureStorageError> {
    let new_size = pages_amount * BLOB_PAGE_SIZE;

    let fl_url: FlUrl = connection.into();

    fl_url
        .append_path_segment(container_name)
        .append_path_segment(blob_name)
        .append_query_param("comp", Some("properties"))
        .with_header("x-ms-blob-content-length", new_size.to_string())
        .with_header("x-ms-blob-type", "PageBlob")
        .add_azure_headers(SignVerb::PUT, &connection, None, None, AZURE_REST_VERSION)
        .put(None)
        .await?
        .to_azure_response_handler()
        .check_if_there_is_an_error()?;

    Ok(())
}

pub async fn save_pages<'s>(
    connection: &AzureStorageConnectionData,
    container_name: &str,
    blob_name: &str,
    start_page_no: usize,
    payload: impl Into<AsSliceOrVec<'s, u8>>,
) -> Result<(), AzureStorageError> {
    let start_bytes = start_page_no * BLOB_PAGE_SIZE;

    let payload = payload.into().into_vec();

    let end_bytes = start_bytes + payload.len() - 1;

    let range_header = format!("bytes={}-{}", start_bytes, end_bytes);

    let fl_url: FlUrl = connection.into();

    fl_url
        .append_path_segment(container_name)
        .append_path_segment(blob_name)
        .append_query_param("comp", Some("page"))
        .with_header("x-ms-page-write", "update")
        .with_header("x-ms-range", range_header)
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

pub async fn get_pages(
    connection: &AzureStorageConnectionData,
    container_name: &str,
    blob_name: &str,
    start_page_no: usize,
    pages_to_read: usize,
) -> Result<Vec<u8>, AzureStorageError> {
    let start_bytes = start_page_no * BLOB_PAGE_SIZE;

    let size_to_read = pages_to_read * BLOB_PAGE_SIZE;

    let end_bytes = start_bytes + size_to_read - 1;

    let range_header = format!("bytes={}-{}", start_bytes, end_bytes);

    let fl_url: FlUrl = connection.into();

    let response = fl_url
        .append_path_segment(container_name)
        .append_path_segment(blob_name)
        .with_header("x-ms-range", range_header)
        .add_azure_headers(SignVerb::GET, &connection, None, None, AZURE_REST_VERSION)
        .get()
        .await?
        .to_azure_response_handler()
        .check_if_there_is_an_error()?;

    Ok(response.get_body().await?)
}

pub async fn create_page_blob(
    connection: &AzureStorageConnectionData,
    container_name: &str,
    blob_name: &str,
    pages_amount: usize,
) -> Result<(), AzureStorageError> {
    let new_size = pages_amount * BLOB_PAGE_SIZE;

    let fl_url: FlUrl = connection.into();

    fl_url
        .append_path_segment(container_name)
        .append_path_segment(blob_name)
        .with_header("x-ms-blob-content-length", new_size.to_string())
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

    use super::*;

    #[tokio::test]
    async fn test_page_blob() {
        let conn_string = std::env::var("TEST_STORAGE_ACCOUNT").unwrap();

        let connection = AzureStorageConnectionData::from_conn_string(conn_string.as_str());

        crate::sdk_azure::containers::create_if_not_exists(&connection, "testtest")
            .await
            .unwrap();

        super::create_page_blob(&connection, "testtest", "test", 1)
            .await
            .unwrap();

        super::resize_page_blob(&connection, "testtest", "test", 4)
            .await
            .unwrap();

        let my_vec: Vec<u8> = vec![33; 512];

        super::save_pages(&connection, "testtest", "test", 1, my_vec)
            .await
            .unwrap();

        let blob_props =
            crate::sdk_azure::blobs::get_blob_properties(&connection, "testtest", "test")
                .await
                .unwrap();

        assert_eq!(512 * 4, blob_props.blob_size)

        //  let res = page_blob.get("testtest", "test", 2, 2).await.unwrap();

        //println!("{}", hex::encode(res));
    }
}
