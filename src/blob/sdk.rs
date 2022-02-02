use crate::azure_response_handler::ToAzureResponseHandler;
use crate::types::AzureStorageError;
use crate::AzureStorageConnection;
use crate::{flurl_ext::FlUrlAzureExtensions, sign_utils::SignVerb};

use flurl::FlUrl;

use super::super::consts::AZURE_REST_VERSION;
use super::BlobProperties;

pub async fn delete_blob_if_exists(
    connection: &AzureStorageConnection,
    container_name: &str,
    blob_name: &str,
) -> Result<(), AzureStorageError> {
    let fl_url: FlUrl = connection.into();

    fl_url
        .append_path_segment(container_name)
        .append_path_segment(blob_name)
        .add_azure_headers(SignVerb::DELETE, connection, None, None, AZURE_REST_VERSION)
        .delete()
        .await?
        .to_azure_response_handler()
        .check_if_there_is_an_error_and_ignore_one(AzureStorageError::BlobNotFound)?;

    Ok(())
}

pub async fn get_blob_properties(
    connection: &AzureStorageConnection,
    container_name: &str,
    blob_name: &str,
) -> Result<BlobProperties, AzureStorageError> {
    let response = super::super::fl_requests::blobs::get_blob_properties(
        connection,
        container_name,
        blob_name,
    )
    .await?
    .check_if_there_is_an_error()?;

    let content_len = response.get_header("content-length").unwrap();

    let blob_size = content_len.parse().unwrap();

    let result = BlobProperties { blob_size };

    Ok(result)
}

pub async fn download_blob(
    connection: &AzureStorageConnection,
    container_name: &str,
    blob_name: &str,
) -> Result<Vec<u8>, AzureStorageError> {
    let fl_url: FlUrl = connection.into();

    let response = fl_url
        .append_path_segment(container_name)
        .append_path_segment(blob_name)
        .add_azure_headers(SignVerb::GET, connection, None, None, AZURE_REST_VERSION)
        .get()
        .await?
        .to_azure_response_handler()
        .check_if_there_is_an_error()?;

    let result = response.get_body().await?;

    Ok(result)
}

pub async fn delete_blob(
    connection: &AzureStorageConnection,
    container_name: &str,
    blob_name: &str,
) -> Result<(), AzureStorageError> {
    let fl_url: FlUrl = connection.into();
    fl_url
        .append_path_segment(container_name)
        .append_path_segment(blob_name)
        .add_azure_headers(SignVerb::DELETE, connection, None, None, AZURE_REST_VERSION)
        .delete()
        .await?
        .to_azure_response_handler()
        .check_if_there_is_an_error()?;

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_blob_not_found() {
        let conn_string = env!("TEST_STORAGE_ACCOUNT");

        let connection = AzureStorageConnection::from_conn_string(conn_string);
        println!("Name:{}", connection.account_name);

        let result = get_blob_properties(&connection, "testtest", "notexists").await;

        if let Err(err) = result {
            assert_eq!(true, matches!(err, AzureStorageError::BlobNotFound));
        } else {
            panic!("Unexpected result type: {:?}", result);
        }
    }
}
