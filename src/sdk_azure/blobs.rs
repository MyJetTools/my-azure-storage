use crate::azure_response_handler::ToAzureResponseHandler;
use crate::blob::BlobProperties;
use crate::connection::AzureStorageConnectionData;
use crate::types::AzureStorageError;
use crate::{flurl_ext::FlUrlAzureExtensions, sign_utils::SignVerb};

use flurl::FlUrl;

use super::super::consts::AZURE_REST_VERSION;
use super::models::{deserialize_list_of_blobs, NextMarkerToRead};

pub struct AzureBlobsListReader<'s> {
    connection: &'s AzureStorageConnectionData,
    next_marker: NextMarkerToRead,
    container_name: &'s str,
}

impl<'s> AzureBlobsListReader<'s> {
    pub fn new(connection: &'s AzureStorageConnectionData, container_name: &'s str) -> Self {
        Self {
            connection,
            next_marker: NextMarkerToRead::Start,
            container_name,
        }
    }

    fn get_next_marker(&mut self) -> Option<String> {
        let mut result = NextMarkerToRead::End;

        std::mem::swap(&mut result, &mut self.next_marker);

        if let NextMarkerToRead::Next(marker) = result {
            Some(marker)
        } else {
            None
        }
    }

    pub async fn get_next(&mut self) -> Result<Option<Vec<String>>, AzureStorageError> {
        if let NextMarkerToRead::End = &self.next_marker {
            return Ok(None);
        }

        let fl_url: FlUrl = self.connection.into();

        let next_marker = self.get_next_marker();

        let response = fl_url
            .append_path_segment(self.container_name)
            .append_query_param("comp", "list")
            .append_query_param("restype", "container")
            .add_azure_headers(
                SignVerb::GET,
                self.connection,
                None,
                next_marker,
                AZURE_REST_VERSION,
            )
            .get()
            .await?
            .to_azure_response_handler()
            .check_if_there_is_an_error()?;

        let body = response.get_body().await?;

        let azure_response = deserialize_list_of_blobs(body.as_ref());

        if let Some(marker) = azure_response.next_marker {
            self.next_marker = NextMarkerToRead::Next(marker);
        }

        Ok(Some(azure_response.items))
    }
}

pub async fn get_list(
    connection: &AzureStorageConnectionData,
    container_name: &str,
) -> Result<Vec<String>, AzureStorageError> {
    let mut result = vec![];

    let mut reader = AzureBlobsListReader::new(connection, container_name);

    while let Some(chunk) = reader.get_next().await? {
        result.extend(chunk);
    }

    Ok(result)
}

pub async fn get_blob_properties(
    connection: &AzureStorageConnectionData,
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

pub async fn delete_if_exists(
    connection: &AzureStorageConnectionData,
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

pub async fn delete(
    connection: &AzureStorageConnectionData,
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

pub async fn download(
    connection: &AzureStorageConnectionData,
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

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_blob_not_found() {
        let conn_string = env!("TEST_STORAGE_ACCOUNT");

        let connection = AzureStorageConnectionData::from_conn_string(conn_string);
        println!("Name:{}", connection.account_name);

        let result = get_blob_properties(&connection, "testtest", "notexists").await;

        if let Err(err) = result {
            assert_eq!(true, matches!(err, AzureStorageError::BlobNotFound));
        } else {
            panic!("Unexpected result type: {:?}", result);
        }
    }
}
