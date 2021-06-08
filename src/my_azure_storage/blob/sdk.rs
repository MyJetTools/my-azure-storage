use crate::{
    my_azure_storage::{azure_response_handler::ToAzureResponseHandler, FlUrlAzureExtensions},
    AzureConnection, AzureStorageError,
};
use async_trait::async_trait;
use flurl::FlUrl;

use super::super::consts::AZURE_REST_VERSION;
use super::{api::BlobApi, BlobProperties};

#[async_trait]
impl BlobApi for AzureConnection {
    async fn get_blob_properties(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<BlobProperties, AzureStorageError> {
        let response =
            super::super::fl_requests::blobs::get_blob_properties(&self, container_name, blob_name)
                .await?
                .check_if_there_is_an_error()?;

        let content_len = response.get_header("content-length").unwrap();

        let blob_size = content_len.parse().unwrap();

        let result = BlobProperties { blob_size };

        Ok(result)
    }

    async fn download_blob(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<Vec<u8>, AzureStorageError> {
        let response = FlUrl::new(self.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .add_azure_headers(
                super::super::SignVerb::GET,
                &self,
                None,
                None,
                AZURE_REST_VERSION,
            )
            .get()
            .await?
            .to_azure_response_handler()
            .check_if_there_is_an_error()?;

        let result = response.get_body().await?;

        Ok(result)
    }

    async fn delete_blob(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<(), AzureStorageError> {
        FlUrl::new(self.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .add_azure_headers(
                super::super::SignVerb::DELETE,
                &self,
                None,
                None,
                AZURE_REST_VERSION,
            )
            .delete()
            .await?
            .to_azure_response_handler()
            .check_if_there_is_an_error()?;

        Ok(())
    }

    async fn delete_blob_if_exists(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<(), AzureStorageError> {
        FlUrl::new(self.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .add_azure_headers(
                super::super::SignVerb::DELETE,
                &self,
                None,
                None,
                AZURE_REST_VERSION,
            )
            .delete()
            .await?
            .to_azure_response_handler()
            .check_if_there_is_an_error_and_ignore_one(AzureStorageError::BlobNotFound)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_blob_not_found() {
        let conn_string = env!("TEST_STORAGE_ACCOUNT");

        let connection = AzureConnection::from_conn_string(conn_string);
        println!("Name:{}", connection.account_name);

        let result = connection
            .get_blob_properties("testtest", "notexists")
            .await;

        if let Err(err) = result {
            assert_eq!(true, matches!(err, AzureStorageError::BlobNotFound));
        } else {
            panic!("Unexpected result type: {:?}", result);
        }
    }
}
