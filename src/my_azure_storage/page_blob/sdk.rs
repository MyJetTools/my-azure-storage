use async_trait::async_trait;
use flurl::FlUrl;

use crate::my_azure_storage::blob::api::BlobApi;
use crate::my_azure_storage::consts::AZURE_REST_VERSION;
use crate::my_azure_storage::{
    azure_response_handler::*, AzureConnection, AzureStorageError, BlobProperties,
    FlUrlAzureExtensions,
};

use super::api::PageBlobApi;
use super::consts::BLOB_PAGE_SIZE;

#[async_trait]
impl PageBlobApi for AzureConnection {
    async fn create_page_blob(
        &self,
        container_name: &str,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        let new_size = pages_amount * BLOB_PAGE_SIZE;

        FlUrl::new(self.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .with_header_val_string("x-ms-blob-content-length", new_size.to_string())
            .with_header("x-ms-blob-type", "PageBlob")
            .add_azure_headers(
                super::super::SignVerb::PUT,
                &self,
                None,
                None,
                AZURE_REST_VERSION,
            )
            .put(None)
            .await?
            .to_azure_response_handler()
            .check_if_there_is_an_error()?;

        return Ok(());
    }

    async fn create_page_blob_if_not_exists(
        &self,
        container_name: &str,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<BlobProperties, AzureStorageError> {
        loop {
            let result = self.get_blob_properties(container_name, blob_name).await;

            match result {
                Ok(props) => return Ok(props),
                Err(err) => {
                    if matches!(err, AzureStorageError::BlobNotFound) {
                        self.create_page_blob(container_name, blob_name, pages_amount)
                            .await?;
                    } else {
                        return Err(err);
                    }
                }
            };
        }
    }

    async fn resize_page_blob(
        &self,
        container_name: &str,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        let new_size = pages_amount * BLOB_PAGE_SIZE;

        FlUrl::new(self.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .append_query_param("comp", "properties")
            .with_header_val_string("x-ms-blob-content-length", new_size.to_string())
            .with_header("x-ms-blob-type", "PageBlob")
            .add_azure_headers(
                super::super::SignVerb::PUT,
                &self,
                None,
                None,
                AZURE_REST_VERSION,
            )
            .put(None)
            .await?
            .to_azure_response_handler()
            .check_if_there_is_an_error()?;

        Ok(())
    }

    async fn save_pages(
        &self,
        container_name: &str,
        blob_name: &str,
        start_page_no: usize,
        payload: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        let start_bytes = start_page_no * BLOB_PAGE_SIZE;

        let end_bytes = start_bytes + payload.len() - 1;

        let range_header = format!("bytes={}-{}", start_bytes, end_bytes);

        FlUrl::new(self.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .append_query_param("comp", "page")
            .with_header("x-ms-page-write", "update")
            .with_header_val_string("x-ms-range", range_header)
            .add_azure_headers(
                super::super::SignVerb::PUT,
                &self,
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

    async fn get(
        &self,
        container_name: &str,
        blob_name: &str,
        start_page_no: usize,
        pages_to_read: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        let start_bytes = start_page_no * BLOB_PAGE_SIZE;

        let size_to_read = pages_to_read * BLOB_PAGE_SIZE;

        let end_bytes = start_bytes + size_to_read - 1;

        let range_header = format!("bytes={}-{}", start_bytes, end_bytes);

        let response = FlUrl::new(self.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .with_header_val_string("x-ms-range", range_header)
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

        Ok(response.get_body().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::super::blob_container::api::BlobContainersApi;

    #[tokio::test]
    async fn test_page_blob() {
        let conn_string = env!("TEST_STORAGE_ACCOUNT");

        let connection = AzureConnection::from_conn_string(conn_string);

        connection
            .create_container_if_not_exist("testtest")
            .await
            .unwrap();

        connection
            .create_page_blob("testtest", "test", 1)
            .await
            .unwrap();

        connection
            .resize_page_blob("testtest", "test", 4)
            .await
            .unwrap();

        let vec: Vec<u8> = vec![33; 512];

        connection
            .save_pages("testtest", "test", 1, vec)
            .await
            .unwrap();

        let blob_props = connection
            .get_blob_properties("testtest", "test")
            .await
            .unwrap();

        assert_eq!(512 * 4, blob_props.blob_size)

        //  let res = page_blob.get("testtest", "test", 2, 2).await.unwrap();

        //println!("{}", hex::encode(res));
    }
}
