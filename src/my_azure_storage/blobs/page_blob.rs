use std::rc::Rc;

use flurl::FlUrl;

use crate::my_azure_storage::{
    azure_response_handler::*, AzureConnection, AzureStorageError, FlUrlAzureExtensions,
};

use super::models::BlobProperties;

pub struct PageBlob {
    pub connection: Rc<AzureConnection>,
}

impl PageBlob {
    pub fn new(connection: Rc<AzureConnection>) -> Self {
        Self {
            connection: connection.clone(),
        }
    }

    pub async fn create(
        &self,
        container_name: &str,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        let new_size = pages_amount * BLOB_PAGE_SIZE;

        FlUrl::new(self.connection.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .with_header_val_string("x-ms-blob-content-length", new_size.to_string())
            .with_header("x-ms-blob-type", "PageBlob")
            .add_azure_headers(
                super::super::SignVerb::PUT,
                self.connection.as_ref(),
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

    pub async fn delete(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<(), AzureStorageError> {
        FlUrl::new(self.connection.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .add_azure_headers(
                super::super::SignVerb::DELETE,
                self.connection.as_ref(),
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

    pub async fn delete_if_exists(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<(), AzureStorageError> {
        FlUrl::new(self.connection.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .add_azure_headers(
                super::super::SignVerb::DELETE,
                self.connection.as_ref(),
                None,
                None,
                AZURE_REST_VERSION,
            )
            .delete()
            .await?
            .to_azure_response_handler()
            .check_if_there_is_an_error_and_ignore_blob_already_exists()?;

        Ok(())
    }

    pub async fn create_if_not_exists(
        &self,
        container_name: &str,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<BlobProperties, AzureStorageError> {
        loop {
            let result = self.get_properties(container_name, blob_name).await;

            match result {
                Ok(props) => return Ok(props),
                Err(err) => {
                    if matches!(err, AzureStorageError::BlobNotFound) {
                        self.create(container_name, blob_name, pages_amount).await?;
                    } else {
                        return Err(err);
                    }
                }
            };
        }
    }

    pub async fn resize_blob_size(
        &self,
        container_name: &str,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        let new_size = pages_amount * BLOB_PAGE_SIZE;

        FlUrl::new(self.connection.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .append_query_param("comp", "properties")
            .with_header_val_string("x-ms-blob-content-length", new_size.to_string())
            .with_header("x-ms-blob-type", "PageBlob")
            .add_azure_headers(
                super::super::SignVerb::PUT,
                self.connection.as_ref(),
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

    pub async fn save_pages(
        &self,
        container_name: &str,
        blob_name: &str,
        start_page_no: usize,
        payload: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        let start_bytes = start_page_no * BLOB_PAGE_SIZE;

        let end_bytes = start_bytes + payload.len() - 1;

        let range_header = format!("bytes={}-{}", start_bytes, end_bytes);

        FlUrl::new(self.connection.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .append_query_param("comp", "page")
            .with_header("x-ms-page-write", "update")
            .with_header_val_string("x-ms-range", range_header)
            .add_azure_headers(
                super::super::SignVerb::PUT,
                self.connection.as_ref(),
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

    pub async fn get(
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

        let response = FlUrl::new(self.connection.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .with_header_val_string("x-ms-range", range_header)
            .add_azure_headers(
                super::super::SignVerb::GET,
                self.connection.as_ref(),
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

    pub async fn download(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<Vec<u8>, AzureStorageError> {
        let response = FlUrl::new(self.connection.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .add_azure_headers(
                super::super::SignVerb::GET,
                self.connection.as_ref(),
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

    pub async fn get_properties(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<BlobProperties, AzureStorageError> {
        let response = FlUrl::new(self.connection.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .add_azure_headers(
                super::super::SignVerb::HEAD,
                self.connection.as_ref(),
                None,
                None,
                AZURE_REST_VERSION,
            )
            .head()
            .await?
            .to_azure_response_handler()
            .check_if_there_is_an_error()?;

        let content_len = response.get_header("content-length").unwrap();

        let blob_size = content_len.parse().unwrap();

        let result = BlobProperties { blob_size };

        Ok(result)
    }
}

const AZURE_REST_VERSION: &str = "2017-07-29";

pub const BLOB_PAGE_SIZE: usize = 512;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_page_blob() {
        let conn_string = env!("TEST_STORAGE_ACCOUNT");

        let connection = AzureConnection::from_conn_string(conn_string);

        connection
            .create_container_if_not_exist("testtest")
            .await
            .unwrap();

        let connection = Rc::new(connection);

        let page_blob = PageBlob::new(connection);

        page_blob.create("testtest", "test", 1).await.unwrap();

        page_blob
            .resize_blob_size("testtest", "test", 4)
            .await
            .unwrap();

        let vec: Vec<u8> = vec![33; 512];

        page_blob
            .save_pages("testtest", "test", 1, vec)
            .await
            .unwrap();

        let blob_props = page_blob.get_properties("testtest", "test").await.unwrap();

        assert_eq!(512 * 4, blob_props.blob_size)

        //  let res = page_blob.get("testtest", "test", 2, 2).await.unwrap();

        //println!("{}", hex::encode(res));
    }

    #[tokio::test]
    async fn test_container_not_found() {
        let conn_string = env!("TEST_STORAGE_ACCOUNT");

        let connection = AzureConnection::from_conn_string(conn_string);
        println!("Name:{}", connection.account_name);

        let connection = Rc::new(connection);

        let page_blob = PageBlob::new(connection);

        let result = page_blob.get_properties("notexists", "notexists").await;

        if let Err(err) = result {
            assert_eq!(true, matches!(err, AzureStorageError::ContainerNotFound));
        } else {
            panic!(format!("Unexpected result type: {:?}", result));
        }
    }

    #[tokio::test]
    async fn test_blob_not_found() {
        let conn_string = env!("TEST_STORAGE_ACCOUNT");

        let connection = AzureConnection::from_conn_string(conn_string);
        println!("Name:{}", connection.account_name);

        let connection = Rc::new(connection);

        let page_blob = PageBlob::new(connection);

        let result = page_blob.get_properties("testtest", "notexists").await;

        if let Err(err) = result {
            assert_eq!(true, matches!(err, AzureStorageError::BlobNotFound));
        } else {
            panic!(format!("Unexpected result type: {:?}", result));
        }
    }
}
