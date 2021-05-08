use std::rc::Rc;

use flurl::FlUrl;
use hyper::Error;

use crate::my_azure_storage::{AzureConnection, FlUrlAzureExtensions};

pub struct PageBlob {
    connection: Rc<AzureConnection>,
}

impl PageBlob {
    pub fn new(connection: Rc<AzureConnection>) -> Self {
        Self {
            connection: connection.clone(),
        }
    }

    pub async fn resize_blob_size(
        &self,
        container_name: &str,
        blob_name: &str,
        new_size: u64,
    ) -> Result<(), Error> {
        FlUrl::new(self.connection.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .append_query_param("comp", "properties")
            .with_header_val_string("x-ms-blob-content-length", new_size.to_string())
            .add_azure_headers(
                super::super::SignVerb::PUT,
                self.connection.as_ref(),
                None,
                AZURE_REST_VERSION,
            )
            .put(Vec::new())
            .await?;

        return Ok(());
    }

    pub async fn save_pages(
        &self,
        container_name: &str,
        blob_name: &str,
        start_page_no: usize,
        payload: Vec<u8>,
    ) -> Result<(), Error> {
        let start_bytes = start_page_no * PAGE_SIZE;

        let end_bytes = start_bytes + payload.len() - 1;

        FlUrl::new(self.connection.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .with_header_val_string("Content-Length", payload.len().to_string())
            .with_header("x-ms-page-write", "update")
            .with_header_val_string("x-ms-range", format!("{}-{}", start_bytes, end_bytes))
            .add_azure_headers(
                super::super::SignVerb::PUT,
                self.connection.as_ref(),
                None,
                AZURE_REST_VERSION,
            )
            .put(payload)
            .await?;

        Ok(())
    }

    pub async fn get(
        &self,
        container_name: &str,
        blob_name: &str,
        start_page_no: usize,
        size_to_read: usize,
    ) -> Result<Vec<u8>, Error> {
        let start_bytes = start_page_no * PAGE_SIZE;

        let end_bytes = start_bytes + size_to_read - 1;

        let response = FlUrl::new(self.connection.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .with_header("Content-Length", "0")
            .with_header_val_string("x-ms-range", format!("{}-{}", start_bytes, end_bytes))
            .add_azure_headers(
                super::super::SignVerb::GET,
                self.connection.as_ref(),
                None,
                AZURE_REST_VERSION,
            )
            .get()
            .await?;

        Ok(response.get_body().await?)
    }
}

const AZURE_REST_VERSION: &str = "2011-08-18";

const PAGE_SIZE: usize = 512;
