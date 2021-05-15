use std::rc::Rc;

use crate::my_azure_storage::{azure_response_handler::*, AzureStorageError};

use super::super::AzureConnection;

use super::super::FlUrlAzureExtensions;
use super::blob_models;
use flurl::FlUrl;

pub struct BlockBlob {
    connection: Rc<AzureConnection>,
}

impl BlockBlob {
    pub fn new(connection: Rc<AzureConnection>) -> Self {
        Self {
            connection: connection.clone(),
        }
    }

    pub async fn get_list_of_blobs(
        &self,
        container_name: &str,
    ) -> Result<Vec<String>, AzureStorageError> {
        let mut result = vec![];

        let mut next_marker: Option<String> = None;

        loop {
            let response = FlUrl::new(self.connection.blobs_api_url.as_str())
                .append_path_segment(container_name)
                .append_query_param("comp", "list")
                .append_query_param("restype", "container")
                .add_azure_headers(
                    super::super::SignVerb::GET,
                    self.connection.as_ref(),
                    None,
                    next_marker,
                    AZURE_REST_VERSION,
                )
                .get()
                .await?
                .to_azure_response_handler()
                .check_if_there_is_an_error()?;

            let body = response.get_body().await?;

            let azure_response = blob_models::deserialize_list_of_blobs(body.as_ref());

            result.extend(azure_response.items);

            if azure_response.next_marker.is_none() {
                break;
            }

            next_marker = azure_response.next_marker;
        }

        return Ok(result);
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

    pub async fn upload(
        &self,
        container_name: &str,
        blob_name: &str,
        content: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        FlUrl::new(self.connection.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .with_header("x-ms-blob-type", "BlockBlob")
            .add_azure_headers(
                super::super::SignVerb::PUT,
                self.connection.as_ref(),
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
}

pub const AZURE_REST_VERSION: &str = "2017-07-29";
