use hyper::Error;

use super::super::FlUrlAzureExtensions;
use super::blob_models;
use flurl::FlUrl;

use super::super::AzureConnection;

impl AzureConnection {
    pub async fn get_list_of_blob_containers(&self) -> Result<Vec<String>, Error> {
        let mut result = vec![];

        let mut next_marker: Option<String> = None;

        loop {
            let response = FlUrl::new(self.blobs_api_url.as_str())
                .append_query_param("comp", "list")
                .with_header("Content-Length", "0")
                .add_azure_headers(super::super::sign_utils::SignVerb::GET, self, next_marker)
                .get()
                .await?;

            let body = response.get_body().await?;

            let azure_response = blob_models::deserialize_list_of_containers(body.as_ref());

            result.extend(azure_response.items);

            if azure_response.next_marker.is_none() {
                break;
            }

            next_marker = azure_response.next_marker;
        }

        return Ok(result);
    }

    pub async fn get_list_of_blobs(&self, container_name: &str) -> Result<Vec<String>, Error> {
        let mut result = vec![];

        let mut next_marker: Option<String> = None;

        loop {
            let response = FlUrl::new(self.blobs_api_url.as_str())
                .append_path_segment(container_name)
                .append_query_param("comp", "list")
                .append_query_param("restype", "container")
                .with_header("Content-Length", "0")
                .add_azure_headers(super::super::SignVerb::GET, self, next_marker)
                .get()
                .await?;

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

    pub async fn get_blob_content(
        &self,
        container_name: &str,
        blob_name: &str,
    ) -> Result<Vec<u8>, Error> {
        let response = FlUrl::new(self.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .with_header("Content-Length", "0")
            .add_azure_headers(super::super::SignVerb::GET, self, None)
            .get()
            .await?;

        return response.get_body().await;
    }

    pub async fn delete_blob(&self, container_name: &str, blob_name: &str) -> Result<(), Error> {
        let response = FlUrl::new(self.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .with_header("Content-Length", "0")
            .add_azure_headers(super::super::SignVerb::DELETE, self, None)
            .delete()
            .await?;

        Ok(())
    }

    pub async fn upload_blob(
        &self,
        container_name: &str,
        blob_name: &str,
        content: Vec<u8>,
    ) -> Result<(), Error> {
        FlUrl::new(self.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_path_segment(blob_name)
            .with_header_val_string("Content-Length", content.len().to_string())
            .with_header("x-ms-blob-type", "BlockBlob")
            .add_azure_headers(super::super::SignVerb::PUT, self, None)
            .put(content)
            .await?;

        Ok(())
    }
}
