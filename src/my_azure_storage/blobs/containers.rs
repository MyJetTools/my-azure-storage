use super::super::AzureConnection;
use crate::{
    my_azure_storage::{azure_response_handler::*, FlUrlAzureExtensions},
    AzureStorageError,
};
use flurl::FlUrl;
use hyper::Error;

impl AzureConnection {
    pub async fn create_container_if_not_exist(
        &self,
        container_name: &str,
    ) -> Result<(), AzureStorageError> {
        FlUrl::new(self.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_query_param("restype", "container")
            .add_azure_headers(
                super::super::SignVerb::PUT,
                self,
                None,
                None,
                AZURE_REST_VERSION,
            )
            .put(None)
            .await?
            .to_azure_response_handler()
            .check_if_there_is_an_error_and_ignore_one(AzureStorageError::ContainerAlreadyExists)?;

        return Ok(());
    }

    pub async fn delete_container(&self, container_name: &str) -> Result<(), AzureStorageError> {
        FlUrl::new(self.blobs_api_url.as_str())
            .append_path_segment(container_name)
            .append_query_param("restype", "container")
            .add_azure_headers(
                super::super::SignVerb::DELETE,
                self,
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

    pub async fn get_list_of_blob_containers(&self) -> Result<Vec<String>, Error> {
        let mut result = vec![];

        let mut next_marker: Option<String> = None;

        loop {
            let response = FlUrl::new(self.blobs_api_url.as_str())
                .append_query_param("comp", "list")
                .add_azure_headers(
                    super::super::sign_utils::SignVerb::GET,
                    &self,
                    None,
                    next_marker,
                    AZURE_REST_VERSION,
                )
                .get()
                .await?;

            let body = response.get_body().await?;

            let azure_response =
                super::container_models::deserialize_list_of_containers(body.as_ref());

            result.extend(azure_response.items);

            if azure_response.next_marker.is_none() {
                break;
            }

            next_marker = azure_response.next_marker;
        }

        return Ok(result);
    }
}

const AZURE_REST_VERSION: &str = "2017-07-29";

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_and_delete_container() {
        let conn_string = env!("TEST_STORAGE_ACCOUNT");

        let connection = AzureConnection::from_conn_string(conn_string);

        connection
            .create_container_if_not_exist("testtest")
            .await
            .unwrap();

        connection.delete_container("testtest").await.unwrap();
    }
}
