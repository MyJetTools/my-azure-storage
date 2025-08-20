use crate::{connection::AzureStorageConnectionData, AzureStorageError};
use flurl::{body::FlUrlBody, FlUrl, FlUrlError};

use super::{
    azure_response_handler::ToAzureResponseHandler, flurl_ext::FlUrlAzureExtensions,
    models::NextMarkerToRead,
};

pub struct AzureContainersListReader<'s> {
    connection: &'s AzureStorageConnectionData,
    next_marker: NextMarkerToRead,
}

impl<'s> AzureContainersListReader<'s> {
    pub fn new(connection: &'s AzureStorageConnectionData) -> Self {
        Self {
            connection,
            next_marker: NextMarkerToRead::Start,
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

    pub async fn get_next(&mut self) -> Result<Option<Vec<String>>, FlUrlError> {
        if let NextMarkerToRead::End = &self.next_marker {
            return Ok(None);
        }

        let fl_url: FlUrl = self.connection.into();

        let next_marker = self.get_next_marker();

        let mut response = fl_url
            .append_query_param("comp", Some("list"))
            .add_azure_headers(
                super::sign_utils::SignVerb::GET,
                self.connection,
                None,
                next_marker,
                AZURE_REST_VERSION,
            )
            .get()
            .await?;

        let body = response.get_body_as_slice().await?;

        let azure_response = super::models::deserialize_list_of_containers(body.as_ref());

        if let Some(marker) = azure_response.next_marker {
            self.next_marker = NextMarkerToRead::Next(marker);
        }

        Ok(Some(azure_response.items))
    }
}

pub async fn get_list(connection: &AzureStorageConnectionData) -> Result<Vec<String>, FlUrlError> {
    let mut result = vec![];

    let mut reader = AzureContainersListReader::new(connection);

    while let Some(chunk) = reader.get_next().await? {
        result.extend(chunk);
    }

    Ok(result)
}

pub async fn create_if_not_exists(
    connection: &AzureStorageConnectionData,
    container_name: &str,
) -> Result<(), AzureStorageError> {
    let fl_url: FlUrl = connection.into();

    fl_url
        .append_path_segment(container_name)
        .append_query_param("restype", Some("container"))
        .add_azure_headers(
            super::sign_utils::SignVerb::PUT,
            connection,
            None,
            None,
            AZURE_REST_VERSION,
        )
        .put(FlUrlBody::Empty)
        .await?
        .to_azure_response_handler()
        .check_if_there_is_an_error_and_ignore_one(AzureStorageError::ContainerAlreadyExists)?;

    return Ok(());
}

pub async fn delete(
    connection: &AzureStorageConnectionData,
    container_name: &str,
) -> Result<(), AzureStorageError> {
    let fl_url: FlUrl = connection.into();

    fl_url
        .append_path_segment(container_name)
        .append_query_param("restype", Some("container"))
        .add_azure_headers(
            super::sign_utils::SignVerb::DELETE,
            connection,
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
    connection: &AzureStorageConnectionData,
    container_name: &str,
) -> Result<(), AzureStorageError> {
    let fl_url: FlUrl = connection.into();

    fl_url
        .append_path_segment(container_name)
        .append_query_param("restype", Some("container"))
        .add_azure_headers(
            super::sign_utils::SignVerb::DELETE,
            connection,
            None,
            None,
            AZURE_REST_VERSION,
        )
        .delete()
        .await?
        .to_azure_response_handler()
        .check_if_there_is_an_error_and_ignore_one(AzureStorageError::ContainerNotFound)?;

    Ok(())
}

const AZURE_REST_VERSION: &str = "2017-07-29";

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_create_and_delete_container() {
        let conn_string = std::env::var("TEST_STORAGE_ACCOUNT");

        if conn_string.is_err() {
            println!("TEST_STORAGE_ACCOUNT is not set. Skipping test.");
            return;
        }

        let conn_string = conn_string.unwrap();

        let connection = AzureStorageConnectionData::from_conn_string(conn_string.as_str());

        create_if_not_exists(&connection, "testtest").await.unwrap();

        super::delete(&connection, "testtest").await.unwrap();
    }

    #[tokio::test]
    async fn test_container_not_found() {
        let conn_string = std::env::var("TEST_STORAGE_ACCOUNT");

        if conn_string.is_err() {
            println!("TEST_STORAGE_ACCOUNT is not set. Skipping test.");
            return;
        }

        let conn_string = conn_string.unwrap();

        let connection = AzureStorageConnectionData::from_conn_string(conn_string.as_str());
        println!("Name:{}", connection.account_name);

        let result =
            super::super::blobs::get_blob_properties(&connection, "notexists", "notexists").await;

        if let Err(err) = result {
            assert_eq!(true, matches!(err, AzureStorageError::ContainerNotFound));
        } else {
            panic!("Unexpected result type: {:?}", result);
        }
    }
}
