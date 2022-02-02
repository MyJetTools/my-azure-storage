use crate::{
    azure_response_handler::ToAzureResponseHandler,
    blob_container::models::deserialize_list_of_blobs, flurl_ext::FlUrlAzureExtensions,
    sign_utils::SignVerb, types::AzureStorageError, AzureStorageConnection,
};
use flurl::FlUrl;
use hyper::Error;

pub async fn create_container_if_not_exist(
    connection: &AzureStorageConnection,
    container_name: &str,
) -> Result<(), AzureStorageError> {
    let fl_url: FlUrl = connection.into();

    fl_url
        .append_path_segment(container_name)
        .append_query_param("restype", "container")
        .add_azure_headers(SignVerb::PUT, connection, None, None, AZURE_REST_VERSION)
        .put(None)
        .await?
        .to_azure_response_handler()
        .check_if_there_is_an_error_and_ignore_one(AzureStorageError::ContainerAlreadyExists)?;

    return Ok(());
}

pub async fn delete_container(
    connection: &AzureStorageConnection,
    container_name: &str,
) -> Result<(), AzureStorageError> {
    let fl_url: FlUrl = connection.into();

    fl_url
        .append_path_segment(container_name)
        .append_query_param("restype", "container")
        .add_azure_headers(SignVerb::DELETE, connection, None, None, AZURE_REST_VERSION)
        .delete()
        .await?
        .to_azure_response_handler()
        .check_if_there_is_an_error()?;

    Ok(())
}

pub async fn delete_container_if_exists(
    connection: &AzureStorageConnection,
    container_name: &str,
) -> Result<(), AzureStorageError> {
    let fl_url: FlUrl = connection.into();

    fl_url
        .append_path_segment(container_name)
        .append_query_param("restype", "container")
        .add_azure_headers(SignVerb::DELETE, connection, None, None, AZURE_REST_VERSION)
        .delete()
        .await?
        .to_azure_response_handler()
        .check_if_there_is_an_error_and_ignore_one(AzureStorageError::ContainerNotFound)?;

    Ok(())
}

pub async fn get_list_of_blob_containers(
    connection: &AzureStorageConnection,
) -> Result<Vec<String>, Error> {
    let mut result = vec![];

    let mut next_marker: Option<String> = None;

    loop {
        let fl_url: FlUrl = connection.into();

        let response = fl_url
            .append_query_param("comp", "list")
            .add_azure_headers(
                super::super::sign_utils::SignVerb::GET,
                &connection,
                None,
                next_marker,
                AZURE_REST_VERSION,
            )
            .get()
            .await?;

        let body = response.get_body().await?;

        let azure_response = super::models::deserialize_list_of_containers(body.as_ref());

        result.extend(azure_response.items);

        if azure_response.next_marker.is_none() {
            break;
        }

        next_marker = azure_response.next_marker;
    }

    return Ok(result);
}

pub async fn get_list_of_blobs(
    connection: &AzureStorageConnection,
    container_name: &str,
) -> Result<Vec<String>, AzureStorageError> {
    let mut result = vec![];

    let mut next_marker: Option<String> = None;

    loop {
        let fl_url: FlUrl = connection.into();

        let response = fl_url
            .append_path_segment(container_name)
            .append_query_param("comp", "list")
            .append_query_param("restype", "container")
            .add_azure_headers(
                SignVerb::GET,
                &connection,
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

        result.extend(azure_response.items);

        if azure_response.next_marker.is_none() {
            break;
        }

        next_marker = azure_response.next_marker;
    }

    return Ok(result);
}

const AZURE_REST_VERSION: &str = "2017-07-29";

#[cfg(test)]
mod tests {

    use crate::blob::sdk::get_blob_properties;

    use super::*;

    #[tokio::test]
    async fn test_create_and_delete_container() {
        let conn_string = env!("TEST_STORAGE_ACCOUNT");

        let connection = AzureStorageConnection::from_conn_string(conn_string);

        super::create_container_if_not_exist(&connection, "testtest")
            .await
            .unwrap();

        super::delete_container(&connection, "testtest")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_container_not_found() {
        let conn_string = env!("TEST_STORAGE_ACCOUNT");

        let connection = AzureStorageConnection::from_conn_string(conn_string);
        println!("Name:{}", connection.account_name);

        let result = get_blob_properties(&connection, "notexists", "notexists").await;

        if let Err(err) = result {
            assert_eq!(true, matches!(err, AzureStorageError::ContainerNotFound));
        } else {
            panic!("Unexpected result type: {:?}", result);
        }
    }
}
