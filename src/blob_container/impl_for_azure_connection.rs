use crate::{connection::AzureStorageConnection, types::AzureStorageError};

use super::api::BlobContainersApi;

use async_trait::async_trait;
use hyper::Error;

#[async_trait]
impl BlobContainersApi for AzureStorageConnection {
    async fn create_container_if_not_exist(
        &self,
        container_name: &str,
    ) -> Result<(), AzureStorageError> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                super::sdk::create_container_if_not_exist(connection_data, container_name).await
            }
            AzureStorageConnection::File(connection_data) => {
                let dir_name =
                    crate::file_utils::compile_container_path(connection_data, container_name);

                tokio::fs::create_dir(dir_name).await?;

                Ok(())
            }
        }
    }

    async fn delete_container(&self, container_name: &str) -> Result<(), AzureStorageError> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                super::sdk::delete_container(connection_data, container_name).await
            }
            AzureStorageConnection::File(connection_data) => {
                let dir_name =
                    crate::file_utils::compile_container_path(connection_data, container_name);
                tokio::fs::remove_dir_all(dir_name).await?;
                Ok(())
            }
        }
    }

    async fn delete_container_if_exists(
        &self,
        container_name: &str,
    ) -> Result<(), AzureStorageError> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                super::sdk::delete_container_if_exists(connection_data, container_name).await
            }
            AzureStorageConnection::File(connection_data) => {
                let dir_name =
                    crate::file_utils::compile_container_path(connection_data, container_name);

                match tokio::fs::remove_dir_all(dir_name).await {
                    Ok(_) => Ok(()),
                    Err(_) => Ok(()),
                }
            }
        }
    }

    async fn get_list_of_blob_containers(&self) -> Result<Vec<String>, Error> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                super::sdk::get_list_of_blob_containers(connection_data).await
            }
            AzureStorageConnection::File(connection_data) => {
                let mut result = Vec::new();

                let path = connection_data.root_path.as_str();

                for entry in std::fs::read_dir(path).unwrap() {
                    if let Ok(entity) = entry {
                        let file_type = entity.file_type().unwrap();

                        if file_type.is_dir() {
                            let path = entity.path();

                            let path = format!("{}", path.display());

                            result.push(
                                extract_file_name(path.as_str(), connection_data.path_separator)
                                    .to_string(),
                            );
                        }
                    }
                }

                Ok(result)
            }
        }
    }

    async fn get_list_of_blobs(
        &self,
        container_name: &str,
    ) -> Result<Vec<String>, AzureStorageError> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                super::sdk::get_list_of_blobs(connection_data, container_name).await
            }
            AzureStorageConnection::File(connection_data) => {
                let container_path =
                    crate::file_utils::compile_container_path(connection_data, container_name);
                let mut result = Vec::new();

                for entry in std::fs::read_dir(container_path.as_str()).unwrap() {
                    if let Ok(entity) = entry {
                        let file_type = entity.file_type().unwrap();

                        if file_type.is_dir() {
                            let path = entity.path();

                            let path = format!("{}", path.display());

                            result.push(
                                extract_file_name(path.as_str(), connection_data.path_separator)
                                    .to_string(),
                            );
                        }
                    }
                }

                Ok(result)
            }
        }
    }
}

pub fn extract_file_name(full_path: &str, separator: char) -> &str {
    let full_path_as_bytes = full_path.as_bytes();

    for index in (0..full_path_as_bytes.len()).rev() {
        if full_path_as_bytes[index] == separator as u8 {
            return &full_path[index + 1..];
        }
    }

    panic!("Can not extract filename from fullpath [{}]", full_path);
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_extract_file_name() {
        let src_path = "/Users/Folder/FileName";
        let result = extract_file_name(src_path, '/');
        assert_eq!("FileName", result);
    }
}
