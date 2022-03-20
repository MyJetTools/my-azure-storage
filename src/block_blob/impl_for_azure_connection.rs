use async_trait::async_trait;

use crate::{connection::AzureStorageConnection, types::AzureStorageError};

use super::api::BlockBlobApi;

#[async_trait]
impl BlockBlobApi for AzureStorageConnection {
    async fn upload(
        &self,
        container_name: &str,
        blob_name: &str,
        content: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        match self {
            AzureStorageConnection::AzureStorage(connection_data) => {
                super::sdk::upload(connection_data, container_name, blob_name, content).await
            }
            AzureStorageConnection::File(connection_data) => {
                crate::sdk_files::blobs::upload(
                    connection_data,
                    container_name,
                    blob_name,
                    content.as_slice(),
                )
                .await
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        blob::BlobApi, blob_container::BlobContainersApi, block_blob::BlockBlobApi,
        AzureStorageConnection,
    };

    #[tokio::test]
    async fn test_several_uploads() {
        const CONTAINER_NAME: &str = "test-several-uploads";
        const BLOB_NAME: &str = "test-blob-name";
        let folder = crate::sdk_files::test_utils::get_test_folder();

        let connection_string = AzureStorageConnection::from_conn_string(folder.as_str());

        connection_string
            .create_container_if_not_exist(CONTAINER_NAME)
            .await
            .unwrap();

        let container_folder = format!("{}{}", folder.as_str(), CONTAINER_NAME);

        tokio::fs::metadata(container_folder.as_str())
            .await
            .unwrap();

        let src_content = vec![0u8, 1u8, 2u8];
        connection_string
            .upload(CONTAINER_NAME, BLOB_NAME, src_content.clone())
            .await
            .unwrap();

        let result_content = connection_string
            .download_blob(CONTAINER_NAME, BLOB_NAME)
            .await
            .unwrap();

        assert_eq!(src_content, result_content);

        let src_content = vec![3u8, 4u8, 5u8, 6u8];
        connection_string
            .upload(CONTAINER_NAME, BLOB_NAME, src_content.clone())
            .await
            .unwrap();

        let result_content = connection_string
            .download_blob(CONTAINER_NAME, BLOB_NAME)
            .await
            .unwrap();

        assert_eq!(src_content, result_content);

        tokio::fs::remove_dir_all(container_folder.as_str())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_delete_container() {
        const CONTAINER_NAME: &str = "test-delete-container";
        const BLOB_NAME: &str = "test-blob-name";
        let folder = crate::sdk_files::test_utils::get_test_folder();

        let connection_string = AzureStorageConnection::from_conn_string(folder.as_str());

        connection_string
            .create_container_if_not_exist(CONTAINER_NAME)
            .await
            .unwrap();

        let container_folder = format!("{}{}", folder.as_str(), CONTAINER_NAME);

        tokio::fs::metadata(container_folder.as_str())
            .await
            .unwrap();

        let src_content = vec![0u8, 1u8, 2u8];
        connection_string
            .upload(CONTAINER_NAME, BLOB_NAME, src_content.clone())
            .await
            .unwrap();

        let result_content = connection_string
            .download_blob(CONTAINER_NAME, BLOB_NAME)
            .await
            .unwrap();

        assert_eq!(src_content, result_content);

        connection_string
            .delete_blob(CONTAINER_NAME, BLOB_NAME)
            .await
            .unwrap();

        tokio::fs::remove_dir_all(container_folder.as_str())
            .await
            .unwrap();
    }
}
