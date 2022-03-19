use std::sync::Arc;

use async_trait::async_trait;

use crate::{blob::BlobProperties, AzureStorageConnection, AzureStorageError};

use super::{consts::BLOB_PAGE_SIZE, AzurePageBlobStorage};

pub struct MyAzurePageBlobStorage {
    connection: Arc<AzureStorageConnection>,
    container_name: String,
    blob_name: String,
    id: String,
}

impl MyAzurePageBlobStorage {
    pub async fn new(
        connection: Arc<AzureStorageConnection>,
        container_name: String,
        blob_name: String,
    ) -> Self {
        let id = generate_id();

        if let AzureStorageConnection::File(connection_data) = connection.as_ref() {
            connection_data
                .apply_page_blob_data(
                    id.to_string(),
                    container_name.to_string(),
                    blob_name.to_string(),
                )
                .await;
        }

        Self {
            connection,
            container_name,
            blob_name,
            id,
        }
    }
}

impl Drop for MyAzurePageBlobStorage {
    fn drop(&mut self) {
        if self.connection.as_ref().is_file() {
            let id = self.id.to_string();
            let connection = self.connection.clone();
            tokio::spawn(async move {
                if let AzureStorageConnection::File(connection_data) = connection.as_ref() {
                    connection_data.drop_page_blob_data(id).await;
                }
            });
        }
    }
}

#[async_trait]
impl AzurePageBlobStorage for MyAzurePageBlobStorage {
    fn get_blob_name(&self) -> &str {
        return self.blob_name.as_str();
    }

    fn get_container_name(&self) -> &str {
        return self.container_name.as_str();
    }

    async fn resize(&self, pages_amount: usize) -> Result<(), AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                super::sdk::resize_page_blob(
                    connection_data,
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                    pages_amount,
                )
                .await?;
            }
            AzureStorageConnection::File(connection_data) => {
                connection_data
                    .ressize(self.id.as_str(), pages_amount)
                    .await?;
            }
        }

        Ok(())
    }

    async fn create_container_if_not_exist(&self) -> Result<(), AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::blob_container::sdk::create_container_if_not_exist(
                    connection_data,
                    self.container_name.as_str(),
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                connection_data
                    .create_container_if_not_exists(self.id.as_str())
                    .await?;

                Ok(())
            }
        }
    }

    async fn get_available_pages_amount(&self) -> Result<usize, AzureStorageError> {
        let props = match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::blob::sdk::get_blob_properties(
                    connection_data,
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                )
                .await?
            }
            AzureStorageConnection::File(connection_data) => {
                let file_name = crate::file_utils::compile_blob_path(
                    connection_data,
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                );

                crate::file_utils::get_blob_properties(file_name.as_str()).await?
            }
        };

        Ok(props.blob_size / BLOB_PAGE_SIZE)
    }

    async fn create(&self, pages_amount: usize) -> Result<(), AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::page_blob::sdk::create_page_blob(
                    connection_data,
                    self.container_name.as_str(),
                    &self.blob_name,
                    pages_amount,
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                connection_data
                    .create_blob(self.id.as_str(), pages_amount)
                    .await
            }
        }
    }

    async fn create_if_not_exists(&self, pages_amount: usize) -> Result<usize, AzureStorageError> {
        let props = match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::page_blob::sdk::create_page_blob_if_not_exists(
                    connection_data,
                    self.container_name.as_str(),
                    &self.blob_name,
                    pages_amount,
                )
                .await?
            }
            AzureStorageConnection::File(connection_data) => {
                connection_data
                    .create_blob_if_not_exists(self.id.as_str(), pages_amount)
                    .await?
            }
        };

        Ok(props.blob_size / crate::page_blob::consts::BLOB_PAGE_SIZE)
    }

    async fn get(
        &self,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::page_blob::sdk::get_pages(
                    connection_data,
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                    start_page_no,
                    pages_amount,
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                connection_data
                    .get(self.id.as_str(), start_page_no, pages_amount)
                    .await
            }
        }
    }

    async fn save_pages(
        &self,
        start_page_no: usize,
        payload: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::page_blob::sdk::save_pages(
                    connection_data,
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                    start_page_no,
                    payload,
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                connection_data
                    .save_pages(self.id.as_str(), start_page_no, payload.as_ref())
                    .await
            }
        }
    }

    async fn delete(&self) -> Result<(), AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::blob::sdk::delete_blob(
                    connection_data,
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                connection_data.delete_blob(self.id.as_str()).await
            }
        }
    }

    async fn delete_if_exists(&self) -> Result<(), AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::blob::sdk::delete_blob_if_exists(
                    connection_data,
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                connection_data
                    .delete_blob_if_exists(self.id.as_str())
                    .await
            }
        }
    }

    async fn download(&self) -> Result<Vec<u8>, AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::blob::sdk::download_blob(
                    connection_data,
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                let result = connection_data.download(self.id.as_str()).await?;
                return Ok(result);
            }
        }
    }

    async fn get_blob_properties(&self) -> Result<BlobProperties, AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::blob::sdk::get_blob_properties(
                    connection_data,
                    self.container_name.as_ref(),
                    self.blob_name.as_ref(),
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                let file_name = crate::file_utils::compile_blob_path(
                    connection_data,
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                );

                crate::file_utils::get_blob_properties(file_name.as_str()).await
            }
        }
    }
}

fn generate_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
