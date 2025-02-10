use std::sync::Arc;

use rust_extensions::{SliceOrVec, StrOrString};

use crate::{blob::BlobProperties, AzureStorageConnection, AzureStorageError};

use super::{consts::BLOB_PAGE_SIZE, PageBlobAbstractions, PageBlobProperties};

#[async_trait::async_trait]
pub trait MyAzurePageBlobStorage {
    fn get_blob_name(&self) -> &str;
    fn get_container_name(&self) -> &str;
    async fn resize(&self, pages_amount: usize) -> Result<(), AzureStorageError>;
    async fn create_container_if_not_exists(&self) -> Result<(), AzureStorageError>;

    async fn create(&self, pages_amount: usize) -> Result<(), AzureStorageError>;

    async fn create_if_not_exists(
        &self,
        pages_amount: usize,
        auto_create_container: bool,
    ) -> Result<PageBlobProperties, AzureStorageError> {
        match self.get_blob_properties().await {
            Ok(result) => Ok(result.into()),
            Err(err) => match err {
                AzureStorageError::ContainerNotFound => {
                    if auto_create_container {
                        self.create_container_if_not_exists().await?;
                    } else {
                        return Err(err);
                    }

                    self.create(pages_amount).await?;
                    return Ok(BlobProperties {
                        blob_size: pages_amount * BLOB_PAGE_SIZE,
                    }
                    .into());
                }
                AzureStorageError::BlobNotFound => {
                    self.create(pages_amount).await?;
                    return Ok(BlobProperties {
                        blob_size: pages_amount * BLOB_PAGE_SIZE,
                    }
                    .into());
                }
                _ => Err(err),
            },
        }
    }

    async fn get_pages(
        &self,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError>;

    async fn save_pages<'s>(
        &self,
        start_page_no: usize,
        payload: impl Into<SliceOrVec<'s, u8>> + Send + Sync + 'static,
    ) -> Result<(), AzureStorageError>;

    async fn save_pages_ext(
        &self,
        start_page_no: usize,
        payload: Vec<u8>,
        pages_amount_per_round_trip: usize,
    ) -> Result<(), AzureStorageError> {
        let pages_to_save = payload.len() / BLOB_PAGE_SIZE;

        if pages_to_save <= pages_amount_per_round_trip {
            self.save_pages(start_page_no, payload).await?;
            return Ok(());
        }

        let mut pages_offset = 0;

        while pages_offset < pages_to_save {
            let pages_to_save_chunk = pages_to_save - pages_offset;

            let pages_to_save_amount_this_round_trip =
                if pages_to_save_chunk > pages_amount_per_round_trip {
                    pages_amount_per_round_trip
                } else {
                    pages_to_save_chunk
                };

            let payload_to_save = payload[pages_offset * BLOB_PAGE_SIZE
                ..(pages_offset + pages_to_save_amount_this_round_trip) * BLOB_PAGE_SIZE]
                .to_vec();

            self.save_pages(start_page_no + pages_offset, payload_to_save)
                .await?;

            pages_offset += pages_to_save_amount_this_round_trip;
        }

        Ok(())
    }

    async fn delete(&self) -> Result<(), AzureStorageError>;
    async fn download(&self) -> Result<Vec<u8>, AzureStorageError>;

    async fn get_blob_properties(&self) -> Result<PageBlobProperties, AzureStorageError>;
}

pub struct AzurePageBlobStorage {
    connection: Arc<AzureStorageConnection>,
    container_name: String,
    blob_name: String,
    id: String,
}

impl Drop for AzurePageBlobStorage {
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

impl AzurePageBlobStorage {
    pub async fn new(
        connection: Arc<AzureStorageConnection>,
        container_name: impl Into<StrOrString<'static>>,
        blob_name: impl Into<StrOrString<'static>>,
    ) -> Self {
        let id = generate_id();

        let container_name: StrOrString<'static> = container_name.into();
        let container_name = container_name.to_string();
        let blob_name: StrOrString<'static> = blob_name.into();
        let blob_name = blob_name.to_string();

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

    pub fn get_blob_name(&self) -> &str {
        return self.blob_name.as_str();
    }

    pub fn get_container_name(&self) -> &str {
        return self.container_name.as_str();
    }

    pub async fn resize(&self, pages_amount: usize) -> Result<(), AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::sdk_azure::page_blobs::resize_page_blob(
                    connection_data,
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                    pages_amount,
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                connection_data.resize(self.id.as_str(), pages_amount).await
            }
            AzureStorageConnection::InMemory(connection_data) => {
                let container = crate::connection::in_mem::operations::get_container(
                    connection_data,
                    self.container_name.as_ref(),
                )
                .await?;

                container
                    .resize_page_blob(self.blob_name.as_str(), pages_amount)
                    .await
            }
        }
    }

    pub async fn create_container_if_not_exists(&self) -> Result<(), AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::sdk_azure::containers::create_if_not_exists(
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
            AzureStorageConnection::InMemory(connection_data) => {
                connection_data.create(self.container_name.as_ref()).await;
                Ok(())
            }
        }
    }

    pub async fn create(&self, pages_amount: usize) -> Result<(), AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::sdk_azure::page_blobs::create_page_blob(
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
            AzureStorageConnection::InMemory(connection_data) => {
                let container = crate::connection::in_mem::operations::get_container(
                    connection_data,
                    self.container_name.as_ref(),
                )
                .await?;

                container
                    .create_page_blob(self.blob_name.as_ref(), pages_amount)
                    .await
            }
        }
    }

    /*
       pub async fn create_if_not_exists(
           &self,
           pages_amount: usize,
       ) -> Result<usize, AzureStorageError> {
           let props = match self.connection.as_ref() {
               AzureStorageConnection::AzureStorage(connection_data) => {
                   crate::sdk_azure::page_blobs::create_page_blob_if_not_exists(
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
               AzureStorageConnection::InMemory(connection_data) => {
                   let container = crate::connection::in_mem::operations::get_container(
                       connection_data,
                       self.container_name.as_ref(),
                   )
                   .await?;

                   container
                       .create_page_blob_if_not_exists(self.blob_name.as_ref(), pages_amount)
                       .await?
               }
           };

           Ok(props.blob_size / crate::page_blob::consts::BLOB_PAGE_SIZE)
       }
    */

    pub async fn get_pages(
        &self,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::sdk_azure::page_blobs::get_pages(
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
            AzureStorageConnection::InMemory(connection_data) => {
                let container = crate::connection::in_mem::operations::get_container(
                    connection_data,
                    self.container_name.as_ref(),
                )
                .await?;

                container
                    .get_page_blob_pages(self.blob_name.as_ref(), start_page_no, pages_amount)
                    .await
            }
        }
    }

    pub async fn save_pages<'s>(
        &self,
        start_page_no: usize,
        payload: impl Into<SliceOrVec<'s, u8>>,
    ) -> Result<(), AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::sdk_azure::page_blobs::save_pages(
                    connection_data,
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                    start_page_no,
                    payload.into().into_vec(),
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                connection_data
                    .save_pages(self.id.as_str(), start_page_no, payload)
                    .await
            }
            AzureStorageConnection::InMemory(connection_data) => {
                let container = crate::connection::in_mem::operations::get_container(
                    connection_data,
                    self.container_name.as_ref(),
                )
                .await?;

                container
                    .upload_pages_to_page_blob(self.blob_name.as_ref(), start_page_no, payload)
                    .await
            }
        }
    }

    pub async fn delete(&self) -> Result<(), AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::sdk_azure::blobs::delete(
                    connection_data,
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                let result = connection_data.delete_blob(self.id.as_str()).await;

                crate::sdk_files::containers::check_error_if_container_exists(
                    result,
                    connection_data,
                    self.container_name.as_str(),
                )
                .await
            }
            AzureStorageConnection::InMemory(connection_data) => {
                let container = crate::connection::in_mem::operations::get_container(
                    connection_data,
                    self.container_name.as_ref(),
                )
                .await?;

                if container.delete(self.blob_name.as_ref()).await {
                    Ok(())
                } else {
                    Err(AzureStorageError::BlobNotFound)
                }
            }
        }
    }

    pub async fn delete_if_exists(&self) -> Result<(), AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::sdk_azure::blobs::delete_if_exists(
                    connection_data,
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                let result = connection_data
                    .delete_blob_if_exists(self.id.as_str())
                    .await;

                crate::sdk_files::containers::check_error_if_container_exists(
                    result,
                    connection_data,
                    self.container_name.as_str(),
                )
                .await
            }
            AzureStorageConnection::InMemory(connection_data) => {
                let container = crate::connection::in_mem::operations::get_container(
                    connection_data,
                    self.container_name.as_ref(),
                )
                .await?;

                container.delete(self.blob_name.as_ref()).await;

                Ok(())
            }
        }
    }

    pub async fn download(&self) -> Result<Vec<u8>, AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::sdk_azure::blobs::download(
                    connection_data,
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                let result = connection_data.download(self.id.as_str()).await;

                crate::sdk_files::containers::check_error_if_container_exists(
                    result,
                    connection_data,
                    self.container_name.as_str(),
                )
                .await
            }
            AzureStorageConnection::InMemory(connection_data) => {
                let container = crate::connection::in_mem::operations::get_container(
                    connection_data,
                    self.container_name.as_ref(),
                )
                .await?;

                container.download(self.blob_name.as_str()).await
            }
        }
    }

    pub async fn get_blob_properties(&self) -> Result<BlobProperties, AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(connection_data) => {
                crate::sdk_azure::blobs::get_blob_properties(
                    connection_data,
                    self.container_name.as_ref(),
                    self.blob_name.as_ref(),
                )
                .await
            }
            AzureStorageConnection::File(connection_data) => {
                let file_name = crate::sdk_files::utils::compile_blob_path(
                    connection_data,
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                );

                let result = crate::sdk_files::utils::get_blob_properties(file_name.as_str()).await;

                crate::sdk_files::containers::check_error_if_container_exists(
                    result,
                    connection_data,
                    self.container_name.as_str(),
                )
                .await
            }
            AzureStorageConnection::InMemory(connection_data) => {
                let container = crate::connection::in_mem::operations::get_container(
                    connection_data,
                    self.container_name.as_ref(),
                )
                .await?;

                container.get_blob_properties(self.blob_name.as_str()).await
            }
        }
    }
}

#[async_trait::async_trait]
impl PageBlobAbstractions for AzurePageBlobStorage {
    async fn create_container_if_not_exists(&self) -> Result<(), AzureStorageError> {
        self.create_container_if_not_exists().await
    }
    async fn create_blob_if_not_exists(
        &self,
        init_pages_amounts: usize,
        auto_create_container: bool,
    ) -> Result<PageBlobProperties, AzureStorageError> {
        self.create_if_not_exists(init_pages_amounts, auto_create_container)
            .await
    }
}

#[async_trait::async_trait]
impl MyAzurePageBlobStorage for AzurePageBlobStorage {
    fn get_blob_name(&self) -> &str {
        self.get_blob_name()
    }
    fn get_container_name(&self) -> &str {
        self.get_container_name()
    }
    async fn resize(&self, pages_amount: usize) -> Result<(), AzureStorageError> {
        self.resize(pages_amount).await
    }

    async fn create_container_if_not_exists(&self) -> Result<(), AzureStorageError> {
        self.create_container_if_not_exists().await
    }

    async fn create(&self, pages_amount: usize) -> Result<(), AzureStorageError> {
        self.create(pages_amount).await
    }

    async fn get_pages(
        &self,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        self.get_pages(start_page_no, pages_amount).await
    }

    async fn save_pages<'s>(
        &self,
        start_page_no: usize,
        payload: impl Into<SliceOrVec<'s, u8>> + Send + Sync + 'static,
    ) -> Result<(), AzureStorageError> {
        self.save_pages(start_page_no, payload).await
    }

    async fn delete(&self) -> Result<(), AzureStorageError> {
        self.delete().await
    }
    async fn download(&self) -> Result<Vec<u8>, AzureStorageError> {
        self.download().await
    }

    async fn get_blob_properties(&self) -> Result<PageBlobProperties, AzureStorageError> {
        let result = self.get_blob_properties().await?;

        Ok(result.into())
    }
}
fn generate_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
