use std::io::SeekFrom;

use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::blob::BlobProperties;
use crate::page_blob::consts::BLOB_PAGE_SIZE;
use crate::sdk_files::utils::FileConnectionInfo;
use crate::AzureStorageError;

pub struct PageBlobFileEngine {
    file: Option<File>,
    root_path: String,
    path_separator: char,
    container_name: String,
    blob_name: String,
}

impl FileConnectionInfo for PageBlobFileEngine {
    fn get_root_path(&self) -> &str {
        self.root_path.as_str()
    }

    fn get_separator(&self) -> char {
        self.path_separator
    }
}

impl PageBlobFileEngine {
    pub fn new(
        root_path: String,
        path_separator: char,
        container_name: String,
        blob_name: String,
    ) -> Self {
        Self {
            file: None,
            root_path,
            path_separator,
            container_name,
            blob_name,
        }
    }

    async fn get_file_mut(&mut self) -> Result<&mut File, AzureStorageError> {
        if self.file.is_some() {
            return Ok(self.file.as_mut().unwrap());
        }

        let folder_name =
            crate::sdk_files::utils::compile_container_path(self, self.container_name.as_str());

        if !fs::metadata(folder_name.as_str()).await.is_ok() {
            return Err(AzureStorageError::ContainerNotFound);
        }

        let file_name = crate::sdk_files::utils::compile_blob_path(
            self,
            self.container_name.as_str(),
            self.blob_name.as_str(),
        );

        let file_result = File::open(file_name.as_str()).await;

        match file_result {
            Ok(file) => {
                self.file = Some(file);
                Ok(self.file.as_mut().unwrap())
            }
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => {
                    return Err(AzureStorageError::BlobNotFound);
                }

                _ => Err(AzureStorageError::UnknownError {
                    msg: format!("{:?}", err),
                }),
            },
        }
    }

    pub async fn download(&mut self) -> Result<Vec<u8>, AzureStorageError> {
        let file = self.get_file_mut().await?;

        let mut result = Vec::new();
        file.seek(SeekFrom::Start(0)).await?;
        file.read_to_end(&mut result).await?;
        return Ok(result);
    }

    pub async fn resize(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        let file_access = self.get_file_mut().await?;
        let size = BLOB_PAGE_SIZE * pages_amount;
        file_access.set_len(size as u64).await?;
        return Ok(());
    }

    pub async fn upload(
        &mut self,
        start_page: usize,
        payload: &[u8],
    ) -> Result<(), AzureStorageError> {
        let file_access = self.get_file_mut().await?;

        let start_pos = BLOB_PAGE_SIZE * start_page;

        file_access
            .seek(std::io::SeekFrom::Start(start_pos as u64))
            .await?;

        file_access.write_all(payload).await?;

        return Ok(());
    }

    pub async fn create_container_if_not_exists(&mut self) -> Result<(), AzureStorageError> {
        if self.file.is_some() {
            return Ok(());
        }

        crate::sdk_files::containers::create_if_not_exists(self, self.container_name.as_str()).await
    }

    pub async fn delete_container_if_exists(&mut self) -> Result<(), AzureStorageError> {
        self.file = None;

        let folder_name =
            crate::sdk_files::utils::compile_container_path(self, self.container_name.as_str());

        match tokio::fs::remove_dir_all(folder_name.as_str()).await {
            Ok(_) => Ok(()),
            Err(err) => match err.kind() {
                std::io::ErrorKind::AlreadyExists => Ok(()),
                _ => {
                    return Err(AzureStorageError::UnknownError {
                        msg: format!("{:?}", err),
                    });
                }
            },
        }
    }

    pub async fn create_blob(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        if self.file.is_some() {
            return Err(AzureStorageError::BlobAlreadyExists);
        }

        let folder_name =
            crate::sdk_files::utils::compile_container_path(self, self.container_name.as_str());

        if tokio::fs::metadata(folder_name.as_str()).await.is_err() {
            return Err(AzureStorageError::ContainerNotFound);
        }

        let file_name = crate::sdk_files::utils::compile_blob_path(
            self,
            self.container_name.as_str(),
            self.blob_name.as_str(),
        );

        if tokio::fs::metadata(file_name.as_str()).await.is_ok() {
            return Err(AzureStorageError::BlobAlreadyExists);
        }

        let file = tokio::fs::File::create(file_name.as_str()).await?;

        self.file = Some(file);

        self.resize(pages_amount).await?;

        Ok(())
    }

    pub async fn create_blob_if_not_exists(
        &mut self,
        pages_amount: usize,
    ) -> Result<BlobProperties, AzureStorageError> {
        if self.file.is_some() {
            return Err(AzureStorageError::BlobAlreadyExists);
        }

        let folder_name =
            crate::sdk_files::utils::compile_container_path(self, self.container_name.as_str());

        if tokio::fs::metadata(folder_name.as_str()).await.is_err() {
            return Err(AzureStorageError::ContainerNotFound);
        }

        let file_name = crate::sdk_files::utils::compile_blob_path(
            self,
            self.container_name.as_str(),
            self.blob_name.as_str(),
        );

        if self.get_file_mut().await.is_ok() {
            return crate::sdk_files::utils::get_blob_properties(file_name.as_str()).await;
        }

        let file = tokio::fs::File::create(file_name.as_str()).await?;

        self.file = Some(file);

        self.resize(pages_amount).await?;

        return crate::sdk_files::utils::get_blob_properties(file_name.as_str()).await;
    }

    pub async fn get(
        &mut self,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        let file = self.get_file_mut().await?;
        let pos = BLOB_PAGE_SIZE * start_page_no;

        file.seek(SeekFrom::Start(pos as u64)).await?;

        let read_size = pages_amount * BLOB_PAGE_SIZE;

        let mut result = Vec::with_capacity(read_size);

        file.read_exact(&mut result).await?;

        Ok(result)
    }

    pub async fn save_pages(
        &mut self,
        start_page_no: usize,
        payload: &[u8],
    ) -> Result<(), AzureStorageError> {
        let file = self.get_file_mut().await?;
        let pos = BLOB_PAGE_SIZE * start_page_no;

        file.seek(SeekFrom::Start(pos as u64)).await?;

        file.write(payload).await?;

        Ok(())
    }

    pub async fn delete_blob(&mut self) -> Result<(), AzureStorageError> {
        self.file = None;
        let file_name = crate::sdk_files::utils::compile_blob_path(
            self,
            self.container_name.as_str(),
            self.blob_name.as_str(),
        );
        tokio::fs::remove_file(file_name.as_str()).await?;
        Ok(())
    }

    pub async fn delete_blob_if_exists(&mut self) -> Result<(), AzureStorageError> {
        self.file = None;
        let file_name = crate::sdk_files::utils::compile_blob_path(
            self,
            self.container_name.as_str(),
            self.blob_name.as_str(),
        );

        match tokio::fs::remove_file(file_name.as_str()).await {
            Ok(_) => Ok(()),
            Err(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::connection::FileConnectionData;

    #[tokio::test]
    async fn test_create_delete_container() {
        const CONTAINER_NAME: &str = "test-container";
        let folder = crate::sdk_files::test_utils::get_test_folder();

        let connection_data = FileConnectionData::new(folder);
        let id = "AAA";

        connection_data
            .apply_page_blob_data(
                id.to_string(),
                CONTAINER_NAME.to_string(),
                "test_file".to_string(),
            )
            .await;

        connection_data
            .create_container_if_not_exists(id)
            .await
            .unwrap();

        connection_data
            .delete_container_if_exists(id)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_blob() {
        const CONTAINER_NAME: &str = "test-create-blob";
        let folder = crate::sdk_files::test_utils::get_test_folder();

        let connection_data = FileConnectionData::new(folder);
        let id = "AAA";

        connection_data
            .apply_page_blob_data(
                id.to_string(),
                CONTAINER_NAME.to_string(),
                "blob-name".to_string(),
            )
            .await;

        connection_data
            .create_container_if_not_exists(id)
            .await
            .unwrap();

        connection_data.create_blob(id, 1).await.unwrap();

        connection_data
            .delete_container_if_exists(id)
            .await
            .unwrap();
    }
}
