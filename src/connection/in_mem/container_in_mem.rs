use std::collections::HashMap;

use tokio::sync::RwLock;

use crate::{blob::BlobProperties, page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

use super::{block_blob::BlockBlob, BlobData, PageBlobInMem};

pub struct ContainerInMem {
    blobs: RwLock<HashMap<String, BlobData>>,
}

impl ContainerInMem {
    pub fn new() -> Self {
        Self {
            blobs: RwLock::new(HashMap::new()),
        }
    }

    pub async fn create_page_blob(
        &self,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        let mut write_access = self.blobs.write().await;

        if write_access.contains_key(blob_name) {
            return Err(AzureStorageError::BlobAlreadyExists);
        }

        write_access.insert(
            blob_name.to_string(),
            BlobData::PageBlob(PageBlobInMem::new(pages_amount)),
        );

        Ok(())
    }

    pub async fn create_page_blob_if_not_exists(
        &self,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<BlobProperties, AzureStorageError> {
        let mut write_access = self.blobs.write().await;

        if let Some(blob_data) = write_access.get(blob_name) {
            if let BlobData::PageBlob(page_blob) = blob_data {
                let result = page_blob.get_blob_properties();
                return Ok(result);
            } else {
                panic!("{}", NOT_A_PAGE_BLOB_PANIC);
            }
        }

        write_access.insert(
            blob_name.to_string(),
            BlobData::PageBlob(PageBlobInMem::new(pages_amount)),
        );

        Ok(BlobProperties {
            blob_size: pages_amount * BLOB_PAGE_SIZE,
        })
    }

    pub async fn get_list_of_blobs(&self) -> Vec<String> {
        let read_access = self.blobs.read().await;
        read_access.keys().map(|itm| itm.to_string()).collect()
    }

    pub async fn upload_block_blob(&self, blob_name: String, content: Vec<u8>) {
        let mut write_access = self.blobs.write().await;
        write_access.insert(blob_name, BlobData::BlockBlob(BlockBlob::new(content)));
    }

    pub async fn download(&self, blob_name: &str) -> Result<Vec<u8>, AzureStorageError> {
        let read_access = self.blobs.read().await;

        match read_access.get(blob_name) {
            Some(blob_data) => Ok(blob_data.download()),
            None => Err(AzureStorageError::BlobNotFound),
        }
    }

    pub async fn get_blob_properties(
        &self,
        blob_name: &str,
    ) -> Result<BlobProperties, AzureStorageError> {
        let read_access = self.blobs.read().await;

        match read_access.get(blob_name) {
            Some(blob_data) => Ok(blob_data.get_blob_properties()),
            None => Err(AzureStorageError::BlobNotFound),
        }
    }

    pub async fn delete(&self, blob_name: &str) -> bool {
        let mut write_access = self.blobs.write().await;
        let result = write_access.remove(blob_name);
        return result.is_some();
    }

    pub async fn resize_page_blob(
        &self,
        blob_name: &str,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        let mut write_access = self.blobs.write().await;

        let blob_data = write_access.get_mut(blob_name);

        if let Some(blob_data) = blob_data {
            if let BlobData::PageBlob(page_blob) = blob_data {
                page_blob.resize(pages_amount);
                return Ok(());
            } else {
                panic!("{}", NOT_A_PAGE_BLOB_PANIC);
            }
        }

        return Err(AzureStorageError::BlobNotFound);
    }

    pub async fn upload_pages_to_page_blob(
        &self,
        blob_name: &str,
        start_page_no: usize,
        content: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        let mut write_access = self.blobs.write().await;

        let blob_data = write_access.get_mut(blob_name);

        if let Some(blob_data) = blob_data {
            if let BlobData::PageBlob(page_blob) = blob_data {
                page_blob.save_pages(start_page_no, content);
                return Ok(());
            } else {
                panic!("{}", NOT_A_PAGE_BLOB_PANIC);
            }
        }

        return Err(AzureStorageError::BlobNotFound);
    }

    pub async fn get_page_blob_pages(
        &self,
        blob_name: &str,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        let read_access = self.blobs.write().await;

        let blob_data = read_access.get(blob_name);

        if let Some(blob_data) = blob_data {
            if let BlobData::PageBlob(page_blob) = blob_data {
                let result = page_blob.get_pages(start_page_no, pages_amount);
                return Ok(result);
            } else {
                panic!("{}", NOT_A_PAGE_BLOB_PANIC);
            }
        }

        return Err(AzureStorageError::BlobNotFound);
    }
}

const NOT_A_PAGE_BLOB_PANIC: &str = "The type of the blob is not a pageBlob";
