use std::collections::HashMap;

use rust_extensions::AsSliceOrVec;
use tokio::sync::Mutex;

use crate::{blob::BlobProperties, sdk_files::utils::FileConnectionInfo, AzureStorageError};

use super::PageBlobFileEngine;

pub struct FileConnectionData {
    page_data: Mutex<HashMap<String, PageBlobFileEngine>>,
    pub root_path: String,
    pub path_separator: char,
}

impl FileConnectionInfo for FileConnectionData {
    fn get_root_path(&self) -> &str {
        self.root_path.as_str()
    }

    fn get_separator(&self) -> char {
        self.path_separator
    }
}

impl FileConnectionData {
    pub fn new(root_path: String) -> Self {
        let path_separator = std::path::MAIN_SEPARATOR;
        let root_path = if root_path.ends_with(path_separator) {
            root_path
        } else {
            format!("{}{}", root_path, path_separator)
        };

        Self {
            root_path: root_path.to_string(),
            path_separator,
            page_data: Mutex::new(HashMap::new()),
        }
    }

    pub async fn apply_page_blob_data(
        &self,
        id: String,
        container_name: String,
        blob_name: String,
    ) {
        let mut page_data_access = self.page_data.lock().await;
        let engine = PageBlobFileEngine::new(
            self.root_path.to_string(),
            self.path_separator,
            container_name,
            blob_name,
        );
        page_data_access.insert(id, engine);
    }

    pub async fn drop_page_blob_data(&self, id: String) {
        let mut page_data_access = self.page_data.lock().await;
        page_data_access.remove(id.as_str());
    }

    pub async fn download(&self, id: &str) -> Result<Vec<u8>, AzureStorageError> {
        let mut write_access = self.page_data.lock().await;

        if let Some(page_blob_file) = write_access.get_mut(id) {
            return page_blob_file.download().await;
        }

        panic!("{}", PANIC_MESSAGE);
    }

    pub async fn resize(&self, id: &str, pages_amount: usize) -> Result<(), AzureStorageError> {
        let mut write_access = self.page_data.lock().await;

        if let Some(page_blob_file) = write_access.get_mut(id) {
            return page_blob_file.resize(pages_amount).await;
        }

        panic!("{}", PANIC_MESSAGE);
    }

    pub async fn upload<'s>(
        &self,
        id: &str,
        start_page: usize,
        payload: impl Into<AsSliceOrVec<'s, u8>>,
    ) -> Result<(), AzureStorageError> {
        let mut write_access = self.page_data.lock().await;

        if let Some(page_blob_file) = write_access.get_mut(id) {
            return page_blob_file.upload(start_page, payload).await;
        }

        panic!("{}", PANIC_MESSAGE);
    }

    pub async fn create_container_if_not_exists(&self, id: &str) -> Result<(), AzureStorageError> {
        let mut write_access = self.page_data.lock().await;

        if let Some(page_blob_file) = write_access.get_mut(id) {
            return page_blob_file.create_container_if_not_exists().await;
        }

        panic!("{}", PANIC_MESSAGE);
    }

    pub async fn delete_container_if_exists(&self, id: &str) -> Result<(), AzureStorageError> {
        let mut write_access = self.page_data.lock().await;

        if let Some(page_blob_file) = write_access.get_mut(id) {
            return page_blob_file.delete_container_if_exists().await;
        }

        panic!("{}", PANIC_MESSAGE);
    }

    pub async fn create_blob(
        &self,
        id: &str,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        let mut write_access = self.page_data.lock().await;

        if let Some(page_blob_file) = write_access.get_mut(id) {
            return page_blob_file.create_blob(pages_amount).await;
        }

        panic!("{}", PANIC_MESSAGE);
    }

    pub async fn create_blob_if_not_exists(
        &self,
        id: &str,
        pages_amount: usize,
    ) -> Result<BlobProperties, AzureStorageError> {
        let mut write_access = self.page_data.lock().await;

        if let Some(page_blob_file) = write_access.get_mut(id) {
            return page_blob_file.create_blob_if_not_exists(pages_amount).await;
        }

        panic!("{}", PANIC_MESSAGE);
    }

    pub async fn delete_blob(&self, id: &str) -> Result<(), AzureStorageError> {
        let mut write_access = self.page_data.lock().await;

        if let Some(page_blob_file) = write_access.get_mut(id) {
            return page_blob_file.delete_blob().await;
        }

        panic!("{}", PANIC_MESSAGE);
    }

    pub async fn delete_blob_if_exists(&self, id: &str) -> Result<(), AzureStorageError> {
        let mut write_access = self.page_data.lock().await;

        if let Some(page_blob_file) = write_access.get_mut(id) {
            return page_blob_file.delete_blob_if_exists().await;
        }

        panic!("{}", PANIC_MESSAGE);
    }

    pub async fn get(
        &self,
        id: &str,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        let mut write_access = self.page_data.lock().await;

        if let Some(page_blob_file) = write_access.get_mut(id) {
            return page_blob_file.get(start_page_no, pages_amount).await;
        }

        panic!("{}", PANIC_MESSAGE);
    }

    pub async fn save_pages<'s>(
        &self,
        id: &str,
        start_page_no: usize,
        payload: impl Into<AsSliceOrVec<'s, u8>>,
    ) -> Result<(), AzureStorageError> {
        let mut write_access = self.page_data.lock().await;

        if let Some(page_blob_file) = write_access.get_mut(id) {
            return page_blob_file.save_pages(start_page_no, payload).await;
        }

        panic!("{}", PANIC_MESSAGE);
    }
}

const PANIC_MESSAGE: &str = "Page blob data is not initialized";
