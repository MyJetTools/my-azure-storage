use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use super::ContainerInMem;
#[cfg(feature = "table-storage")]
use super::TableStorageInMem;
#[cfg(feature = "table-storage")]
use crate::table_storage::TableStorageError;

pub struct MemStorageData {
    containers: RwLock<HashMap<String, Arc<ContainerInMem>>>,
    #[cfg(feature = "table-storage")]
    tables: RwLock<HashMap<String, Arc<TableStorageInMem>>>,
}

impl MemStorageData {
    pub fn new() -> Self {
        Self {
            containers: RwLock::new(HashMap::new()),
            #[cfg(feature = "table-storage")]
            tables: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_list(&self) -> Vec<String> {
        let read_access = self.containers.read().await;
        read_access.keys().map(|itm| itm.to_string()).collect()
    }

    pub async fn create(&self, container_name: &str) {
        let mut write_access = self.containers.write().await;
        if write_access.contains_key(container_name) {
            return;
        }
        write_access.insert(container_name.to_string(), Arc::new(ContainerInMem::new()));
    }

    pub async fn get_container(&self, container_name: &str) -> Option<Arc<ContainerInMem>> {
        let read_access = self.containers.read().await;
        let result = read_access.get(container_name)?;
        Some(result.clone())
    }

    pub async fn delete(&self, container_name: &str) -> bool {
        let mut write_access = self.containers.write().await;
        write_access.remove(container_name).is_some()
    }

    #[cfg(feature = "table-storage")]
    pub async fn create_table(&self, table_name: &str) -> Result<(), TableStorageError> {
        let mut write_access = self.tables.write().await;
        if write_access.contains_key(table_name) {
            return Err(TableStorageError::TableAlreadyExists);
        }

        write_access.insert(table_name.to_string(), Arc::new(TableStorageInMem::new()));
        Ok(())
    }

    #[cfg(feature = "table-storage")]
    pub async fn create_table_if_not_exists(
        &self,
        table_name: &str,
    ) -> Result<(), TableStorageError> {
        match self.create_table(table_name).await {
            Ok(_) => Ok(()),
            Err(TableStorageError::TableAlreadyExists) => Ok(()),
            Err(err) => Err(err),
        }
    }

    #[cfg(feature = "table-storage")]
    pub async fn get_table(&self, table_name: &str) -> Option<Arc<TableStorageInMem>> {
        let read_access = self.tables.read().await;
        let result = read_access.get(table_name)?;
        Some(result.clone())
    }

    #[cfg(feature = "table-storage")]
    pub async fn get_table_list(&self) -> Vec<String> {
        let read_access = self.tables.read().await;
        read_access.keys().map(|itm| itm.to_string()).collect()
    }
}
