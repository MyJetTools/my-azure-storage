use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use super::ContainerInMem;

pub struct MemStorageData {
    containers: RwLock<HashMap<String, Arc<ContainerInMem>>>,
}

impl MemStorageData {
    pub fn new() -> Self {
        Self {
            containers: RwLock::new(HashMap::new()),
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
}
