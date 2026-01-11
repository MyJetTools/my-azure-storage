use std::collections::BTreeMap;

use tokio::sync::RwLock;

use crate::table_storage::TableStorageError;

pub struct TableStorageInMem {
    entities: RwLock<BTreeMap<(String, String), Vec<u8>>>,
}

impl TableStorageInMem {
    pub fn new() -> Self {
        Self {
            entities: RwLock::new(BTreeMap::new()),
        }
    }

    pub async fn insert_or_replace(&self, partition_key: &str, row_key: &str, payload: Vec<u8>) {
        let mut write = self.entities.write().await;
        write.insert(make_key(partition_key, row_key), payload);
    }

    pub async fn insert(
        &self,
        partition_key: &str,
        row_key: &str,
        payload: Vec<u8>,
    ) -> Result<(), TableStorageError> {
        let mut write = self.entities.write().await;
        let key = make_key(partition_key, row_key);
        if write.contains_key(&key) {
            return Err(TableStorageError::EntityAlreadyExists);
        }

        write.insert(key, payload);
        Ok(())
    }

    pub async fn get_entity(&self, partition_key: &str, row_key: &str) -> Option<Vec<u8>> {
        let read = self.entities.read().await;
        read.get(&make_key(partition_key, row_key)).cloned()
    }

    pub async fn get_by_partition(&self, partition_key: &str) -> Option<Vec<Vec<u8>>> {
        let read = self.entities.read().await;
        let mut result = Vec::new();
        for ((pk, _), value) in read.iter() {
            if pk == partition_key {
                result.push(value.clone());
            }
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    pub async fn get_all(&self) -> Option<Vec<Vec<u8>>> {
        let read = self.entities.read().await;
        if read.is_empty() {
            return None;
        }

        Some(read.values().cloned().collect())
    }

    pub async fn delete(&self, partition_key: &str, row_key: &str) -> bool {
        let mut write = self.entities.write().await;
        write.remove(&make_key(partition_key, row_key)).is_some()
    }
}

fn make_key(partition_key: &str, row_key: &str) -> (String, String) {
    (partition_key.to_string(), row_key.to_string())
}
