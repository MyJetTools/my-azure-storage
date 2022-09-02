use std::sync::Arc;

use crate::{AzureStorageConnection, AzureStorageError};

use super::TableStorageEntity;

pub struct TableStorage {
    connection: Arc<AzureStorageConnection>,
    table_name: String,
}

impl TableStorage {
    pub fn new(connection: Arc<AzureStorageConnection>, table_name: String) -> TableStorage {
        TableStorage {
            connection,
            table_name,
        }
    }

    pub async fn get_entity<T: TableStorageEntity>(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Result<Option<T>, AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(data) => {
                let result = data
                    .get_table_storage_entity(&self.table_name, partition_key, row_key)
                    .await;

                Ok(result)
            }
            AzureStorageConnection::File(data) => {
                todo!("Not implemented yet");
            }
            AzureStorageConnection::InMemory(data) => {
                todo!("Not implemented yet");
            }
        }
    }
}
