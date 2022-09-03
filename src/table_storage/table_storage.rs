use std::sync::Arc;

use crate::{
    sdk_azure::table_storage::TableEntitiesChunk, AzureStorageConnection, AzureStorageError,
};

use super::{TableStorageEntity, TableStorageError};

pub struct TableStorage<TEntity: TableStorageEntity> {
    connection: Arc<AzureStorageConnection>,
    table_name: String,
    _phantom: std::marker::PhantomData<TEntity>,
}

impl<TEntity: TableStorageEntity> TableStorage<TEntity> {
    pub fn new(connection: Arc<AzureStorageConnection>, table_name: String) -> Self {
        TableStorage {
            connection,
            table_name,
            _phantom: std::marker::PhantomData,
        }
    }

    pub async fn get_entity(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Result<Option<TEntity>, TableStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(data) => {
                data.get_table_storage_entity(&self.table_name, partition_key, row_key)
                    .await
            }
            AzureStorageConnection::File(_data) => {
                todo!("Not implemented yet");
            }
            AzureStorageConnection::InMemory(_data) => {
                todo!("Not implemented yet");
            }
        }
    }

    pub async fn get_entities_by_partition_key(
        &self,
        partition_key: &str,
    ) -> Result<Option<TableEntitiesChunk<TEntity>>, AzureStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(data) => {
                let result = data
                    .get_table_storage_entity_by_partition_key(&self.table_name, partition_key)
                    .await;

                Ok(result)
            }
            AzureStorageConnection::File(_data) => {
                todo!("Not implemented yet");
            }
            AzureStorageConnection::InMemory(_data) => {
                todo!("Not implemented yet");
            }
        }
    }

    pub async fn get_all_entities(
        &self,
    ) -> Result<Option<TableEntitiesChunk<TEntity>>, TableStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(data) => {
                data.get_table_storage_all_entities(&self.table_name).await
            }
            AzureStorageConnection::File(_data) => {
                todo!("Not implemented yet");
            }
            AzureStorageConnection::InMemory(_data) => {
                todo!("Not implemented yet");
            }
        }
    }

    pub async fn insert_or_replace_entity(
        &self,
        entity: &TEntity,
    ) -> Result<(), TableStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(data) => {
                data.insert_or_replace_entity(&self.table_name, entity)
                    .await
            }
            AzureStorageConnection::File(_data) => {
                todo!("Not implemented yet");
            }
            AzureStorageConnection::InMemory(_data) => {
                todo!("Not implemented yet");
            }
        }
    }

    pub async fn insert_entity(&self, entity: &TEntity) -> Result<(), TableStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(data) => {
                data.insert_table_entity(&self.table_name, entity).await
            }
            AzureStorageConnection::File(_data) => {
                todo!("Not implemented yet");
            }
            AzureStorageConnection::InMemory(_data) => {
                todo!("Not implemented yet");
            }
        }
    }

    pub async fn delete_entity(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Result<bool, TableStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(data) => {
                data.delete_table_entity(&self.table_name, partition_key, row_key)
                    .await
            }
            AzureStorageConnection::File(_data) => {
                todo!("Not implemented yet");
            }
            AzureStorageConnection::InMemory(_data) => {
                todo!("Not implemented yet");
            }
        }
    }
}
