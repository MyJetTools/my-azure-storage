use std::sync::Arc;

use crate::{
    connection::in_mem,
    sdk_azure::table_storage::{TableEntitiesChunk, TableNamesChunk},
    AzureStorageConnection,
};
use my_json::json_reader::JsonFirstLineReader;

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

    pub async fn create_table(&self) -> Result<(), TableStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(data) => data.create_table(&self.table_name).await,
            AzureStorageConnection::File(_data) => {
                crate::sdk_files::table_storage::create_table(_data, &self.table_name).await
            }
            AzureStorageConnection::InMemory(data) => {
                data.create_table(self.table_name.as_str()).await
            }
        }
    }

    pub async fn create_table_if_not_exists(&self) -> Result<(), TableStorageError> {
        match self.create_table().await {
            Ok(_) => Ok(()),
            Err(err) => match err {
                TableStorageError::TableAlreadyExists => Ok(()),
                _ => Err(err),
            },
        }
    }

    pub async fn get_table_list(&self) -> Result<Option<TableNamesChunk>, TableStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(data) => data.get_list_of_tables().await,
            AzureStorageConnection::File(_data) => {
                let tables = crate::sdk_files::table_storage::list_tables(_data).await?;
                Ok(tables.map(TableNamesChunk::from_list))
            }
            AzureStorageConnection::InMemory(data) => {
                let tables = data.get_table_list().await;
                if tables.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(TableNamesChunk::from_list(tables)))
                }
            }
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
            AzureStorageConnection::File(_data) => crate::sdk_files::table_storage::get_entity(
                _data,
                &self.table_name,
                partition_key,
                row_key,
            ),
            AzureStorageConnection::InMemory(data) => {
                let table = in_mem::operations::get_table(data, self.table_name.as_str()).await?;
                if let Some(payload) = table.get_entity(partition_key, row_key).await {
                    Ok(Some(TEntity::create(JsonFirstLineReader::new(
                        payload.as_slice(),
                    ))))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub async fn get_entities_by_partition_key(
        &self,
        partition_key: &str,
    ) -> Result<Option<TableEntitiesChunk<TEntity>>, TableStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(data) => {
                data.get_table_storage_entity_by_partition_key(&self.table_name, partition_key)
                    .await
            }
            AzureStorageConnection::File(_data) => {
                match crate::sdk_files::table_storage::get_by_partition::<_, TEntity>(
                    _data,
                    &self.table_name,
                    partition_key,
                )
                .await?
                {
                    Some(items) => Ok(Some(TableEntitiesChunk::from_items(items))),
                    None => Ok(None),
                }
            }
            AzureStorageConnection::InMemory(data) => {
                let table = in_mem::operations::get_table(data, self.table_name.as_str()).await?;
                match table.get_by_partition(partition_key).await {
                    Some(payloads) => {
                        let entities = payloads
                            .into_iter()
                            .map(|p| TEntity::create(JsonFirstLineReader::new(p.as_slice())))
                            .collect();

                        Ok(Some(TableEntitiesChunk::from_items(entities)))
                    }
                    None => Ok(None),
                }
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
                match crate::sdk_files::table_storage::get_all_entities::<_, TEntity>(
                    _data,
                    &self.table_name,
                )
                .await?
                {
                    Some(items) => Ok(Some(TableEntitiesChunk::from_items(items))),
                    None => Ok(None),
                }
            }
            AzureStorageConnection::InMemory(data) => {
                let table = in_mem::operations::get_table(data, self.table_name.as_str()).await?;

                match table.get_all().await {
                    Some(payloads) => {
                        let entities = payloads
                            .into_iter()
                            .map(|p| TEntity::create(JsonFirstLineReader::new(p.as_slice())))
                            .collect();

                        Ok(Some(TableEntitiesChunk::from_items(entities)))
                    }
                    None => Ok(None),
                }
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
                crate::sdk_files::table_storage::insert_or_replace(_data, &self.table_name, entity)
                    .await
            }
            AzureStorageConnection::InMemory(data) => {
                let table = in_mem::operations::get_table(data, self.table_name.as_str()).await?;

                let payload = entity.serialize();

                table
                    .insert_or_replace(entity.get_partition_key(), entity.get_row_key(), payload)
                    .await;

                Ok(())
            }
        }
    }

    pub async fn insert_entity(&self, entity: &TEntity) -> Result<(), TableStorageError> {
        match self.connection.as_ref() {
            AzureStorageConnection::AzureStorage(data) => {
                data.insert_table_entity(&self.table_name, entity).await
            }
            AzureStorageConnection::File(_data) => {
                crate::sdk_files::table_storage::insert(_data, &self.table_name, entity).await
            }
            AzureStorageConnection::InMemory(data) => {
                let table = in_mem::operations::get_table(data, self.table_name.as_str()).await?;

                let payload = entity.serialize();

                table
                    .insert(entity.get_partition_key(), entity.get_row_key(), payload)
                    .await
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
            AzureStorageConnection::File(_data) => crate::sdk_files::table_storage::delete_entity(
                _data,
                &self.table_name,
                partition_key,
                row_key,
            ),
            AzureStorageConnection::InMemory(data) => {
                let table = in_mem::operations::get_table(data, self.table_name.as_str()).await?;

                Ok(table.delete(partition_key, row_key).await)
            }
        }
    }
}
