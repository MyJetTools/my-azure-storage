use crate::{
    sdk_azure::flurl_ext::FlUrlAzureExtensions,
    table_storage::{TableStorageEntity, TableStorageError},
    AzureStorageConnectionData,
};

use super::EntitiesContinuationToken;

pub struct TableEntitiesChunk<'s, TResult: TableStorageEntity> {
    items: Option<Vec<TResult>>,
    continuation_token: Option<EntitiesContinuationToken>,
    connection_data: Option<&'s AzureStorageConnectionData>,
    table_name: Option<&'s str>,
}

impl<'s, TResult: TableStorageEntity> TableEntitiesChunk<'s, TResult> {
    pub fn new(
        connection_data: &'s AzureStorageConnectionData,
        table_name: &'s str,
        items: Vec<TResult>,
        continuation_token: Option<EntitiesContinuationToken>,
    ) -> Self {
        Self {
            connection_data: Some(connection_data),
            table_name: Some(table_name),
            items: Some(items),
            continuation_token,
        }
    }

    pub fn from_items(items: Vec<TResult>) -> Self {
        Self {
            items: Some(items),
            continuation_token: None,
            connection_data: None,
            table_name: None,
        }
    }

    pub async fn get_next(
        &mut self,
    ) -> Result<Option<Vec<TResult>>, crate::table_storage::TableStorageError> {
        if let Some(result) = self.items.take() {
            return Ok(Some(result));
        }

        return self.load_next_items().await;
    }

    async fn load_next_items(
        &mut self,
    ) -> Result<Option<Vec<TResult>>, crate::table_storage::TableStorageError> {
        if let (Some(continuation_token), Some(connection_data), Some(table_name)) = (
            self.continuation_token.as_ref(),
            self.connection_data,
            self.table_name,
        ) {
            let mut fl_url = flurl::FlUrl::new(&connection_data.table_storage_api_url, None)
                .append_path_segment(table_name);

            if let Some(next_partition_key) = continuation_token.next_partition_key.as_ref() {
                fl_url = fl_url
                    .append_query_param("NextPartitionKey", Some(next_partition_key.as_str()));
            }

            if let Some(next_row_key) = continuation_token.next_row_key.as_ref() {
                fl_url = fl_url.append_query_param("NextRowKey", Some(next_row_key.as_str()));
            }

            let mut fl_url = fl_url.add_table_storage_azure_headers(connection_data, None);

            let mut response = fl_url.get().await?;

            self.continuation_token = EntitiesContinuationToken::new(&response);

            let body = response.receive_body().await?;

            let payload = super::models::read_value_payload(&body)?;

            Ok(super::models::read_entities_items(payload))
        } else {
            Ok(None)
        }
    }
}
