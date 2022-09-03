use crate::{
    sdk_azure::flurl_ext::FlUrlAzureExtensions, table_storage::TableStorageEntity,
    AzureStorageConnectionData,
};

use super::EntitiesContinuationToken;

pub struct TableEntitiesChunk<'s, TResult: TableStorageEntity> {
    items: Option<Vec<TResult>>,
    continuation_token: Option<EntitiesContinuationToken>,
    connection_data: &'s AzureStorageConnectionData,
    table_name: &'s str,
}

impl<'s, TResult: TableStorageEntity> TableEntitiesChunk<'s, TResult> {
    pub fn new(
        connection_data: &'s AzureStorageConnectionData,
        table_name: &'s str,
        items: Vec<TResult>,
        continuation_token: Option<EntitiesContinuationToken>,
    ) -> Self {
        Self {
            connection_data,
            table_name,
            items: Some(items),
            continuation_token,
        }
    }

    pub async fn get_next(&mut self) -> Option<Vec<TResult>> {
        if let Some(result) = self.items.take() {
            return Some(result);
        }

        return self.load_next_items().await;
    }

    async fn load_next_items(&mut self) -> Option<Vec<TResult>> {
        if let Some(continuation_token) = self.continuation_token.as_ref() {
            let mut raw_ending = String::with_capacity(255);
            raw_ending.push('?');

            if let Some(next_partition_key) = continuation_token.next_partition_key.as_ref() {
                raw_ending.push_str("NextPartitionKey=");
                raw_ending.push_str(next_partition_key.as_str());
            }

            if let Some(next_row_key) = continuation_token.next_row_key.as_ref() {
                if continuation_token.next_partition_key.is_some() {
                    raw_ending.push('&');
                }

                raw_ending.push_str("NextRowKey=");
                raw_ending.push_str(next_row_key.as_str());
            }

            let fl_url = flurl::FlUrl::new(&self.connection_data.table_storage_api_url, None)
                .append_path_segment(self.table_name)
                .append_raw_ending(raw_ending.as_str())
                .add_table_storage_azure_headers(self.connection_data, None);

            let response = fl_url.get().await.unwrap();

            self.continuation_token = EntitiesContinuationToken::new(&response);

            let body = response.receive_body().await.unwrap();

            super::models::read_entities_items(body.as_slice())
        } else {
            None
        }
    }
}
