use crate::{
    sdk_azure::flurl_ext::FlUrlAzureExtensions, table_storage::TableStorageError,
    AzureStorageConnectionData,
};

pub struct TableNamesChunk<'s> {
    items: Option<Result<Option<Vec<String>>, TableStorageError>>,
    continuation_token: Option<String>,
    connection_data: &'s AzureStorageConnectionData,
}

impl<'s> TableNamesChunk<'s> {
    pub fn new(
        connection_data: &'s AzureStorageConnectionData,
        items: Result<Option<Vec<String>>, TableStorageError>,
        continuation_token: Option<String>,
    ) -> Self {
        Self {
            connection_data,

            items: Some(items),
            continuation_token,
        }
    }

    pub async fn get_next(&mut self) -> Result<Option<Vec<String>>, TableStorageError> {
        if let Some(result) = self.items.take() {
            return result;
        }

        return self.load_next_items().await;
    }

    async fn load_next_items(&mut self) -> Result<Option<Vec<String>>, TableStorageError> {
        if let Some(continuation_token) = self.continuation_token.as_ref() {
            let next_table_name = format!("?NextTableName={}", continuation_token);

            let fl_url = flurl::FlUrl::new(&self.connection_data.table_storage_api_url, None)
                .append_path_segment("Tables")
                .append_raw_ending(next_table_name.as_str())
                .add_table_storage_azure_headers(self.connection_data, None);

            let response = fl_url.get().await.unwrap();

            self.continuation_token = if let Some(token) = response
                .get_headers()
                .get("x-ms-continuation-nexttablename")
            {
                Some(token.to_string())
            } else {
                None
            };

            let body = response.receive_body().await.unwrap();

            let payload = super::models::read_value_payload(body.as_slice())?;

            match super::models::read_table_names(payload) {
                Some(result) => {
                    return Ok(Some(result));
                }
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}
