use crate::{
    sdk_azure::flurl_ext::FlUrlAzureExtensions, table_storage::TableStorageError,
    AzureStorageConnectionData,
};

pub struct TableNamesChunk<'s> {
    items: Option<Result<Option<Vec<String>>, TableStorageError>>,
    continuation_token: Option<String>,
    connection_data: Option<&'s AzureStorageConnectionData>,
}

impl<'s> TableNamesChunk<'s> {
    pub fn new(
        connection_data: &'s AzureStorageConnectionData,
        items: Result<Option<Vec<String>>, TableStorageError>,
        continuation_token: Option<String>,
    ) -> Self {
        Self {
            connection_data: Some(connection_data),

            items: Some(items),
            continuation_token,
        }
    }

    pub fn from_list(items: Vec<String>) -> Self {
        Self {
            items: Some(Ok(Some(items))),
            continuation_token: None,
            connection_data: None,
        }
    }

    pub async fn get_next(&mut self) -> Result<Option<Vec<String>>, TableStorageError> {
        if let Some(result) = self.items.take() {
            return result;
        }

        return self.load_next_items().await;
    }

    async fn load_next_items(&mut self) -> Result<Option<Vec<String>>, TableStorageError> {
        if let (Some(continuation_token), Some(connection_data)) =
            (self.continuation_token.as_ref(), self.connection_data)
        {
            let fl_url = flurl::FlUrl::new(&connection_data.table_storage_api_url, None)
                .append_path_segment("Tables")
                .append_query_param("NextTableName", Some(continuation_token.as_str()))
                .add_table_storage_azure_headers(connection_data, None);

            let mut response = fl_url.get().await?;

            self.continuation_token = if let Some(token) = response
                .get_headers()
                .get("x-ms-continuation-nexttablename")
            {
                Some(token.to_string())
            } else {
                None
            };

            let body = response.receive_body().await?;

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
