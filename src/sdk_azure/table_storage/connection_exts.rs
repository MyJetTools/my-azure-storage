use flurl::FlUrl;
use my_json::json_reader::JsonFirstLineReader;

use crate::{
    sdk_azure::{
        flurl_ext::FlUrlAzureExtensions, table_storage::query_builder::TableStorageQueryBuilder,
    },
    table_storage::{TableStorageEntity, TableStorageError},
};

use super::{TableEntitiesChunk, TableNamesChunk};

impl crate::AzureStorageConnectionData {
    pub async fn get_list_of_tables(&self) -> Result<Option<TableNamesChunk>, TableStorageError> {
        let response = flurl::FlUrl::new(&self.table_storage_api_url.as_str(), None)
            .append_path_segment("Tables")
            .add_table_storage_azure_headers(self, None)
            .get()
            .await?;

        let status_code = response.get_status_code();

        if status_code == 200 {
            let continuation_token = if let Some(token) = response
                .get_headers()
                .get("x-ms-continuation-nexttablename")
            {
                Some(token.to_string())
            } else {
                None
            };

            let payload = response.receive_body().await?;
            let payload = super::models::read_value_payload(payload.as_slice())?;

            let result = super::models::read_table_names(payload);

            if result.is_some() {
                return Ok(Some(TableNamesChunk::new(
                    self,
                    Ok(result),
                    continuation_token,
                )));
            } else {
                return Ok(None);
            }
        } else {
            let payload = response.receive_body().await?;
            return Err(super::models::read_error(payload));
        }
    }

    pub async fn get_table_storage_entity<TResult: TableStorageEntity>(
        &self,
        table_name: &str,
        partition_key: &str,
        row_key: &str,
    ) -> Result<Option<TResult>, TableStorageError> {
        let table_name = format!(
            "{}(PartitionKey='{}',RowKey='{}')",
            table_name, partition_key, row_key
        );

        let mut query_builder = TableStorageQueryBuilder::new();
        TResult::populate_field_names(&mut query_builder);

        let raw_ending = query_builder.get_result();

        let fl_url = flurl::FlUrl::new(&self.table_storage_api_url.as_str(), None)
            .append_path_segment(table_name.as_str())
            .append_raw_ending(raw_ending.as_str())
            .add_table_storage_azure_headers(self, None);

        let result = fl_url.get().await.unwrap();

        let status_code = result.get_status_code();

        let body = result.receive_body().await.unwrap();

        if status_code == 200 {
            return Ok(Some(TResult::create(JsonFirstLineReader::new(
                body.as_slice(),
            ))));
        }

        let err = super::models::read_error(body);
        match err {
            TableStorageError::ResourceNotFound => {
                return Ok(None);
            }
            _ => {
                return Err(err);
            }
        }
    }

    pub async fn get_table_storage_all_entities<'s, TResult: TableStorageEntity>(
        &'s self,
        table_name: &'s str,
    ) -> Result<Option<TableEntitiesChunk<TResult>>, TableStorageError> {
        let table_name_for_request = format!("{}()", table_name);

        let mut query_builder = TableStorageQueryBuilder::new();
        TResult::populate_field_names(&mut query_builder);

        let raw_ending = query_builder.get_result();

        let fl_url = flurl::FlUrl::new(&self.table_storage_api_url.as_str(), None)
            .append_path_segment(table_name_for_request.as_str())
            .append_raw_ending(raw_ending.as_str())
            .add_table_storage_azure_headers(self, None);

        let response = fl_url.get().await?;

        let status_code = response.get_status_code();

        if status_code == 200 {
            return super::models::read_entities(self, table_name, response).await;
        }

        let body = response.receive_body().await?;

        let err = super::models::read_error(body);

        Err(err)
    }

    pub async fn get_table_storage_entity_by_partition_key<'s, TResult: TableStorageEntity>(
        &'s self,
        table_name: &'s str,
        partition_key: &str,
    ) -> Result<Option<TableEntitiesChunk<TResult>>, TableStorageError> {
        let table_name_for_request = format!("{}()", table_name);

        let mut query_builder = TableStorageQueryBuilder::new();

        query_builder.add_partition_key(partition_key);
        TResult::populate_field_names(&mut query_builder);

        let raw_ending = query_builder.get_result();

        let fl_url = flurl::FlUrl::new(&self.table_storage_api_url.as_str(), None)
            .append_path_segment(table_name_for_request.as_str())
            .append_raw_ending(raw_ending.as_str())
            .add_table_storage_azure_headers(self, None);

        let response = fl_url.get().await?;

        let status_code = response.get_status_code();

        if status_code == 200 {
            return super::models::read_entities(self, table_name, response).await;
        }

        let body = response.receive_body().await?;

        let err = super::models::read_error(body);

        Err(err)
    }

    pub async fn insert_or_replace_entity<TEntity: TableStorageEntity>(
        &self,
        table_name: &str,
        entity: &TEntity,
    ) -> Result<(), TableStorageError> {
        let table_name_for_request = format!(
            "{}(PartitionKey='{}',RowKey='{}')",
            table_name,
            entity.get_partition_key(),
            entity.get_row_key()
        );

        let body = entity.serialize();

        let response = flurl::FlUrl::new(&self.table_storage_api_url.as_str(), None)
            .append_path_segment(table_name_for_request.as_str())
            .add_table_storage_azure_headers(self, Some(body.len()))
            .put(Some(body))
            .await?;

        let status_code = response.get_status_code();

        if status_code == 204 {
            return Ok(());
        }

        let body = response.receive_body().await?;

        let err = super::models::read_error(body);
        Err(err)
    }

    pub async fn insert_table_entity<TEntity: TableStorageEntity>(
        &self,
        table_name: &str,
        entity: &TEntity,
    ) -> Result<(), TableStorageError> {
        let table_name_for_request = format!("{}()", table_name);

        let body = entity.serialize();

        let response = flurl::FlUrl::new(&self.table_storage_api_url.as_str(), None)
            .append_path_segment(table_name_for_request.as_str())
            .with_header("Content-Type", "application/json")
            .add_table_storage_azure_headers(self, Some(body.len()))
            .post(Some(body))
            .await?;

        let status_code = response.get_status_code();

        if status_code == 200 && status_code < 210 {
            return Ok(());
        }

        let body = response.receive_body().await?;

        let err = super::models::read_error(body);
        Err(err)
    }

    pub async fn delete_table_entity(
        &self,
        table_name: &str,
        partition_key: &str,
        row_key: &str,
    ) -> Result<bool, TableStorageError> {
        let table_name_for_request = format!(
            "{}(PartitionKey='{}',RowKey='{}')",
            table_name, partition_key, row_key
        );

        let response = flurl::FlUrl::new(&self.table_storage_api_url.as_str(), None)
            .append_path_segment(table_name_for_request.as_str())
            .with_header("If-Match", "*")
            .add_table_storage_azure_headers(self, None)
            .delete()
            .await?;

        let status_code = response.get_status_code();

        println!("Status code: {}", status_code);

        if status_code == 200 && status_code < 299 {
            return Ok(true);
        }

        let body = response.receive_body().await?;

        let err = super::models::read_error(body);
        match err {
            TableStorageError::ResourceNotFound => Ok(false),
            _ => Err(err),
        }
    }

    pub fn get_table_storage_auth_header(&self, date: &str, flurl: &FlUrl) -> String {
        let canonicalized_resourse = crate::sdk_azure::sign_utils::get_canonicalized_resourse(
            flurl,
            self.account_name.as_str(),
        );

        let string_to_sign = format!("{}\n{}", date, canonicalized_resourse);

        let signature = crate::sdk_azure::sign_utils::sign_transaction(
            string_to_sign.as_str(),
            &self.account_key,
        );
        format!("SharedKeyLite {}:{}", &self.account_name, signature)
    }
}
