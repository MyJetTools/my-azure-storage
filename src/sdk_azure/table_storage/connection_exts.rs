use flurl::FlUrl;
use my_json::json_reader::{array_parser::JsonArrayIterator, JsonFirstLineReader};

use crate::{
    sdk_azure::{
        flurl_ext::FlUrlAzureExtensions, table_storage::query_builder::TableStorageQueryBuilder,
    },
    table_storage::TableStorageEntity,
};

use super::{ContinuationToken, TableEntitiesChunk};

impl crate::AzureStorageConnectionData {
    pub async fn get_table_storage_entity<TResult: TableStorageEntity>(
        &self,
        table_name: &str,
        partition_key: &str,
        row_key: &str,
    ) -> Option<TResult> {
        let table_name = format!("{}()", table_name);

        let mut query_builder = TableStorageQueryBuilder::new();

        query_builder.add_partition_key(partition_key);
        query_builder.add_row_key(row_key);
        TResult::populate_field_names(&mut query_builder);

        let raw_ending = query_builder.get_result();

        let fl_url = flurl::FlUrl::new(&self.table_storage_api_url.as_str(), None)
            .append_path_segment(table_name.as_str())
            .append_raw_ending(raw_ending.as_str())
            .add_table_storage_azure_headers(self, None, None);

        let result = fl_url.get().await.unwrap();

        let body = result.receive_body().await.unwrap();

        println!("{}", std::str::from_utf8(body.as_slice()).unwrap());

        for first_line in JsonFirstLineReader::new(body.as_slice()) {
            let first_line = first_line.unwrap();

            if first_line.get_name().unwrap() == "value" {
                for value in first_line.get_value() {
                    let array = value.as_bytes().unwrap();

                    for itm in JsonArrayIterator::new(array) {
                        let itm = itm.unwrap();
                        let json_reader = my_json::json_reader::JsonFirstLineReader::new(itm);
                        return Some(TResult::create(json_reader));
                    }
                }
            }
        }

        None
    }

    pub async fn get_table_storage_all_entities<'s, TResult: TableStorageEntity>(
        &'s self,
        table_name: &'s str,
    ) -> Option<TableEntitiesChunk<TResult>> {
        let table_name_for_request = format!("{}()", table_name);

        let mut query_builder = TableStorageQueryBuilder::new();
        TResult::populate_field_names(&mut query_builder);

        let raw_ending = query_builder.get_result();

        let fl_url = flurl::FlUrl::new(&self.table_storage_api_url.as_str(), None)
            .append_path_segment(table_name_for_request.as_str())
            .append_raw_ending(raw_ending.as_str())
            .add_table_storage_azure_headers(self, None, None);

        let response = fl_url.get().await.unwrap();

        let continuation_token = ContinuationToken::new(&response);

        let body = response.receive_body().await.unwrap();

        let entities = parse_entities(body.as_slice())?;

        return Some(TableEntitiesChunk::new(
            self,
            table_name,
            entities,
            continuation_token,
        ));
    }

    pub async fn get_table_storage_entity_by_partition_key<'s, TResult: TableStorageEntity>(
        &'s self,
        table_name: &'s str,
        partition_key: &str,
    ) -> Option<TableEntitiesChunk<TResult>> {
        let table_name_for_request = format!("{}()", table_name);

        let mut query_builder = TableStorageQueryBuilder::new();

        query_builder.add_partition_key(partition_key);
        TResult::populate_field_names(&mut query_builder);

        let raw_ending = query_builder.get_result();

        let fl_url = flurl::FlUrl::new(&self.table_storage_api_url.as_str(), None)
            .append_path_segment(table_name_for_request.as_str())
            .append_raw_ending(raw_ending.as_str())
            .add_table_storage_azure_headers(self, None, None);

        let response = fl_url.get().await.unwrap();

        let continuation_token = ContinuationToken::new(&response);

        let body = response.receive_body().await.unwrap();

        let entities = parse_entities(body.as_slice())?;

        return Some(TableEntitiesChunk::new(
            self,
            table_name,
            entities,
            continuation_token,
        ));
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

pub fn parse_entities<TResult: TableStorageEntity>(body: &[u8]) -> Option<Vec<TResult>> {
    for first_line in JsonFirstLineReader::new(body) {
        let first_line = first_line.unwrap();

        if first_line.get_name().unwrap() == "value" {
            let mut items = Vec::with_capacity(1000);
            for value in first_line.get_value() {
                let array = value.as_bytes().unwrap();
                for itm in JsonArrayIterator::new(array) {
                    let itm = itm.unwrap();
                    let json_reader = my_json::json_reader::JsonFirstLineReader::new(itm);
                    items.push(TResult::create(json_reader));
                }
            }

            if items.len() > 0 {
                return Some(items);
            }
        }
    }

    None
}
