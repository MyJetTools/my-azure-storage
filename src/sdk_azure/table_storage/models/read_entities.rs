use flurl::FlUrlResponse;
use my_json::json_reader::array_parser::JsonArrayIterator;

use crate::{
    sdk_azure::table_storage::{EntitiesContinuationToken, TableEntitiesChunk},
    table_storage::{TableStorageEntity, TableStorageError},
    AzureStorageConnectionData,
};

pub async fn read_entities<'s, TResult: TableStorageEntity>(
    connection_data: &'s AzureStorageConnectionData,
    table_name: &'s str,
    response: FlUrlResponse,
) -> Result<Option<TableEntitiesChunk<'s, TResult>>, TableStorageError> {
    let continuation_token = EntitiesContinuationToken::new(&response);
    let body = response.receive_body().await?;
    let payload_with_value = super::read_value_payload(&body)?;
    match read_entities_items(payload_with_value) {
        Some(entities) => {
            return Ok(Some(TableEntitiesChunk::new(
                connection_data,
                table_name,
                entities,
                continuation_token,
            )));
        }
        None => {
            return Ok(None);
        }
    }
}

pub fn read_entities_items<TResult: TableStorageEntity>(payload: &[u8]) -> Option<Vec<TResult>> {
    let mut items = Vec::with_capacity(1000);

    for itm in JsonArrayIterator::new(payload) {
        let itm = itm.unwrap();
        let json_reader = my_json::json_reader::JsonFirstLineReader::new(itm);
        items.push(TResult::create(json_reader));
    }

    if items.len() > 0 {
        return Some(items);
    }

    None
}
