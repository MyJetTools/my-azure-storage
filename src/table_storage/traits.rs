use crate::sdk_azure::table_storage::TableStorageQueryBuilder;

pub trait TableStorageEntity {
    fn create(json: my_json::json_reader::JsonFirstLineReader) -> Self;
    fn populate_field_names(bulder: &mut TableStorageQueryBuilder);
    fn serialize(&self) -> Vec<u8>;
}
