use my_json::json_reader::JsonFirstLineReader;

use crate::table_storage::TableStorageError;

pub fn read_error_type(whole_payload: &[u8], value_payload: &[u8]) -> TableStorageError {
    for first_line in JsonFirstLineReader::new(value_payload) {
        let first_line = first_line.unwrap();

        if first_line.get_name().unwrap() == "code" {
            match first_line.get_value().unwrap().as_str().unwrap() {
                "TableNotFound" => {
                    return TableStorageError::TableNotFound;
                }
                "EntityAlreadyExists" => {
                    return TableStorageError::EntityAlreadyExists;
                }
                "ResourceNotFound" => {
                    return TableStorageError::ResourceNotFound;
                }
                "TableAlreadyExists" => {
                    return TableStorageError::TableAlreadyExists;
                }
                _ => {
                    return TableStorageError::Unknown(
                        String::from_utf8(whole_payload.to_vec()).unwrap(),
                    );
                }
            }
        }
    }

    return TableStorageError::Unknown(String::from_utf8(whole_payload.to_vec()).unwrap());
}
