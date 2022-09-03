use my_json::json_reader::JsonFirstLineReader;

use crate::table_storage::TableStorageError;

pub fn read_error(body: Vec<u8>) -> TableStorageError {
    for first_line in JsonFirstLineReader::new(body.as_slice()) {
        let first_line = first_line.unwrap();

        match first_line.get_name().unwrap() {
            "odata.error" => {
                return super::read_error_type(
                    body.as_slice(),
                    first_line.get_value().unwrap().as_bytes().unwrap(),
                );
            }
            _ => {
                return TableStorageError::Unknown(String::from_utf8(body.to_vec()).unwrap());
            }
        }
    }
    return TableStorageError::Unknown(String::from_utf8(body).unwrap());
}
