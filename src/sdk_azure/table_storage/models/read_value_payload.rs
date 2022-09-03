use my_json::json_reader::JsonFirstLineReader;

use crate::table_storage::TableStorageError;

pub fn read_value_payload(body: &[u8]) -> Result<&[u8], TableStorageError> {
    for first_line in JsonFirstLineReader::new(body) {
        let first_line = first_line.unwrap();

        match first_line.get_name().unwrap() {
            "value" => {
                return Ok(first_line.get_value().unwrap().as_bytes().unwrap());
            }
            "odata.error" => {
                let err = super::read_error_type(
                    body,
                    first_line.get_value().unwrap().as_bytes().unwrap(),
                );
                return Err(err);
            }
            _ => {
                return Err(TableStorageError::Unknown(
                    String::from_utf8(body.to_vec()).unwrap(),
                ));
            }
        }
    }

    Err(TableStorageError::Unknown(
        String::from_utf8(body.to_vec()).unwrap(),
    ))
}
