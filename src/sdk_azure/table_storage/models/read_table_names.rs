use my_json::json_reader::array_parser::JsonArrayIterator;

pub fn read_table_names(payload: &[u8]) -> Option<Vec<String>> {
    let mut items = Vec::with_capacity(1000);

    for itm in JsonArrayIterator::new(payload) {
        let json_payload = itm.unwrap();
        items.push(read_table_name(json_payload));
    }

    if items.len() > 0 {
        return Some(items);
    }

    None
}

fn read_table_name(payload: &[u8]) -> String {
    let mut table_name = None;

    for first_line in my_json::json_reader::JsonFirstLineReader::new(payload) {
        let first_line = first_line.unwrap();

        if first_line.get_name().unwrap() == "TableName" {
            table_name = Some(
                first_line
                    .get_value()
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string(),
            );
        }
    }

    if table_name.is_none() {
        panic!("Table name not found");
    }

    table_name.unwrap()
}
