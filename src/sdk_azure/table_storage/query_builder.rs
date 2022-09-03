use flurl::url_utils;

enum PopulationMode {
    NotInitialized,
    Filter,
    Select,
}

pub struct TableStorageQueryBuilder {
    result: Option<Vec<u8>>,
    mode: PopulationMode,
}

impl TableStorageQueryBuilder {
    pub fn new() -> Self {
        Self {
            result: None,
            mode: PopulationMode::NotInitialized,
        }
    }

    fn get_o_data_mut(&mut self) -> &mut Vec<u8> {
        if self.result.is_none() {
            self.result = Some(Vec::with_capacity(2048));
        }

        let result = self.result.as_mut().unwrap();

        match &mut self.mode {
            PopulationMode::NotInitialized => {
                self.mode = PopulationMode::Filter;
                result.extend_from_slice("?$filter=(".as_bytes());
            }
            PopulationMode::Filter => {
                result.extend_from_slice(")+and+(".as_bytes());
            }
            PopulationMode::Select => {
                panic!("Can not get odate in Select mode");
            }
        }
        result
    }

    fn get_select_mut(&mut self) -> &mut Vec<u8> {
        if self.result.is_none() {
            self.result = Some(Vec::with_capacity(2048));
        }

        let result = self.result.as_mut().unwrap();

        match &mut self.mode {
            PopulationMode::NotInitialized => {
                self.mode = PopulationMode::Select;
                result.extend_from_slice("?$select=".as_bytes());
            }
            PopulationMode::Filter => {
                self.mode = PopulationMode::Select;
                result.extend_from_slice(")&$select=".as_bytes());
            }
            PopulationMode::Select => {
                result.push(b',');
            }
        }
        result
    }

    pub fn add_partition_key(&mut self, partition_key_value: &str) {
        let o_data = self.get_o_data_mut();
        o_data.extend_from_slice("PartitionKey+eq+%27".as_bytes());
        url_utils::encode_to_url_string_and_copy(o_data, partition_key_value);
        o_data.extend_from_slice("%27".as_bytes());
    }

    pub fn add_row_key(&mut self, row_key: &str) {
        let o_data = self.get_o_data_mut();
        o_data.extend_from_slice("RowKey+eq+%27".as_bytes());
        url_utils::encode_to_url_string_and_copy(o_data, row_key);
        o_data.extend_from_slice("%27".as_bytes());
    }

    pub fn add_select_param(&mut self, select_param: &str) {
        let select = self.get_select_mut();
        url_utils::encode_to_url_string_and_copy(select, select_param);
    }

    pub fn get_result(self) -> String {
        match &self.mode {
            PopulationMode::NotInitialized => {
                return "".to_string();
            }
            PopulationMode::Filter => {
                let mut result = self.result.unwrap();
                result.push(b')');
                return String::from_utf8(result).unwrap();
            }
            PopulationMode::Select => {
                return String::from_utf8(self.result.unwrap()).unwrap();
            }
        }
    }
}
