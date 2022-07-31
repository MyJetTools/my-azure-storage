use super::{in_mem::MemStorageData, AzureStorageConnectionData, FileConnectionData};

pub enum AzureStorageConnection {
    AzureStorage(AzureStorageConnectionData),
    File(FileConnectionData),
    InMemory(MemStorageData),
}

impl AzureStorageConnection {
    pub fn new_in_memory() -> Self {
        Self::InMemory(MemStorageData::new())
    }
    pub fn from_conn_string(connection_string: &str) -> Self {
        if connection_string.contains("DefaultEndpointsProtocol")
            && connection_string.contains("AccountName")
            && connection_string.contains("AccountKey")
        {
            let connection_data = AzureStorageConnectionData::from_conn_string(connection_string);
            return Self::AzureStorage(connection_data);
        }

        if connection_string.starts_with("~")
            || connection_string.starts_with("/")
            || connection_string.starts_with(".")
        {
            let connection_string = if connection_string.starts_with("~") {
                format!("{}{}", env!("HOME"), &connection_string[1..])
            } else {
                connection_string.to_string()
            };

            let connection_data = FileConnectionData::new(connection_string);

            return Self::File(connection_data);
        }

        panic!("Can not parse connection string: {}", connection_string);
    }

    pub fn is_file(&self) -> bool {
        match self {
            AzureStorageConnection::File(_) => true,
            _ => false,
        }
    }
}
