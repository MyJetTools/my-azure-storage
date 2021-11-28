use super::AzureStorageConnectionInfo;

pub struct AzureStorageConnection {
    info: AzureStorageConnectionInfo,
}

impl AzureStorageConnection {
    pub fn from_conn_string(conn_string: &str) -> Self {
        Self {
            info: AzureStorageConnectionInfo::from_conn_string(conn_string),
        }
    }

    pub fn get_connection_info(&self) -> &AzureStorageConnectionInfo {
        &self.info
    }
}
