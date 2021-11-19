use super::{azure_connection_trait::GetAzureConnectionInfo, AzureConnectionInfo};

pub struct AzureConnection {
    info: AzureConnectionInfo,
}

impl AzureConnection {
    pub fn from_conn_string(conn_string: &str) -> Self {
        Self {
            info: AzureConnectionInfo::from_conn_string(conn_string),
        }
    }
}

impl<'s> GetAzureConnectionInfo<'s> for AzureConnection {
    fn get_connection_info(&'s self) -> &'s AzureConnectionInfo {
        return &self.info;
    }
}
