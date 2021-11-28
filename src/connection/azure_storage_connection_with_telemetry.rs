use std::sync::Arc;

use my_telemetry::MyTelemetry;

use super::AzureStorageConnectionInfo;

pub struct AzureStorageConnectionWithTelemetry<TMyTelemetry: MyTelemetry + Send + Sync + 'static> {
    info: AzureStorageConnectionInfo,
    pub telemetry: Option<Arc<TMyTelemetry>>,
}

impl<TMyTelemetry: MyTelemetry + Send + Sync + 'static>
    AzureStorageConnectionWithTelemetry<TMyTelemetry>
{
    pub fn from_conn_string(conn_string: &str, telemetry: Option<Arc<TMyTelemetry>>) -> Self {
        Self {
            info: AzureStorageConnectionInfo::from_conn_string(conn_string),
            telemetry,
        }
    }

    pub fn get_telemetry(&self) -> Option<Arc<TMyTelemetry>> {
        return self.telemetry.clone();
    }

    pub fn get_connection_info(&self) -> &AzureStorageConnectionInfo {
        &self.info
    }
}
