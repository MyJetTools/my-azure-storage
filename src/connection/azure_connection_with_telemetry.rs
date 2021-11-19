use std::sync::Arc;

use my_telemetry::MyTelemetry;

use super::{azure_connection_trait::GetAzureConnectionInfo, AzureConnectionInfo};

pub struct AzureConnectionWithTelemetry<TMyTelemetry: MyTelemetry + Send + Sync + 'static> {
    info: AzureConnectionInfo,
    pub telemetry: Option<Arc<TMyTelemetry>>,
}

impl<TMyTelemetry: MyTelemetry + Send + Sync + 'static> AzureConnectionWithTelemetry<TMyTelemetry> {
    pub fn from_conn_string(conn_string: &str, telemetry: Option<Arc<TMyTelemetry>>) -> Self {
        Self {
            info: AzureConnectionInfo::from_conn_string(conn_string),
            telemetry,
        }
    }
}

impl<'s, TMyTelemetry: MyTelemetry + Send + Sync + 'static> GetAzureConnectionInfo<'s>
    for AzureConnectionWithTelemetry<TMyTelemetry>
{
    fn get_connection_info(&'s self) -> &'s AzureConnectionInfo {
        return &self.info;
    }
}
