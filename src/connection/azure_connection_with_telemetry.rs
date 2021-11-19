use std::time::Duration;

use my_telemetry::MyTelemetry;

use super::{azure_connection_trait::GetAzureConnectionInfo, AzureConnectionInfo};

pub struct AzureConnectionWithTelemetry<TMyTelemetry: MyTelemetry + Send + Sync + 'static> {
    info: AzureConnectionInfo,
    pub telemetry: Option<TMyTelemetry>,
}

impl<TMyTelemetry: MyTelemetry + Send + Sync + 'static> AzureConnectionWithTelemetry<TMyTelemetry> {
    pub fn from_conn_string(conn_string: &str, telemetry: Option<TMyTelemetry>) -> Self {
        Self {
            info: AzureConnectionInfo::from_conn_string(conn_string),
            telemetry,
        }
    }

    pub fn track_dependency_duration(&self, resource: String, success: bool, duration: Duration) {
        if let Some(telemetry) = &self.telemetry {
            telemetry.track_dependency_duration(
                self.info.blobs_api_url.to_string(),
                "HTTP".to_string(),
                resource,
                success,
                duration,
            );
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
