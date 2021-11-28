use std::sync::Arc;

use crate::AzureConnectionInfo;

pub trait AzureStorageConnection {
    fn get_conneciton_info(&self) -> &AzureConnectionInfo;
    fn get_telemetry<TMyTelemetry, MyTelemetry>(&self) -> Option<Arc<TMyTelemetry>>;
}
