use async_trait::async_trait;
use my_telemetry::MyTelemetry;
use rust_extensions::StopWatch;

use crate::{
    connection::{AzureConnectionWithTelemetry, GetAzureConnectionInfo},
    types::AzureStorageError,
};

use super::api::BlockBlobApi;

#[async_trait]
impl<TMyTelemetry: MyTelemetry + Send + Sync + 'static> BlockBlobApi
    for AzureConnectionWithTelemetry<TMyTelemetry>
{
    async fn upload(
        &self,
        container_name: &str,
        blob_name: &str,
        content: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();

        let mut sw = StopWatch::new();
        sw.start();
        let result = super::sdk::upload(connection, container_name, blob_name, content).await;

        let success = result.is_ok();

        self.track_dependency_duration("block_blob/upload".to_string(), success, sw.duration());

        return result;
    }
}
