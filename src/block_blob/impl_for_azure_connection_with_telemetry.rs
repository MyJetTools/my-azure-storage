use async_trait::async_trait;
use my_telemetry::MyTelemetry;

use crate::{connection::AzureStorageConnectionWithTelemetry, types::AzureStorageError};

use super::api::BlockBlobApi;

#[async_trait]
impl<TMyTelemetry: MyTelemetry + Send + Sync + 'static> BlockBlobApi
    for AzureStorageConnectionWithTelemetry<TMyTelemetry>
{
    async fn upload(
        &self,
        container_name: &str,
        blob_name: &str,
        content: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();

        let result = super::sdk::upload(
            connection,
            container_name,
            blob_name,
            content,
            self.telemetry.clone(),
        )
        .await;

        return result;
    }
}
