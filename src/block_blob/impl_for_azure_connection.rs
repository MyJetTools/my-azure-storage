use async_trait::async_trait;
use my_telemetry::MyTelemetryToConsole;

use crate::{connection::AzureStorageConnection, types::AzureStorageError};

use super::api::BlockBlobApi;

#[async_trait]
impl BlockBlobApi for AzureStorageConnection {
    async fn upload(
        &self,
        container_name: &str,
        blob_name: &str,
        content: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        let connection = self.get_connection_info();
        super::sdk::upload::<MyTelemetryToConsole>(
            connection,
            container_name,
            blob_name,
            content,
            None,
        )
        .await
    }
}
