use flurl::{FlUrl, FlUrlTelemetry};

use crate::sdk_azure::consts::DEPENDENCY_TYPE;

use super::AzureStorageConnectionData;

impl Into<FlUrl> for &AzureStorageConnectionData {
    fn into(self) -> FlUrl {
        let mut fl_url = FlUrl::new_with_timeout(self.blobs_api_url.as_str(), self.time_out);

        if let Some(telemetry) = &self.telemetry {
            fl_url.telemetry = FlUrlTelemetry {
                telemetry: telemetry.clone(),
                dependency_type: DEPENDENCY_TYPE.to_string(),
            }
            .into()
        }

        return fl_url;
    }
}
