use flurl::{FlUrl, FlUrlTelemetry};

use crate::consts::DEPENDENCY_TYPE;

use super::AzureStorageConnectionData;

impl Into<FlUrl> for &AzureStorageConnectionData {
    fn into(self) -> FlUrl {
        let mut fl_url = FlUrl::new(self.blobs_api_url.as_str());

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
