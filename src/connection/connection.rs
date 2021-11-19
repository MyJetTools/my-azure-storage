use std::collections::HashMap;

use flurl::FlUrl;
use my_telemetry::MyTelemetry;

use super::sign_utils::SignVerb;

#[derive(Clone)]
pub struct AzureConnection<TMyTelemetry: MyTelemetry + Send + Sync + 'static> {
    pub account_name: String,
    pub account_key: Vec<u8>,
    pub endpoint_suffix: String,
    pub default_endpoints_protocol: String,
    pub blobs_api_url: String,
    pub time_out_ms: String,
    pub telemetry: Option<TMyTelemetry>,
}
