use super::{connection::AzureConnection, sign_utils::SignVerb};
use chrono::Utc;
use flurl::FlUrl;

pub trait FlUrlAzureExtensions {
    fn add_azure_headers(
        self,
        verb: SignVerb,
        connection: &AzureConnection,
        next_marker: Option<String>,
        azure_rest_version: &str,
    ) -> Self;
}

impl FlUrlAzureExtensions for FlUrl {
    fn add_azure_headers(
        self,
        verb: SignVerb,
        connection: &AzureConnection,
        next_marker: Option<String>,
        azure_rest_version: &str,
    ) -> Self {
        let now = Utc::now();

        let date = now.to_rfc2822().replace("+0000", "GMT");

        let mut flurl = self
            .append_query_param("timeout", connection.time_out_ms.as_str())
            .append_query_param("maxresults", "100")
            .with_header("x-ms-date", date.as_str())
            .with_header("x-ms-version", azure_rest_version);

        if let Some(next_marker) = next_marker {
            flurl = flurl.append_query_param_string("marker", next_marker);
        }

        let auth_key = connection.get_auth_header(verb, &flurl);

        flurl.with_header_val_string("Authorization", auth_key)
    }
}
