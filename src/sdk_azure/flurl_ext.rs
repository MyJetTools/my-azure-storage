use crate::connection::AzureStorageConnectionData;

use super::sign_utils::SignVerb;
use chrono::Utc;
use flurl::FlUrl;

pub trait FlUrlAzureExtensions {
    fn add_azure_headers(
        self,
        verb: SignVerb,
        connection: &AzureStorageConnectionData,
        content_len: Option<usize>,
        next_marker: Option<String>,
        azure_rest_version: &str,
    ) -> Self;

    #[cfg(feature = "table-storage")]
    fn add_table_storage_azure_headers(
        self,
        connection: &AzureStorageConnectionData,
        content_len: Option<usize>,
    ) -> Self;
}

impl FlUrlAzureExtensions for FlUrl {
    fn add_azure_headers(
        mut self,
        verb: SignVerb,
        connection: &AzureStorageConnectionData,
        content_len: Option<usize>,
        next_marker: Option<String>,
        azure_rest_version: &str,
    ) -> Self {
        let now = Utc::now();

        let date = now.to_rfc2822().replace("+0000", "GMT");

        self = match content_len {
            Some(size) => self.with_header("Content-Length", size.to_string()),
            None => self.with_header("Content-Length", "0"),
        };

        let mut flurl = self
            .append_query_param("timeout", Some(connection.time_out_as_string.as_str()))
            .append_query_param("maxresults", Some("100"))
            .with_header("x-ms-date", date.as_str())
            .with_header("x-ms-version", azure_rest_version);

        if let Some(next_marker) = next_marker {
            flurl = flurl.append_query_param("marker", Some(next_marker));
        }

        let auth_key = connection.get_auth_header(verb, content_len, &flurl);

        flurl.with_header("Authorization", auth_key)
    }

    #[cfg(feature = "table-storage")]
    fn add_table_storage_azure_headers(
        mut self,
        connection: &AzureStorageConnectionData,
        content_len: Option<usize>,
    ) -> Self {
        let now = Utc::now();

        let date = now.to_rfc2822().replace("+0000", "GMT");

        self = match content_len {
            Some(size) => self.with_header_val_string("Content-Length", size.to_string()),
            None => self.with_header("Content-Length", "0"),
        };

        let fl_url = self
            .with_header("x-ms-date", date.as_str())
            .with_header("x-ms-version", "2015-12-11")
            .with_header("Content-Type", "application/json")
            .with_header("Accept", "application/json;odata=nometadata");

        let auth_key = connection.get_table_storage_auth_header(&date, &fl_url);

        fl_url.with_header_val_string("Authorization", auth_key)
    }
}
