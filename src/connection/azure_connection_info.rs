use std::collections::HashMap;

use flurl::FlUrl;

use crate::sign_utils::{self, SignVerb};

pub struct AzureConnectionInfo {
    pub account_name: String,
    pub account_key: Vec<u8>,
    pub endpoint_suffix: String,
    pub default_endpoints_protocol: String,
    pub blobs_api_url: String,
    pub time_out_ms: String,
}

impl AzureConnectionInfo {
    pub fn from_conn_string(conn_string: &str) -> Self {
        let key_values = conn_string.split(";");

        let mut conn_keys: HashMap<&str, &str> = HashMap::new();

        for kv in key_values {
            let eq_index = kv.find("=").unwrap();

            let key = &kv[..eq_index];
            let value = &kv[eq_index + 1..];

            conn_keys.insert(key, value);
        }

        let account_name = conn_keys.get("AccountName").unwrap().to_string();

        let account_key = conn_keys.get("AccountKey").unwrap();

        let account_key = base64::decode(account_key).unwrap();

        let endpoint_suffix = conn_keys.get("EndpointSuffix").unwrap().to_string();

        let default_endpoints_protocol = conn_keys
            .get("DefaultEndpointsProtocol")
            .unwrap()
            .to_string();

        let blobs_api_url = format!(
            "{}://{}.blob.{}",
            default_endpoints_protocol, account_name, endpoint_suffix
        );

        Self {
            account_name,
            account_key: account_key,
            endpoint_suffix: conn_keys.get("EndpointSuffix").unwrap().to_string(),
            default_endpoints_protocol,
            blobs_api_url,
            time_out_ms: "60".to_string(),
        }
    }

    pub fn get_auth_header(
        &self,
        verb: SignVerb,
        content_len: Option<usize>,
        flurl: &FlUrl,
    ) -> String {
        let content_len = match content_len {
            Some(len) => len.to_string(),
            None => "".to_string(),
        };

        let string_to_sign = sign_utils::get_auth_header(
            self.account_name.as_str(),
            content_len.as_str(),
            verb,
            &flurl,
        );

        let signature = sign_utils::sign_transaction(string_to_sign.as_str(), &self.account_key);
        format!("SharedKey {}:{}", &self.account_name, signature)
    }
}
