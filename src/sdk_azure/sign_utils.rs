use std::collections::BTreeMap;

use hmac::{Hmac, Mac};
use rust_extensions::StringBuilder;
use sha2::Sha256;

use flurl::FlUrl;

type HmacSha256 = Hmac<Sha256>;

pub enum SignVerb {
    GET,
    POST,
    PUT,
    DELETE,
    HEAD,
}

impl SignVerb {
    pub fn to_string(&self) -> &str {
        match self {
            SignVerb::GET => "GET",
            SignVerb::POST => "POST",
            SignVerb::PUT => "PUT",
            SignVerb::DELETE => "DELETE",
            SignVerb::HEAD => "HEAD",
        }
    }
}

pub fn get_auth_header(
    storage_account_name: &str,
    content_len: &str,
    verb: SignVerb,
    flurl: &FlUrl,
) -> String {
    let canonicalized_headers = get_canonicalized_headers(flurl);
    let canonicalized_resourse = get_canonicalized_resourse(flurl, storage_account_name);

    let content_md5 = "";

    let if_match = "";

    let result = format!(
        "{}\n\n\n{}\n{}\n\n\n\n{}\n\n\n\n{}{}",
        verb.to_string(),
        content_len,
        content_md5,
        if_match,
        canonicalized_headers,
        canonicalized_resourse
    );

    result
}

pub fn get_canonicalized_resourse(flurl: &FlUrl, account_name: &str) -> String {
    let mut sb = StringBuilder::new();

    sb.append_char('/');

    sb.append_str(account_name);

    sb.append_str(flurl.url.get_path().as_str());

    let mut sorted_query: BTreeMap<&str, &str> = BTreeMap::new();

    for (key, value) in &flurl.url.query {
        sorted_query.insert(key, value.as_ref().unwrap());
    }

    for (key, value) in sorted_query {
        sb.append_char('\n');
        sb.append_str(key.to_lowercase().as_str());
        sb.append_char(':');
        sb.append_str(value);
    }

    return sb.to_string_utf8().unwrap();
}

fn get_canonicalized_headers(flurl: &FlUrl) -> String {
    let mut sorted: BTreeMap<String, &str> = BTreeMap::new();

    for (key, value) in &flurl.headers {
        if key.starts_with("x-ms-") {
            sorted.insert(key.to_lowercase(), value.as_str());
        }
    }

    let mut headers_builder = StringBuilder::new();

    for (key, value) in sorted {
        headers_builder.append_str(key.as_str());
        headers_builder.append_char(':');
        headers_builder.append_str(value.replace("\r\n", "").as_str());
        headers_builder.append_char('\n');
    }

    return headers_builder.to_string_utf8().unwrap();
}

pub fn sign_transaction(string_to_sign: &str, key: &[u8]) -> String {
    /*cspell: Disable */
    let mut hmac256 = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");

    hmac256.update(string_to_sign.as_bytes());

    let result = hmac256.finalize();

    let code_bytes = result.into_bytes();

    let bytes_result = code_bytes.as_slice();

    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes_result)
    /*cspell: Enable */
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_canonical_headers_builder() {
        let flurl = FlUrl::new("https://127.0.0.1")
            .with_header("x-ms-date", "Fri, 17 Nov 2017 00:44:48 GMT")
            .with_header("x-ms-version", "2017-07-29");

        let msft_canonical_headers = get_canonicalized_headers(&flurl);

        assert_eq!(
            "x-ms-date:Fri, 17 Nov 2017 00:44:48 GMT\nx-ms-version:2017-07-29\n",
            msft_canonical_headers
        );
    }
    #[test]
    pub fn test_canonical_resources_builder_list_of_blobs_usecase() {
        let flurl = FlUrl::new("https://127.0.0.1").append_query_param("comp", "list");

        let msft_canonical_resources = get_canonicalized_resourse(&flurl, "contosorest");

        assert_eq!("/contosorest/\ncomp:list", msft_canonical_resources);
    }

    #[test]
    pub fn test_auth_header_to_sign() {
        let flurl = FlUrl::new("https://127.0.0.1")
            .with_header("x-ms-date", "Fri, 17 Nov 2017 01:07:37 GMT")
            .append_query_param("comp", "list")
            .with_header("x-ms-version", "2017-07-29");

        let header_to_sign = get_auth_header("contosorest", "0", SignVerb::GET, &flurl);

        assert_eq!(
            "GET\n\n\n0\n\n\n\n\n\n\n\n\nx-ms-date:Fri, 17 Nov 2017 01:07:37 GMT\nx-ms-version:2017-07-29\n/contosorest/\ncomp:list",
            header_to_sign

        )
    }
}
