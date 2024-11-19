use flurl::FlUrlResponse;
use std::collections::HashMap;

use crate::AzureStorageError;

pub struct AzureResponseHandler {
    fl_response: FlUrlResponse,
    headers: HashMap<String, String>,
}

impl<'t> AzureResponseHandler {
    pub fn new(fl_response: FlUrlResponse) -> Self {
        let mut headers = HashMap::new();
        for (key, value) in fl_response.get_headers().iter() {
            if let Some(value) = value {
                headers.insert(key.to_string(), value.to_string());
            }
        }
        Self {
            headers,
            fl_response,
        }
    }

    pub fn get_header(&'t self, header_name: &'t str) -> Result<&'t String, AzureStorageError> {
        let result = self.headers.get(header_name);
        let result = match result {
            Some(value) => Ok(value),
            None => {
                let msg = format!("Header not found {}", header_name);
                let err = AzureStorageError::UnknownError { msg };
                Err(err)
            }
        };

        result
    }

    fn get_azure_error(&self) -> Option<AzureStorageError> {
        let status_code = self.fl_response.get_status_code();
        if status_code >= 300 {
            let err_header_result = self.get_header("x-ms-error-code");

            if let Ok(err_header) = err_header_result {
                let err_header = err_header.as_str();
                return Some(AzureStorageError::parse(err_header));
            }
        }

        None
    }

    pub async fn get_body(self) -> Result<Vec<u8>, AzureStorageError> {
        let result = self.fl_response.receive_body().await?;
        return Ok(result);
    }

    pub fn check_if_there_is_an_error(self) -> Result<AzureResponseHandler, AzureStorageError> {
        let has_error = self.get_azure_error();

        return match has_error {
            Some(err) => {
                return Err(err);
            }
            None => Ok(self),
        };
    }

    pub fn check_if_there_is_an_error_and_ignore_one(
        self,
        error_to_ignore: AzureStorageError,
    ) -> Result<AzureResponseHandler, AzureStorageError> {
        let has_error = self.get_azure_error();

        match has_error {
            Some(_err) => {
                if matches!(error_to_ignore, _err) {
                    return Ok(self);
                }

                return Err(_err);
            }
            None => Ok(self),
        }
    }
}

pub trait ToAzureResponseHandler {
    fn to_azure_response_handler(self) -> AzureResponseHandler;
}

impl<'t> ToAzureResponseHandler for FlUrlResponse {
    fn to_azure_response_handler(self) -> AzureResponseHandler {
        AzureResponseHandler::new(self)
    }
}
