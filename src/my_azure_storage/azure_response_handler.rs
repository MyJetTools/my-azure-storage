use flurl::FlUrlResponse;
use std::collections::HashMap;

use super::AzureStorageError;

pub struct AzureResponseHandler {
    fl_response: FlUrlResponse,
    headers: HashMap<String, String>,
}

impl<'t> AzureResponseHandler {
    pub fn new(fl_response: FlUrlResponse) -> Self {
        let mut result = AzureResponseHandler {
            headers: HashMap::new(),
            fl_response,
        };

        result
            .fl_response
            .fill_headers_to_hashmap(&mut result.headers);

        result
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

                let err = match err_header {
                    "ContainerNotFound" => AzureStorageError::ContainerNotFound,
                    "ContainerBeingDeleted" => AzureStorageError::ContainerBeingDeleted,
                    "BlobNotFound" => AzureStorageError::BlobNotFound,
                    "ContainerAlreadyExists" => AzureStorageError::ContainerAlreadyExists,
                    _ => {
                        println!("{:?}", err_header);
                        AzureStorageError::UnknownError {
                            msg: err_header.to_string(),
                        }
                    }
                };

                return Some(err);
            }
        }

        None
    }

    pub async fn get_body(self) -> Result<Vec<u8>, AzureStorageError> {
        let result = self.fl_response.get_body().await?;

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

    pub fn check_if_there_is_an_error_and_ignore_container_already_exists(
        self,
    ) -> Result<AzureResponseHandler, AzureStorageError> {
        let has_error = self.get_azure_error();

        return match has_error {
            Some(err) => match err {
                AzureStorageError::ContainerAlreadyExists => Ok(self),
                _ => Err(err),
            },
            None => Ok(self),
        };
    }

    pub fn check_if_there_is_an_error_and_ignore_blob_already_exists(
        self,
    ) -> Result<AzureResponseHandler, AzureStorageError> {
        let has_error = self.get_azure_error();

        return match has_error {
            Some(err) => match err {
                AzureStorageError::BlobAlreadyExists => Ok(self),
                _ => Err(err),
            },
            None => Ok(self),
        };
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
