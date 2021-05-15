use flurl::FlUrlResponse;
use std::collections::HashMap;

use super::AzureStorageError;

pub fn check_if_there_is_an_error<'t>(
    fl_response: &'t FlUrlResponse,
) -> Result<HashMap<&'t str, &'t str>, AzureStorageError> {
    let headers = fl_response.get_headers();

    let status_code = fl_response.get_status_code();
    if status_code >= 300 {
        let err_header_result = headers.get("x-ms-error-code");

        println!("{:?}", err_header_result);

        if let Some(err_header) = err_header_result {
            let err_header = *err_header;

            let err = match err_header {
                "ContainerNotFound" => AzureStorageError::ContainerNotFound,
                "ContainerBeingDeleted" => AzureStorageError::ContainerBeingDeleted,
                "BlobNotFound" => AzureStorageError::BlobNotFound,
                "ContainerAlreadyExists" => AzureStorageError::ContainerAlreadyExists,
                _ => AzureStorageError::UnknownError {
                    msg: err_header.to_string(),
                },
            };

            return Err(err);
        }
    }

    Ok(headers)
}
