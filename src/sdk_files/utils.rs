use crate::{blob::BlobProperties, AzureStorageError};

pub trait FileConnectionInfo {
    fn get_root_path(&self) -> &str;
    fn get_separator(&self) -> char;
}

pub fn compile_blob_path<TFileConnectionInfo: FileConnectionInfo>(
    connection: &TFileConnectionInfo,
    container_name: &str,
    blob_name: &str,
) -> String {
    format!(
        "{root_path}{container_name}{path_separator}{blob_name}",
        root_path = connection.get_root_path(),
        path_separator = connection.get_separator(),
    )
}

pub fn compile_container_path<TFileConnectionInfo: FileConnectionInfo>(
    connection: &TFileConnectionInfo,
    container_name: &str,
) -> String {
    format!(
        "{root_path}{container_name}",
        root_path = connection.get_root_path(),
    )
}

pub async fn get_blob_properties(file_name: &str) -> Result<BlobProperties, AzureStorageError> {
    match tokio::fs::metadata(file_name).await {
        Ok(metadata) => {
            return Ok(BlobProperties {
                blob_size: metadata.len() as usize,
            });
        }
        Err(err) => {
            if let std::io::ErrorKind::NotFound = err.kind() {
                return Err(AzureStorageError::BlobNotFound);
            }

            return Err(AzureStorageError::IoError(err));
        }
    }
}

pub fn extract_file_name(full_path: &str, separator: char) -> &str {
    let full_path_as_bytes = full_path.as_bytes();

    for index in (0..full_path_as_bytes.len()).rev() {
        if full_path_as_bytes[index] == separator as u8 {
            return &full_path[index + 1..];
        }
    }

    panic!("Can not extract filename from full path [{}]", full_path);
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_extract_file_name() {
        let src_path = "/Users/Folder/FileName";
        let result = extract_file_name(src_path, '/');
        assert_eq!("FileName", result);
    }
}
