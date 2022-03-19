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
    let metadata = tokio::fs::metadata(file_name).await?;

    Ok(BlobProperties {
        blob_size: metadata.len() as usize,
    })
}
