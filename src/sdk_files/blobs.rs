use crate::{blob::BlobProperties, AzureStorageError};

use super::utils::FileConnectionInfo;

pub async fn get_list<TFileConnectionInfo: FileConnectionInfo>(
    connection_data: &TFileConnectionInfo,
    container_name: &str,
) -> Result<Vec<String>, AzureStorageError> {
    let container_path = super::utils::compile_container_path(connection_data, container_name);
    let path_separator = connection_data.get_separator();

    let mut result = Vec::new();

    for entry in std::fs::read_dir(container_path.as_str()).unwrap() {
        if let Ok(entity) = entry {
            let file_type = entity.file_type().unwrap();

            if file_type.is_dir() {
                let path = entity.path();

                let path = format!("{}", path.display());

                result.push(
                    super::utils::extract_file_name(path.as_str(), path_separator).to_string(),
                );
            }
        }
    }

    Ok(result)
}

pub async fn get_blob_properties<TFileConnectionInfo: FileConnectionInfo>(
    connection_data: &TFileConnectionInfo,
    container_name: &str,
    blob_name: &str,
) -> Result<BlobProperties, AzureStorageError> {
    let file_name = super::utils::compile_blob_path(connection_data, container_name, blob_name);

    super::utils::get_blob_properties(file_name.as_str()).await
}

pub async fn delete<TFileConnectionInfo: FileConnectionInfo>(
    connection_data: &TFileConnectionInfo,
    container_name: &str,
    blob_name: &str,
) -> Result<(), AzureStorageError> {
    let file_name = super::utils::compile_blob_path(connection_data, container_name, blob_name);

    match tokio::fs::remove_file(file_name).await {
        Ok(_) => Ok(()),
        Err(err) => match err.kind() {
            std::io::ErrorKind::NotFound => {
                return Err(AzureStorageError::BlobNotFound);
            }
            _ => {
                return Err(AzureStorageError::UnknownError {
                    msg: err.to_string(),
                });
            }
        },
    }
}

pub async fn delete_if_exists<TFileConnectionInfo: FileConnectionInfo>(
    connection_data: &TFileConnectionInfo,
    container_name: &str,
    blob_name: &str,
) -> Result<(), AzureStorageError> {
    let file_name = super::utils::compile_blob_path(connection_data, container_name, blob_name);

    match tokio::fs::remove_file(file_name).await {
        Ok(_) => Ok(()),
        Err(_) => Ok(()),
    }
}

pub async fn download<TFileConnectionInfo: FileConnectionInfo>(
    connection_data: &TFileConnectionInfo,
    container_name: &str,
    blob_name: &str,
) -> Result<Vec<u8>, AzureStorageError> {
    let file_name = super::utils::compile_blob_path(connection_data, container_name, blob_name);

    let mut file = tokio::fs::File::open(file_name).await?;

    let mut result = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut file, &mut result).await?;
    return Ok(result);
}

pub async fn upload<TFileConnectionInfo: FileConnectionInfo>(
    connection_data: &TFileConnectionInfo,
    container_name: &str,
    blob_name: &str,
    content: &[u8],
) -> Result<(), AzureStorageError> {
    let file_name =
        crate::sdk_files::utils::compile_blob_path(connection_data, container_name, blob_name);

    let mut f = tokio::fs::OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open(file_name.as_str())
        .await?;
    tokio::io::AsyncWriteExt::write_all(&mut f, &content).await?;
    tokio::io::AsyncWriteExt::flush(&mut f).await?;
    Ok(())
}
