use crate::AzureStorageError;

use super::utils::FileConnectionInfo;

pub async fn create_if_not_exists<TFileConnectionInfo: FileConnectionInfo>(
    connection: &TFileConnectionInfo,
    container_name: &str,
) -> Result<(), AzureStorageError> {
    let folder_name = super::utils::compile_container_path(connection, container_name);

    match tokio::fs::create_dir(folder_name.as_str()).await {
        Ok(_) => Ok(()),
        Err(err) => match err.kind() {
            std::io::ErrorKind::AlreadyExists => Ok(()),
            _ => {
                return Err(AzureStorageError::UnknownError {
                    msg: format!("{:?}", err),
                });
            }
        },
    }
}

pub async fn delete<TFileConnectionInfo: FileConnectionInfo>(
    connection_data: &TFileConnectionInfo,
    container_name: &str,
) -> Result<(), AzureStorageError> {
    let folder_name = super::utils::compile_container_path(connection_data, container_name);
    tokio::fs::remove_dir_all(folder_name.as_str()).await?;
    Ok(())
}

pub async fn delete_if_exists<TFileConnectionInfo: FileConnectionInfo>(
    connection_data: &TFileConnectionInfo,
    container_name: &str,
) -> Result<(), AzureStorageError> {
    let folder_name = super::utils::compile_container_path(connection_data, container_name);

    match tokio::fs::remove_dir_all(folder_name).await {
        Ok(_) => Ok(()),
        Err(_) => Ok(()),
    }
}

pub async fn get_list<TFileConnectionInfo: FileConnectionInfo>(
    connection_data: &TFileConnectionInfo,
) -> Result<Vec<String>, hyper::Error> {
    let mut result = Vec::new();

    let root_path = connection_data.get_root_path();
    let path_separator = connection_data.get_separator();

    for entry in std::fs::read_dir(root_path).unwrap() {
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
