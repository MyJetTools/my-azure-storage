use std::collections::BTreeMap;

use tokio::fs;

use crate::{
    sdk_files::utils::FileConnectionInfo, table_storage::TableStorageEntity,
    table_storage::TableStorageError,
};

fn encode_name(src: &str) -> String {
    hex::encode(src.as_bytes())
}

fn decode_name(src: &str) -> Option<String> {
    hex::decode(src.as_bytes())
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
}

fn table_dir_path<T: FileConnectionInfo>(connection: &T, table_name: &str) -> String {
    format!("{}{}", connection.get_root_path(), encode_name(table_name))
}

fn partition_file_path<T: FileConnectionInfo>(
    connection: &T,
    table_name: &str,
    partition_key: &str,
) -> String {
    format!(
        "{}{}{}",
        table_dir_path(connection, table_name),
        connection.get_separator(),
        encode_name(partition_key)
    )
}

fn serialize_partition(entries: &BTreeMap<String, Vec<u8>>) -> String {
    let mut result = String::with_capacity(entries.len() * 64);
    for (row_key, payload) in entries {
        let row_hex = encode_name(row_key);
        let payload_hex = hex::encode(payload);
        result.push_str(&row_hex);
        result.push(':');
        result.push_str(&payload_hex);
        result.push('\n');
    }
    result
}

fn deserialize_partition(content: &str) -> BTreeMap<String, Vec<u8>> {
    let mut map = BTreeMap::new();
    for line in content.lines() {
        if let Some((row_hex, payload_hex)) = line.split_once(':') {
            if let (Some(row_key), Ok(payload)) =
                (decode_name(row_hex), hex::decode(payload_hex.as_bytes()))
            {
                map.insert(row_key, payload);
            }
        }
    }
    map
}

pub async fn create_table<T: FileConnectionInfo>(
    connection: &T,
    table_name: &str,
) -> Result<(), TableStorageError> {
    let path = table_dir_path(connection, table_name);
    match fs::metadata(&path).await {
        Ok(meta) if meta.is_dir() => Err(TableStorageError::TableAlreadyExists),
        Ok(_) => Err(TableStorageError::TableAlreadyExists),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(&path).await?;
            Ok(())
        }
        Err(err) => Err(TableStorageError::Unknown(err.to_string())),
    }
}

pub async fn list_tables<T: FileConnectionInfo>(
    connection: &T,
) -> Result<Option<Vec<String>>, TableStorageError> {
    let mut dir = match fs::read_dir(connection.get_root_path()).await {
        Ok(d) => d,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(TableStorageError::Unknown(err.to_string())),
    };

    let mut result = Vec::new();
    while let Ok(Some(entry)) = dir.next_entry().await {
        if let Ok(file_type) = entry.file_type().await {
            if file_type.is_dir() {
                if let Some(decoded) = entry.file_name().to_str().and_then(decode_name) {
                    result.push(decoded);
                }
            }
        }
    }

    if result.is_empty() {
        Ok(None)
    } else {
        Ok(Some(result))
    }
}

async fn load_partition<T: FileConnectionInfo>(
    connection: &T,
    table_name: &str,
    partition_key: &str,
) -> Result<Option<BTreeMap<String, Vec<u8>>>, TableStorageError> {
    let table_path = table_dir_path(connection, table_name);
    if fs::metadata(&table_path).await.is_err() {
        return Err(TableStorageError::TableNotFound);
    }

    let path = partition_file_path(connection, table_name, partition_key);
    match fs::read_to_string(&path).await {
        Ok(content) => Ok(Some(deserialize_partition(&content))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(TableStorageError::Unknown(err.to_string())),
    }
}

async fn save_partition<T: FileConnectionInfo>(
    connection: &T,
    table_name: &str,
    partition_key: &str,
    entries: &BTreeMap<String, Vec<u8>>,
) -> Result<(), TableStorageError> {
    let table_path = table_dir_path(connection, table_name);
    fs::create_dir_all(&table_path).await?;

    let path = partition_file_path(connection, table_name, partition_key);
    let content = serialize_partition(entries);
    fs::write(path, content).await?;
    Ok(())
}

pub async fn get_entity<T: FileConnectionInfo, TEntity: TableStorageEntity>(
    connection: &T,
    table_name: &str,
    partition_key: &str,
    row_key: &str,
) -> Result<Option<TEntity>, TableStorageError> {
    match load_partition(connection, table_name, partition_key).await? {
        Some(map) => {
            if let Some(payload) = map.get(row_key) {
                Ok(Some(TEntity::create(
                    my_json::json_reader::JsonFirstLineReader::new(payload),
                )))
            } else {
                Ok(None)
            }
        }
        None => Ok(None),
    }
}

pub async fn get_by_partition<T: FileConnectionInfo, TEntity: TableStorageEntity>(
    connection: &T,
    table_name: &str,
    partition_key: &str,
) -> Result<Option<Vec<TEntity>>, TableStorageError> {
    match load_partition(connection, table_name, partition_key).await? {
        Some(map) => {
            let entities = map
                .into_iter()
                .map(|(_, payload)| {
                    TEntity::create(my_json::json_reader::JsonFirstLineReader::new(&payload))
                })
                .collect();
            Ok(Some(entities))
        }
        None => Ok(None),
    }
}

pub async fn get_all_entities<T: FileConnectionInfo, TEntity: TableStorageEntity>(
    connection: &T,
    table_name: &str,
) -> Result<Option<Vec<TEntity>>, TableStorageError> {
    let table_path = table_dir_path(connection, table_name);
    let mut dir = match fs::read_dir(&table_path).await {
        Ok(d) => d,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(TableStorageError::TableNotFound)
        }
        Err(err) => return Err(TableStorageError::Unknown(err.to_string())),
    };

    let mut all = Vec::new();
    let mut found_any = false;
    while let Ok(Some(entry)) = dir.next_entry().await {
        if let Ok(file_type) = entry.file_type().await {
            if file_type.is_file() {
                let partition_name = entry.file_name();
                let partition_key_opt = partition_name.to_str().and_then(decode_name);
                if partition_key_opt.is_none() {
                    continue;
                }
                let partition_key = partition_key_opt.unwrap();
                if let Some(map) = load_partition(connection, table_name, &partition_key).await? {
                    found_any = true;
                    for (_, payload) in map {
                        all.push(TEntity::create(
                            my_json::json_reader::JsonFirstLineReader::new(&payload),
                        ));
                    }
                }
            }
        }
    }

    if found_any {
        Ok(Some(all))
    } else {
        Ok(None)
    }
}

pub async fn insert_or_replace<T: FileConnectionInfo, TEntity: TableStorageEntity>(
    connection: &T,
    table_name: &str,
    entity: &TEntity,
) -> Result<(), TableStorageError> {
    let mut map = load_partition(connection, table_name, entity.get_partition_key())
        .await?
        .unwrap_or_default();
    map.insert(entity.get_row_key().to_string(), entity.serialize());
    save_partition(connection, table_name, entity.get_partition_key(), &map).await
}

pub async fn insert<T: FileConnectionInfo, TEntity: TableStorageEntity>(
    connection: &T,
    table_name: &str,
    entity: &TEntity,
) -> Result<(), TableStorageError> {
    let mut map = load_partition(connection, table_name, entity.get_partition_key())
        .await?
        .unwrap_or_default();
    if map.contains_key(entity.get_row_key()) {
        return Err(TableStorageError::EntityAlreadyExists);
    }
    map.insert(entity.get_row_key().to_string(), entity.serialize());
    save_partition(connection, table_name, entity.get_partition_key(), &map).await
}

pub async fn delete_entity<T: FileConnectionInfo>(
    connection: &T,
    table_name: &str,
    partition_key: &str,
    row_key: &str,
) -> Result<bool, TableStorageError> {
    let mut map = match load_partition(connection, table_name, partition_key).await? {
        Some(m) => m,
        None => return Ok(false),
    };

    let existed = map.remove(row_key).is_some();
    if existed {
        save_partition(connection, table_name, partition_key, &map).await?;
    }
    Ok(existed)
}
