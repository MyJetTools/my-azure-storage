# my-azure-storage

Rust client for Azure Storage blobs and (optional) table storage with pluggable backends (real Azure REST, local filesystem, or in-memory for tests).

## Add to Cargo.toml

```toml
[dependencies]
my-azure-storage-sdk = { git = "https://github.com/MyJetTools/my-azure-storage", tag = "0.5.1" }
# enable if you need Azure Table Storage
# my-azure-storage-sdk = { git = "...", tag = "0.5.1", features = ["table-storage"] }
```

## Create a connection (Azure / file / in-memory)

```rust
use std::sync::Arc;
use my_azure_storage_sdk::AzureStorageConnection;

// Azure account
let conn = AzureStorageConnection::from_conn_string("DefaultEndpointsProtocol=https;AccountName=xxx;AccountKey=xxxx;EndpointSuffix=core.windows.net");
let conn = Arc::new(conn);

// File-backed (good for offline/local runs)
let file_conn = Arc::new(AzureStorageConnection::from_conn_string("~/tmp/azure-emulator/"));

// Fully in-memory (great for tests)
let mem_conn = Arc::new(AzureStorageConnection::new_in_memory());
```

## Blob containers

```rust
use my_azure_storage_sdk::blob_container::BlobContainersApi;

// create once
conn.create_container_if_not_exists("images").await?;

// list containers / blobs
let containers = conn.get_list_of_blob_containers().await?;
let blobs = conn.get_list_of_blobs("images").await?;

// delete
conn.delete_container_if_not_exists("old-backups").await?;
```

## Block blobs (upload/download whole blobs)

```rust
use my_azure_storage_sdk::{block_blob::BlockBlobApi, blob::BlobApi};

let bytes = b"hello world".to_vec();
conn.upload_block_blob("images", "hello.txt", bytes).await?;

let downloaded = conn.download_blob("images", "hello.txt").await?;
let props = conn.get_blob_properties("images", "hello.txt").await?;

conn.delete_blob("images", "hello.txt").await?;
```

## Page blobs (sparse, random-access, 512-byte pages)

```rust
use std::sync::Arc;
use my_azure_storage_sdk::{
    page_blob::{consts::BLOB_PAGE_SIZE, AzurePageBlobStorage},
    AzureStorageError,
};

let page_blob = AzurePageBlobStorage::new(conn.clone(), "vm-disks", "disk.vhd").await;
page_blob.create_container_if_not_exists().await?;

// create 16 pages (16 * 512 bytes)
page_blob.create(16).await?;

// write 2 pages starting at page 0
let data = vec![0u8; 2 * BLOB_PAGE_SIZE];
page_blob.save_pages(0, data).await?;

// read back a slice
let slice = page_blob.get_pages(0, 2).await?;

// grow / resize
page_blob.resize(32).await?;

page_blob.delete_if_exists().await?;
```

## Table storage (feature flag: `table-storage`)

```rust
use my_azure_storage_sdk::table_storage::{TableStorage, TableStorageEntity};
use my_json::json_writer::JsonObjectWriter;
use std::sync::Arc;

#[derive(Clone)]
struct Todo {
    partition: String,
    row: String,
    title: String,
}

impl TableStorageEntity for Todo {
    fn create(json: my_json::json_reader::JsonFirstLineReader) -> Self {
        // minimal parser for the example
        let partition = json.get_first_line("PartitionKey").unwrap().as_str().to_string();
        let row = json.get_first_line("RowKey").unwrap().as_str().to_string();
        let title = json.get_first_line("title").unwrap().as_str().to_string();
        Self { partition, row, title }
    }
    fn populate_field_names(builder: &mut my_azure_storage_sdk::sdk_azure::table_storage::TableStorageQueryBuilder) {
        builder.add_fields(&["PartitionKey", "RowKey", "title"]);
    }
    fn serialize(&self) -> Vec<u8> {
        let mut writer = JsonObjectWriter::new();
        writer.write("PartitionKey", &self.partition);
        writer.write("RowKey", &self.row);
        writer.write("title", &self.title);
        writer.into_bytes()
    }
    fn get_partition_key(&self) -> &str { &self.partition }
    fn get_row_key(&self) -> &str { &self.row }
}

let table = TableStorage::<Todo>::new(conn.clone(), "todos".to_string());
table.create_table_if_not_exists().await?;

let todo = Todo { partition: "p1".into(), row: "1".into(), title: "finish docs".into() };
table.insert_or_replace_entity(&todo).await?;

let fetched = table.get_entity("p1", "1").await?;
let all_in_partition = table.get_entities_by_partition_key("p1").await?;
table.delete_entity("p1", "1").await?;
```

### Table storage details

- Enable with `features = ["table-storage"]`. Backends: Azure REST and in-memory. File-backed table storage is basic (hex-encoded names on disk) and intended for local/dev use only.
- Paging: table name listing returns `TableNamesChunk`; entities use `TableEntitiesChunk`. Call `get_next().await?` in a loop until it returns `Ok(None)`.
- CRUD semantics: `insert_entity` fails when the entity exists; `insert_or_replace_entity` overwrites. Deletes return a boolean (found/not found).
- Errors: look for `TableStorageError::TableNotFound`, `EntityAlreadyExists`, and network errors propagated from Azure.
- Testing: for fast tests use `AzureStorageConnection::new_in_memory()`. For persistence without Azure, prefer blob/page file backends; table file backend is minimal.

## Local test / emulation tips

- Use the file-backed connection (`~/tmp/azure-emulator/`) to persist blobs locally without hitting Azure.
- Use `AzureStorageConnection::new_in_memory()` for fast unit tests (blobs, page blobs, and table storage).

