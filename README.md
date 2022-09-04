# my-azure-storage

### AzureConnectionString

First of al - we need a connection to Account. We wrap it into Arc so it can be reused with several instances of Blobs and Table Storages;

```Rust
    let connection_string = AzureStorageConnection::from_conn_string(""DefaultEndpointsProtocol=https;AccountName=xxx;AccountKey=xxxx;EndpointSuffix=core.windows.net");
    let connection_string = Arc::new(connection_string);
```

### Table Storage usages:

#### Basic Setup
First of all we specify TableEntity for a table. To do that - there is a macros library: https://github.com/MyJetTools/table-storage-entity

```Toml
[dependencies]
table-storage-entity = { tag = "xxx", git = "https://github.com/MyJetTools/table-storage-entity.git", features=["table-storage"] }
my-json = { tag = "xxx", git = "https://github.com/MyJetTools/my-json.git" }
```

```Rust
use table_storage_entity::TableStorageEntity;

#[derive(TableStorageEntity, Debug)]
pub struct TestTableEntity {
    pub partition_key: String,
    pub row_key: String,
    pub timestamp: Option<String>,
    pub id: String,
}
```
```Rust
let table_storage: TableStorage<TestTableEntity> =
    TableStorage::new(connection_string, "TestTable".to_string());
```

#### Manipulations with tables

##### Crate Table
```Rust

let result = table_storage.crate_table().await;
```


##### Crate Table if not exists
```Rust
let result = table_storage.crate_table_if_not_exists().await;
```


##### Get list of tables
```Rust

if let Some(mut table_names_chunk) = table_storage.get_table_list().await? {
    while let Some(table_names) = table_names_chunk.get_next().await? {
        println!("Got tables: {:?}", table_names)
    }
}
```

