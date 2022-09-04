# my-azure-storage

### AzureConnectionString

First of al - we need a connection to Account. We wrap it into Arc so it can be reused with several instances of Blobs and Table Storages;

```Rust
    let connection_string = AzureStorageConnection::from_conn_string(""DefaultEndpointsProtocol=https;AccountName=xxx;AccountKey=xxxx;EndpointSuffix=core.windows.net");
    let connection_string = Arc::new(connection_string);
```

### Table Storage usages:

https://github.com/MyJetTools/my-azure-storage/wiki/Azure-Table-Storage-usages
