mod my_azure_storage;

pub use my_azure_storage::blobs::block_blob::BlockBlob;
pub use my_azure_storage::blobs::models::BlobProperties;
pub use my_azure_storage::blobs::page_blob::PageBlob;
pub use my_azure_storage::blobs::page_blob::BLOB_PAGE_SIZE;
pub use my_azure_storage::AzureConnection;
pub use my_azure_storage::AzureStorageError;
