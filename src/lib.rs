pub mod my_azure_storage;
pub use my_azure_storage::AzureConnection;
pub use my_azure_storage::AzureStorageError;

pub use my_azure_storage::blob;
pub use my_azure_storage::blob_container;
pub use my_azure_storage::block_blob;
pub use my_azure_storage::page_blob;

pub use my_azure_storage::page_blob::BLOB_PAGE_SIZE;
