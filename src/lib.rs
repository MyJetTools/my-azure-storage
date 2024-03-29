pub mod blob;
pub mod blob_container;
pub mod block_blob;
mod connection;

pub mod page_blob;
pub mod sdk_azure;
mod sdk_files;
#[cfg(feature = "table-storage")]
pub mod table_storage;

pub use connection::{AzureStorageConnection, AzureStorageConnectionData};
mod types;
pub use types::AzureStorageError;
