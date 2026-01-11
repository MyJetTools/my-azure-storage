mod blob_data;
mod block_blob;
mod container_in_mem;
mod mem_storage_data;
pub mod operations;
mod page_blob;
#[cfg(feature = "table-storage")]
mod table_storage;
pub use blob_data::BlobData;
pub use container_in_mem::ContainerInMem;
pub use mem_storage_data::MemStorageData;
pub use page_blob::PageBlobInMem;
#[cfg(feature = "table-storage")]
pub use table_storage::TableStorageInMem;
