mod blob_models;
pub mod block_blob;
pub mod page_blob;

pub use blob_models::{deserialize_list_of_blobs, deserialize_list_of_containers};
