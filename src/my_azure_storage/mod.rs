pub mod blob;

mod azure_response_handler;
mod connection;
mod consts;
mod fl_requests;
mod flurl_ext;
mod sign_utils;
mod string_builder;
mod types;

pub use connection::AzureConnection;
pub use flurl_ext::FlUrlAzureExtensions;
pub use sign_utils::SignVerb;
use string_builder::StringBuilder;
pub use types::AzureStorageError;

pub use blob::BlobProperties;

pub mod blob_container;
pub mod block_blob;
pub mod page_blob;

pub use page_blob::BLOB_PAGE_SIZE;
