use crate::blob::BlobProperties;

use super::{block_blob::BlockBlob, PageBlobInMem};

pub enum BlobData {
    BlockBlob(BlockBlob),
    PageBlob(PageBlobInMem),
}

impl BlobData {
    pub fn download(&self) -> Vec<u8> {
        match self {
            BlobData::BlockBlob(block_blob) => return block_blob.get_content().to_vec(),
            BlobData::PageBlob(page_blob) => return page_blob.download(),
        }
    }

    pub fn get_blob_properties(&self) -> BlobProperties {
        match self {
            BlobData::BlockBlob(block_blob) => {
                return BlobProperties {
                    blob_size: block_blob.get_content().len(),
                }
            }
            BlobData::PageBlob(page_blob) => {
                return BlobProperties {
                    blob_size: page_blob.get_size(),
                }
            }
        }
    }
}
