use crate::blob::BlobProperties;

use super::consts::BLOB_PAGE_SIZE;

#[derive(Debug, Clone)]
pub struct PageBlobProperties {
    pub blob_properties: BlobProperties,
}

impl PageBlobProperties {
    pub fn new(blob_properties: BlobProperties) -> Self {
        Self { blob_properties }
    }

    pub fn get_pages_amount(&self) -> usize {
        self.blob_properties.blob_size / BLOB_PAGE_SIZE
    }
    pub fn get_blob_size(&self) -> usize {
        self.blob_properties.blob_size
    }
}

impl Into<PageBlobProperties> for BlobProperties {
    fn into(self) -> PageBlobProperties {
        PageBlobProperties::new(self)
    }
}
