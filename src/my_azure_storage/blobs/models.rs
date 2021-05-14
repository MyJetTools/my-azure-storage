pub struct AzureItems<T> {
    pub next_marker: Option<String>,
    pub items: Vec<T>,
}

#[derive(Debug)]
pub struct BlobProperties {
    pub blob_size: u64,
}
