pub struct AzureItems<T> {
    pub next_marker: Option<String>,
    pub items: Vec<T>,
}

pub struct BlobProperties {
    pub len: usize,
}
