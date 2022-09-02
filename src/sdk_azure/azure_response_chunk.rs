pub struct AzureResponseChunk<T> {
    pub next_marker: Option<String>,
    pub items: Vec<T>,
}
