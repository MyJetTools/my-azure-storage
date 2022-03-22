pub struct BlockBlob {
    content: Vec<u8>,
}

impl BlockBlob {
    pub fn new(content: Vec<u8>) -> Self {
        Self { content }
    }

    pub fn get_content(&self) -> &[u8] {
        self.content.as_slice()
    }
}
