use rust_extensions::AsSliceOrVec;

#[derive(Clone)]
pub struct PageBlobContentToUpload(Vec<u8>);

impl PageBlobContentToUpload {
    pub fn new<'s>(content: impl Into<AsSliceOrVec<'s, u8>>, fill_byte: u8) -> Self {
        let content: AsSliceOrVec<'_, u8> = content.into();

        let mut content = content.into_vec();

        let pages = super::consts::get_required_pages_amount(content.len());

        let required_size = pages * super::consts::BLOB_PAGE_SIZE;

        let bytes_to_fill = required_size - content.len();

        if bytes_to_fill > 0 {
            content.resize(required_size, fill_byte);
        }

        Self(content)
    }

    pub fn get_size_in_pages(&self) -> usize {
        super::consts::get_required_pages_amount(self.0.len())
    }
}

impl<'s> Into<AsSliceOrVec<'s, u8>> for PageBlobContentToUpload {
    fn into(self) -> AsSliceOrVec<'s, u8> {
        AsSliceOrVec::AsVec(self.0)
    }
}
