use crate::{blob::BlobProperties, page_blob::consts::BLOB_PAGE_SIZE};

struct Page {
    data: [u8; BLOB_PAGE_SIZE],
}

impl Page {
    pub fn new() -> Self {
        Self {
            data: [0; BLOB_PAGE_SIZE],
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.data.as_slice()
    }

    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        self.data.as_mut()
    }
}

pub struct PageBlobInMem {
    pages: Vec<Page>,
}

impl PageBlobInMem {
    pub fn new(pages_amount: usize) -> Self {
        let mut result = Self { pages: Vec::new() };

        result.resize(pages_amount);

        result
    }

    pub fn get_blob_properties(&self) -> BlobProperties {
        BlobProperties {
            blob_size: self.pages.len() * BLOB_PAGE_SIZE,
        }
    }

    pub fn resize(&mut self, pages_amount: usize) {
        while self.pages.len() < pages_amount {
            self.pages.push(Page::new());
        }

        while self.pages.len() > pages_amount {
            let index = self.pages.len() - 1;
            self.pages.remove(index);
        }
    }

    pub fn get_pages(&self, start_page_no: usize, pages_amount: usize) -> Vec<u8> {
        let mut result = Vec::new();

        let mut page_index = start_page_no;

        while page_index < start_page_no + pages_amount {
            result.extend_from_slice(self.pages[page_index].as_slice());

            page_index += 1;
        }

        result
    }

    pub fn save_pages(&mut self, start_page_no: usize, payload: Vec<u8>) {
        let pages_amount = payload.len() / BLOB_PAGE_SIZE;
        let mut page_index = start_page_no;

        let mut payload_index = 0;

        while page_index < start_page_no + pages_amount {
            let slice = &payload[payload_index..payload_index + BLOB_PAGE_SIZE];

            let page = self.pages.get_mut(page_index).unwrap();

            page.as_slice_mut().copy_from_slice(slice);

            page_index += 1;
            payload_index += BLOB_PAGE_SIZE;
        }
    }

    pub fn download(&self) -> Vec<u8> {
        let pages_amount = self.pages.len();

        let mut result = Vec::new();

        let mut page_index = 0;

        while page_index < pages_amount {
            result.extend_from_slice(self.pages[page_index].as_slice());

            page_index += 1;
        }

        result
    }

    pub fn get_size(&self) -> usize {
        self.pages.len() * BLOB_PAGE_SIZE
    }
}
