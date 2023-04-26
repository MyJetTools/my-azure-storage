pub const BLOB_PAGE_SIZE: usize = 512;

pub fn get_required_pages_amount(payload_size: usize) -> usize {
    let pages = payload_size / BLOB_PAGE_SIZE;

    if payload_size % BLOB_PAGE_SIZE == 0 {
        pages
    } else {
        pages + 1
    }
}

#[cfg(test)]
mod tests {
    use crate::page_blob::consts::BLOB_PAGE_SIZE;

    #[test]
    fn test() {
        assert_eq!(super::get_required_pages_amount(1), 1);

        assert_eq!(super::get_required_pages_amount(BLOB_PAGE_SIZE), 1);

        assert_eq!(super::get_required_pages_amount(BLOB_PAGE_SIZE + 1), 2);
        assert_eq!(super::get_required_pages_amount(BLOB_PAGE_SIZE * 2), 2);
    }
}
