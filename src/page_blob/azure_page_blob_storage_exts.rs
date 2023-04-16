use crate::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

use super::AzurePageBlobStorage;

#[async_trait::async_trait]
pub trait AzurePageBlobStorageExts {
    async fn save_pages_ext(
        &self,
        start_page_no: usize,
        payload: Vec<u8>,
        pages_amount_per_round_trip: usize,
    ) -> Result<(), AzureStorageError>;
}
#[async_trait::async_trait]
impl AzurePageBlobStorageExts for AzurePageBlobStorage {
    async fn save_pages_ext(
        &self,
        start_page_no: usize,
        payload: Vec<u8>,
        pages_amount_per_round_trip: usize,
    ) -> Result<(), AzureStorageError> {
        let pages_to_save = payload.len() / BLOB_PAGE_SIZE;

        if pages_to_save <= pages_amount_per_round_trip {
            self.save_pages(start_page_no, payload).await?;
            return Ok(());
        }

        let mut pages_offset = 0;

        while pages_offset < pages_to_save {
            let pages_to_save_chunk = pages_to_save - pages_offset;

            let pages_to_save_amount_this_round_trip =
                if pages_to_save_chunk > pages_amount_per_round_trip {
                    pages_amount_per_round_trip
                } else {
                    pages_to_save_chunk
                };

            let payload_to_save = payload[pages_offset * BLOB_PAGE_SIZE
                ..(pages_offset + pages_to_save_amount_this_round_trip) * BLOB_PAGE_SIZE]
                .to_vec();

            self.save_pages(start_page_no + pages_offset, payload_to_save)
                .await?;

            pages_offset += pages_to_save_amount_this_round_trip;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        page_blob::{consts::BLOB_PAGE_SIZE, AzurePageBlobStorage, AzurePageBlobStorageExts},
        AzureStorageConnection,
    };

    #[tokio::test]
    async fn save_pages_by_chunks() {
        let connection = AzureStorageConnection::new_in_memory();

        let page_blob = AzurePageBlobStorage::new(Arc::new(connection), "Test", "Test").await;
        page_blob.create_container_if_not_exist().await.unwrap();
        page_blob.create_if_not_exists(0).await.unwrap();

        page_blob.resize(3).await.unwrap();

        let mut payload = Vec::new();

        for _ in 0..BLOB_PAGE_SIZE {
            payload.push(1u8);
        }

        for _ in 0..BLOB_PAGE_SIZE {
            payload.push(2u8);
        }

        for _ in 0..BLOB_PAGE_SIZE {
            payload.push(3u8);
        }

        page_blob
            .save_pages_ext(0, payload.clone(), 2)
            .await
            .unwrap();

        let download = page_blob.download().await.unwrap();

        assert_eq!(download, payload);
    }
}
