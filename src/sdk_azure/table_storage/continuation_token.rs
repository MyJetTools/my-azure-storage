use flurl::FlUrlResponse;

pub struct ContinuationToken {
    pub next_partition_key: Option<String>,
    pub next_row_key: Option<String>,
}

impl ContinuationToken {
    pub fn new(response: &FlUrlResponse) -> Option<Self> {
        let headers = response.get_headers();

        let next_partition_key =
            if let Some(header) = headers.get("x-ms-continuation-NextPartitionKey") {
                Some(header.to_string())
            } else {
                None
            };

        let next_row_key = if let Some(header) = headers.get("x-ms-continuation-NextRowKey") {
            Some(header.to_string())
        } else {
            None
        };

        if next_partition_key.is_none() && next_row_key.is_none() {
            return None;
        }

        Some(ContinuationToken {
            next_partition_key,
            next_row_key,
        })
        .into()
    }
}
