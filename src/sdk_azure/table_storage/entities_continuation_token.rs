use flurl::FlUrlResponse;

pub struct EntitiesContinuationToken {
    pub next_partition_key: Option<String>,
    pub next_row_key: Option<String>,
}

impl EntitiesContinuationToken {
    pub fn new(response: &FlUrlResponse) -> Option<Self> {
        let headers = response.get_headers();

        let next_partition_key =
            if let Some(header) = headers.get("x-ms-continuation-nextpartitionkey") {
                Some(header.to_string())
            } else {
                None
            };

        let next_row_key = if let Some(header) = headers.get("x-ms-continuation-nextrowkey") {
            Some(header.to_string())
        } else {
            None
        };

        if next_partition_key.is_none() && next_row_key.is_none() {
            return None;
        }

        Some(EntitiesContinuationToken {
            next_partition_key,
            next_row_key,
        })
        .into()
    }
}
