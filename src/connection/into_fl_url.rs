use flurl::FlUrl;

use super::AzureStorageConnectionData;

impl Into<FlUrl> for &AzureStorageConnectionData {
    fn into(self) -> FlUrl {
        let fl_url = FlUrl::new(self.blobs_api_url.as_str(), None);
        return fl_url;
    }
}
