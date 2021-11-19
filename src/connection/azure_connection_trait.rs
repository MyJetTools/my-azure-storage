use super::AzureConnectionInfo;

pub trait GetAzureConnectionInfo<'s> {
    fn get_connection_info(&'s self) -> &'s AzureConnectionInfo;
}
