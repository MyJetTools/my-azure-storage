const ROOT_URL: &str = "azure-storage-tests";

pub fn get_test_folder() -> String {
    let folder_separator = std::path::MAIN_SEPARATOR;
    let folder = format!(
        "{home}{folder_separator}{ROOT_URL}{folder_separator}",
        home = env!("HOME"),
    );

    folder
}
