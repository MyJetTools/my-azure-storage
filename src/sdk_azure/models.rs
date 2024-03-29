use my_xml_reader::{MyXmlReader, XmlTagInfo};

use super::azure_response_chunk::AzureResponseChunk;

const ROOT_NODE_NAME: &str = "EnumerationResults";

const CONTAINERS_ARRAY_NODE: &str = "Containers";
const CONTAINER_ARRAY_ITEM_NODE: &str = "Container";

const BLOBS_ARRAY_NODE: &str = "Blobs";
const BLOB_ARRAY_ITEM_NODE: &str = "Blob";

const NEXT_MARKER_NODE: &str = "NextMarker";

pub enum NextMarkerToRead {
    Start,
    Next(String),
    End,
}

fn get_array_of_names<'t>(
    xml_reader: &mut MyXmlReader<'t>,
    array_node: XmlTagInfo<'t>,
    item_node_name: &str,
) -> Vec<String> {
    let mut result = vec![];

    loop {
        let node = xml_reader
            .find_the_node_inside_parent(&array_node, item_node_name)
            .unwrap();

        if node.is_none() {
            break;
        }

        let name_node_open = xml_reader.find_the_open_node("Name").unwrap().unwrap();

        let name_node = xml_reader.read_the_whole_node(name_node_open).unwrap();

        result.push(name_node.get_value().unwrap());
    }

    result
}

pub fn deserialize_list_of_containers(xml: &[u8]) -> AzureResponseChunk<String> {
    let mut xml_reader = MyXmlReader::from_slice(xml).unwrap();

    let root_node = xml_reader
        .find_the_open_node(ROOT_NODE_NAME)
        .unwrap()
        .unwrap();

    let mut blobs: Option<Vec<String>> = None;

    let mut next_marker: Option<String> = None;

    loop {
        let open_node = xml_reader
            .find_any_of_these_nodes_inside_parent(
                &root_node,
                vec![CONTAINERS_ARRAY_NODE, NEXT_MARKER_NODE].as_slice(),
            )
            .unwrap();

        if open_node.is_none() {
            break;
        }

        let open_node = open_node.unwrap();

        match open_node.name {
            NEXT_MARKER_NODE => {
                let next_marker_node = xml_reader.read_the_whole_node(open_node).unwrap();
                next_marker = next_marker_node.get_value();
            }
            CONTAINERS_ARRAY_NODE => {
                blobs = Some(get_array_of_names(
                    &mut xml_reader,
                    open_node,
                    CONTAINER_ARRAY_ITEM_NODE,
                ))
            }
            _ => {}
        }
    }

    return AzureResponseChunk {
        next_marker,
        items: blobs.unwrap(),
    };
}

pub fn deserialize_list_of_blobs(xml: &[u8]) -> AzureResponseChunk<String> {
    let mut xml_reader = MyXmlReader::from_slice(xml).unwrap();

    let root_node = xml_reader
        .find_the_open_node(ROOT_NODE_NAME)
        .unwrap()
        .unwrap();

    let mut containers: Option<Vec<String>> = None;

    let mut next_marker: Option<String> = None;

    loop {
        let open_node = xml_reader
            .find_any_of_these_nodes_inside_parent(
                &root_node,
                vec![BLOBS_ARRAY_NODE, NEXT_MARKER_NODE].as_slice(),
            )
            .unwrap();

        if open_node.is_none() {
            break;
        }

        let open_node = open_node.unwrap();

        match open_node.name {
            NEXT_MARKER_NODE => {
                let next_marker_node = xml_reader.read_the_whole_node(open_node).unwrap();
                next_marker = next_marker_node.get_value();
            }
            BLOBS_ARRAY_NODE => {
                containers = Some(get_array_of_names(
                    &mut xml_reader,
                    open_node,
                    BLOB_ARRAY_ITEM_NODE,
                ))
            }
            _ => {}
        }
    }

    return AzureResponseChunk {
        next_marker,
        items: containers.unwrap(),
    };
}
