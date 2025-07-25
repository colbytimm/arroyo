#[cfg(test)]
mod tests {
    use super::*;
    use arroyo_rpc::ConnectorOptions;
    use std::collections::HashMap;

    #[test]
    fn test_connection_from_options() {
        let mut options = ConnectorOptions::new(HashMap::from([
            ("endpoint_url".to_string(), "https://test.documents.azure.com:443/".to_string()),
            ("primary_key".to_string(), "test_key".to_string()),
            ("database".to_string(), "testdb".to_string()),
            ("container".to_string(), "testcontainer".to_string()),
            ("throughput".to_string(), "1000".to_string()),
        ]));

        let config = AzureCosmosDbConnector::connection_from_options(&mut options).unwrap();
        
        assert_eq!(config.endpoint_url, "https://test.documents.azure.com:443/");
        assert_eq!(config.database, "testdb");
        assert_eq!(config.container, "testcontainer");
        assert_eq!(config.throughput, Some(1000));
    }

    #[test]
    fn test_table_from_options_source() {
        let mut options = ConnectorOptions::new(HashMap::from([
            ("type".to_string(), "source".to_string()),
            ("source.start_from".to_string(), "beginning".to_string()),
            ("source.leases_container".to_string(), "custom_leases".to_string()),
            ("source.max_items_per_snapshot".to_string(), "5000".to_string()),
        ]));

        let table = AzureCosmosDbConnector::table_from_options(&mut options).unwrap();
        
        match table.r#type {
            AzureCosmosDbTableType::Source { start_from, leases_container, max_items_per_snapshot } => {
                assert!(matches!(start_from, StartFrom::Beginning));
                assert_eq!(leases_container, "custom_leases");
                assert_eq!(max_items_per_snapshot, 5000);
            },
            _ => panic!("Expected source table type"),
        }
    }

    #[test]
    fn test_table_from_options_sink() {
        let mut options = ConnectorOptions::new(HashMap::from([
            ("type".to_string(), "sink".to_string()),
            ("sink.upsert_mode".to_string(), "false".to_string()),
            ("sink.id_field".to_string(), "custom_id".to_string()),
            ("sink.partition_key_field".to_string(), "pk".to_string()),
            ("sink.batch_size".to_string(), "50".to_string()),
        ]));

        let table = AzureCosmosDbConnector::table_from_options(&mut options).unwrap();
        
        match table.r#type {
            AzureCosmosDbTableType::Sink { upsert_mode, id_field, partition_key_field, batch_size } => {
                assert!(!upsert_mode);
                assert_eq!(id_field, "custom_id");
                assert_eq!(partition_key_field, Some("pk".to_string()));
                assert_eq!(batch_size, 50);
            },
            _ => panic!("Expected sink table type"),
        }
    }

    #[test]
    fn test_connector_metadata() {
        let connector = AzureCosmosDbConnector {};
        let metadata = connector.metadata();
        
        assert_eq!(metadata.id, "azure_cosmos_db");
        assert_eq!(metadata.name, "Azure Cosmos DB");
        assert!(metadata.source);
        assert!(metadata.sink);
        assert!(metadata.enabled);
        assert!(metadata.testing);
    }

    #[test]
    fn test_connector_name() {
        let connector = AzureCosmosDbConnector {};
        assert_eq!(connector.name(), "azure_cosmos_db");
    }

    #[test]
    fn test_config_description() {
        let connector = AzureCosmosDbConnector {};
        let config = AzureCosmosDbConfig {
            endpoint_url: "https://test.documents.azure.com:443/".to_string(),
            primary_key: VarStr::new("test_key".to_string()),
            database: "testdb".to_string(),
            container: "testcontainer".to_string(),
            throughput: Some(1000),
        };
        
        let description = connector.config_description(config);
        assert_eq!(description, "testdb/testcontainer");
    }

    #[test]
    fn test_table_type_source() {
        let connector = AzureCosmosDbConnector {};
        let config = AzureCosmosDbConfig {
            endpoint_url: "https://test.documents.azure.com:443/".to_string(),
            primary_key: VarStr::new("test_key".to_string()),
            database: "testdb".to_string(),
            container: "testcontainer".to_string(),
            throughput: None,
        };
        let table = AzureCosmosDbTable {
            r#type: AzureCosmosDbTableType::Source {
                start_from: StartFrom::Now,
                leases_container: "leases".to_string(),
                max_items_per_snapshot: 10000,
            },
        };
        
        let table_type = connector.table_type(config, table);
        assert_eq!(table_type, ConnectionType::Source);
    }

    #[test]
    fn test_table_type_sink() {
        let connector = AzureCosmosDbConnector {};
        let config = AzureCosmosDbConfig {
            endpoint_url: "https://test.documents.azure.com:443/".to_string(),
            primary_key: VarStr::new("test_key".to_string()),
            database: "testdb".to_string(),
            container: "testcontainer".to_string(),
            throughput: None,
        };
        let table = AzureCosmosDbTable {
            r#type: AzureCosmosDbTableType::Sink {
                upsert_mode: true,
                id_field: "id".to_string(),
                partition_key_field: None,
                batch_size: 25,
            },
        };
        
        let table_type = connector.table_type(config, table);
        assert_eq!(table_type, ConnectionType::Sink);
    }
}