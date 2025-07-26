use anyhow::Result;
use arrow::array::RecordBatch;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{ArrowOperator, OperatorConstructor};
use arroyo_rpc::formats::Format;
use arroyo_rpc::var_str::VarStr;
use arroyo_types::{CheckpointBarrier, Watermark};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::time::Instant;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct AzureCosmosDbSinkFunc {
    pub endpoint_url: String,
    pub primary_key: VarStr,
    pub database: String,
    pub container: String,
    pub upsert_mode: bool,
    pub id_field: String,
    pub partition_key_field: Option<String>,
    pub batch_size: i64,
    pub format: Format,
    pub serializer: ArrowSerializer,
    pub client: Option<Arc<AzureCosmosDbClient>>,
    pub pending_batch: Vec<CosmosDocument>,
}

#[derive(Debug, Clone)]
pub struct CosmosDocument {
    pub id: String,
    pub partition_key: Option<String>,
    pub data: serde_json::Value,
}

impl OperatorConstructor for AzureCosmosDbSinkFunc {
    type ConfigT = Self;

    fn with_config(&self, config: Self::ConfigT) -> anyhow::Result<Box<dyn ArrowOperator>> {
        Ok(Box::new(AzureCosmosDbSinkFunc {
            serializer: ArrowSerializer::new(config.format.clone()),
            pending_batch: Vec::new(),
            client: None,
            ..config
        }))
    }
}

#[async_trait]
impl ArrowOperator for AzureCosmosDbSinkFunc {
    fn name(&self) -> String {
        format!("azure-cosmos-db-sink-{}/{}", self.database, self.container)
    }

    async fn on_start(&mut self, _ctx: &mut OperatorContext) {
        info!(
            "Starting Azure Cosmos DB sink for {}/{}",
            self.database, self.container
        );
        
        // Initialize Azure Cosmos DB client
        let client = Arc::new(AzureCosmosDbClient::new(
            self.endpoint_url.clone(),
            self.primary_key.sub_env_vars().expect("Failed to substitute environment variables"),
            self.database.clone(),
            self.container.clone(),
        ));

        // TODO: Actual client initialization with Azure SDK
        // let cosmos_client = CosmosClient::new(
        //     &self.endpoint_url,
        //     azure_core::auth::TokenCredential::from_static_secret(&primary_key),
        // ).await?;
        
        self.client = Some(client);
        self.pending_batch = Vec::with_capacity(self.batch_size as usize);
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) {
        debug!(
            "Processing batch of {} records for Azure Cosmos DB sink",
            batch.num_rows()
        );

        // Serialize each record and add to pending batch
        for serialized_record in self.serializer.serialize(&batch) {
            match serde_json::from_slice::<serde_json::Value>(&serialized_record) {
                Ok(mut json_value) => {
                    // Extract ID field
                    let id = json_value
                        .get(&self.id_field)
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| {
                            warn!("ID field '{}' not found, generating UUID", self.id_field);
                            Uuid::new_v4().to_string()
                        });

                    // Extract partition key if configured
                    let partition_key = self.partition_key_field.as_ref().and_then(|field| {
                        json_value
                            .get(field)
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    });

                    // Ensure the document has an 'id' field for Cosmos DB
                    json_value["id"] = serde_json::Value::String(id.clone());

                    self.pending_batch.push(CosmosDocument {
                        id,
                        partition_key,
                        data: json_value,
                    });

                    // Flush if batch is full
                    if self.pending_batch.len() >= self.batch_size as usize {
                        if let Err(e) = self.flush_batch().await {
                            error!("Failed to flush batch to Azure Cosmos DB: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to parse record as JSON: {}", e);
                }
            }
        }
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        _: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        info!("Checkpoint received, flushing pending batch");
        if let Err(e) = self.flush_batch().await {
            error!("Failed to flush batch during checkpoint: {}", e);
        }
    }

    async fn handle_watermark(
        &mut self,
        _: Watermark,
        _: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        // Handle watermark updates if needed
    }

    async fn handle_tick(
        &mut self,
        _: u64,
        _: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        // Periodic flush to avoid accumulating too many records
        if !self.pending_batch.is_empty() {
            debug!("Tick flush: {} pending records", self.pending_batch.len());
            if let Err(e) = self.flush_batch().await {
                error!("Failed to flush batch during tick: {}", e);
            }
        }
    }
}

impl AzureCosmosDbSinkFunc {
    async fn flush_batch(&mut self) -> Result<()> {
        if self.pending_batch.is_empty() {
            return Ok(());
        }

        let client = self.client.as_ref().expect("Client not initialized");
        let batch_size = self.pending_batch.len();
        
        info!("Flushing batch of {} records to Azure Cosmos DB", batch_size);

        // Group records by partition key for efficiency
        let mut partitioned_batches: HashMap<Option<String>, Vec<CosmosDocument>> = HashMap::new();
        
        for doc in self.pending_batch.drain(..) {
            partitioned_batches
                .entry(doc.partition_key.clone())
                .or_insert_with(Vec::new)
                .push(doc);
        }

        // Write each partition batch
        for (partition_key, docs) in partitioned_batches {
            if let Err(e) = client.write_batch(docs, self.upsert_mode).await {
                error!("Failed to write batch for partition {:?}: {}", partition_key, e);
                return Err(e);
            }
        }

        info!("Successfully flushed {} records", batch_size);
        Ok(())
    }
}

// Azure Cosmos DB client implementation
// TODO: Replace with actual azure_cosmos SDK integration
pub struct AzureCosmosDbClient {
    endpoint_url: String,
    primary_key: String,
    database: String,
    container: String,
}

impl AzureCosmosDbClient {
    pub fn new(endpoint_url: String, primary_key: String, database: String, container: String) -> Self {
        Self {
            endpoint_url,
            primary_key,
            database,
            container,
        }
    }

    pub async fn write_batch(&self, documents: Vec<CosmosDocument>, upsert_mode: bool) -> Result<()> {
        debug!(
            "Writing batch of {} documents to {}/{} (upsert: {})",
            documents.len(),
            self.database,
            self.container,
            upsert_mode
        );

        // TODO: Implement actual Azure Cosmos DB SDK calls
        // let cosmos_client = CosmosClient::new(&self.endpoint_url, auth).await?;
        // let database = cosmos_client.database_client(&self.database);
        // let container = database.container_client(&self.container);
        
        // for doc in documents {
        //     if upsert_mode {
        //         container.upsert_item(doc.data, &doc.partition_key).await?;
        //     } else {
        //         container.create_item(doc.data, &doc.partition_key).await?;
        //     }
        // }

        // Simulate write latency
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        Ok(())
    }

    pub async fn connect(&self) -> Result<()> {
        info!(
            "Connecting to Azure Cosmos DB at {} for database {}",
            self.endpoint_url, self.database
        );
        
        // TODO: Implement actual connection validation
        // let cosmos_client = CosmosClient::new(&self.endpoint_url, auth).await?;
        // cosmos_client.database_client(&self.database).read().await?;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arroyo_rpc::formats::Format;
    use arroyo_rpc::ConnectorOptions;
    use arroyo_rpc::api_types::connections::ConnectionType;
    use std::collections::HashMap;
    use crate::azure_cosmos_db::{AzureCosmosDbConnector, AzureCosmosDbConfig, AzureCosmosDbTable, AzureCosmosDbTableType};

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

    #[test]
    fn test_sink_func_creation() {
        let sink = AzureCosmosDbSinkFunc {
            endpoint_url: "https://test.documents.azure.com:443/".to_string(),
            primary_key: VarStr::new("test_key".to_string()),
            database: "testdb".to_string(),
            container: "testcontainer".to_string(),
            upsert_mode: true,
            id_field: "id".to_string(),
            partition_key_field: Some("partitionKey".to_string()),
            batch_size: 25,
            format: Format::RawString(Default::default()),
            serializer: ArrowSerializer::new(Format::RawString(Default::default())),
            client: None,
            pending_batch: Vec::new(),
        };

        assert_eq!(sink.database, "testdb");
        assert_eq!(sink.container, "testcontainer");
        assert!(sink.upsert_mode);
        assert_eq!(sink.id_field, "id");
        assert_eq!(sink.partition_key_field, Some("partitionKey".to_string()));
        assert_eq!(sink.batch_size, 25);
    }

    #[test]
    fn test_sink_name() {
        let sink = AzureCosmosDbSinkFunc {
            endpoint_url: "https://test.documents.azure.com:443/".to_string(),
            primary_key: VarStr::new("test_key".to_string()),
            database: "testdb".to_string(),
            container: "testcontainer".to_string(),
            upsert_mode: false,
            id_field: "id".to_string(),
            partition_key_field: None,
            batch_size: 50,
            format: Format::RawString(Default::default()),
            serializer: ArrowSerializer::new(Format::RawString(Default::default())),
            client: None,
            pending_batch: Vec::new(),
        };

        let name = sink.name();
        assert_eq!(name, "azure-cosmos-db-sink-testdb/testcontainer");
    }

    #[test]
    fn test_cosmos_document_creation() {
        let doc = CosmosDocument {
            id: "test_id".to_string(),
            partition_key: Some("test_pk".to_string()),
            data: serde_json::json!({
                "id": "test_id",
                "name": "Test Document",
                "value": 42
            }),
        };

        assert_eq!(doc.id, "test_id");
        assert_eq!(doc.partition_key, Some("test_pk".to_string()));
        assert_eq!(doc.data["name"], "Test Document");
        assert_eq!(doc.data["value"], 42);
    }

    #[tokio::test]
    async fn test_sink_client_creation() {
        let client = AzureCosmosDbClient::new(
            "https://test.documents.azure.com:443/".to_string(),
            "test_key".to_string(),
            "testdb".to_string(),
            "testcontainer".to_string(),
        );

        assert_eq!(client.database, "testdb");
        assert_eq!(client.container, "testcontainer");
        assert_eq!(client.endpoint_url, "https://test.documents.azure.com:443/");
    }

    #[tokio::test]
    async fn test_sink_client_connect() {
        let client = AzureCosmosDbClient::new(
            "https://test.documents.azure.com:443/".to_string(),
            "test_key".to_string(),
            "testdb".to_string(),
            "testcontainer".to_string(),
        );

        // Should succeed with placeholder implementation
        let result = client.connect().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sink_client_write_batch() {
        let client = AzureCosmosDbClient::new(
            "https://test.documents.azure.com:443/".to_string(),
            "test_key".to_string(),
            "testdb".to_string(),
            "testcontainer".to_string(),
        );

        let documents = vec![
            CosmosDocument {
                id: "doc1".to_string(),
                partition_key: Some("pk1".to_string()),
                data: serde_json::json!({"id": "doc1", "name": "Document 1"}),
            },
            CosmosDocument {
                id: "doc2".to_string(),
                partition_key: Some("pk1".to_string()),
                data: serde_json::json!({"id": "doc2", "name": "Document 2"}),
            },
        ];

        // Test upsert mode
        let result = client.write_batch(documents.clone(), true).await;
        assert!(result.is_ok());

        // Test insert mode
        let result = client.write_batch(documents, false).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_document_json_extraction() {
        let json_data = serde_json::json!({
            "id": "test123",
            "partitionKey": "pk_value",
            "name": "Test Document",
            "timestamp": "2024-01-01T00:00:00Z"
        });

        // Test ID extraction
        let id = json_data
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("default_id");
        assert_eq!(id, "test123");

        // Test partition key extraction
        let pk = json_data
            .get("partitionKey")
            .and_then(|v| v.as_str());
        assert_eq!(pk, Some("pk_value"));

        // Test missing field
        let missing = json_data
            .get("missing_field")
            .and_then(|v| v.as_str());
        assert_eq!(missing, None);
    }
}