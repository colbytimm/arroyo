use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use arroyo_formats::de::ArrowDeserializer;
use arroyo_operator::context::{Collector, OperatorContext, SourceContext};
use arroyo_operator::operator::{ArrowOperator, OperatorConstructor, SourceOperator};
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::TableConfig;
use arroyo_rpc::var_str::VarStr;
use arroyo_types::{CheckpointBarrier, SourceError, Watermark};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{interval, Instant};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::{StartFrom};

#[derive(Debug, Clone)]
pub struct AzureCosmosDbSourceFunc {
    pub endpoint_url: String,
    pub primary_key: VarStr,
    pub database: String,
    pub container: String,
    pub start_from: StartFrom,
    pub leases_container: String,
    pub max_items_per_snapshot: i64,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub metadata_fields: Vec<String>,
    pub client: Option<Arc<AzureCosmosDbSourceClient>>,
    pub continuation_token: Option<String>,
    pub deserializer: Option<ArrowDeserializer>,
}

impl OperatorConstructor for AzureCosmosDbSourceFunc {
    type ConfigT = Self;

    fn with_config(&self, config: Self::ConfigT) -> anyhow::Result<Box<dyn ArrowOperator>> {
        Ok(Box::new(AzureCosmosDbSourceFunc {
            client: None,
            continuation_token: None,
            deserializer: None,
            ..config
        }))
    }
}

#[async_trait]
impl ArrowOperator for AzureCosmosDbSourceFunc {
    fn name(&self) -> String {
        format!("azure-cosmos-db-source-{}/{}", self.database, self.container)
    }

    async fn on_start(&mut self, _ctx: &mut OperatorContext) {
        info!(
            "Starting Azure Cosmos DB source for {}/{}",
            self.database, self.container
        );

        // Initialize Azure Cosmos DB client
        let client = Arc::new(AzureCosmosDbSourceClient::new(
            self.endpoint_url.clone(),
            self.primary_key.sub_env_vars().expect("Failed to substitute environment variables"),
            self.database.clone(),
            self.container.clone(),
            self.leases_container.clone(),
        ));

        self.client = Some(client);

        // TODO: Initialize deserializer with proper schema
        // let schema = ctx.get_schema();
        // self.deserializer = Some(ArrowDeserializer::new(
        //     self.format.clone(),
        //     schema,
        //     &self.metadata_fields,
        //     self.framing.clone(),
        //     self.bad_data.clone().unwrap_or_default(),
        // ));
    }

    async fn process_batch(&mut self, _: RecordBatch, _ctx: &mut OperatorContext) {
        // This is a source operator, so we don't process input batches
    }

    async fn handle_checkpoint(&mut self, _: CheckpointBarrier, _: &mut OperatorContext) {
        // Save current change feed position/continuation token
        if let Some(token) = &self.continuation_token {
            debug!("Checkpointing continuation token: {}", token);
            // TODO: Persist continuation token to lease container
        }
    }

    async fn handle_watermark(&mut self, _: Watermark, _: &mut OperatorContext) {
        // Handle watermark updates
    }
}

#[async_trait]
impl SourceOperator for AzureCosmosDbSourceFunc {
    async fn run(&mut self, ctx: &mut SourceContext) -> SourceFinishType {
        info!(
            "Azure Cosmos DB source starting for {}/{}",
            self.database, self.container
        );

        let client = self.client.as_ref().expect("Client not initialized");

        // Initialize change feed reading
        match client.initialize_change_feed(self.start_from).await {
            Ok(initial_token) => {
                self.continuation_token = initial_token;
                info!("Change feed initialized successfully");
            }
            Err(e) => {
                error!("Failed to initialize change feed: {}", e);
                return SourceFinishType::Immediate;
            }
        }

        // Main reading loop
        let mut poll_interval = interval(Duration::from_millis(500));
        let mut consecutive_empty_reads = 0;
        const MAX_EMPTY_READS: u32 = 10;

        loop {
            poll_interval.tick().await;

            // Check if we should stop
            if ctx.should_stop() {
                info!("Azure Cosmos DB source received stop signal");
                break;
            }

            // Read changes from change feed
            match client.read_change_feed(&self.continuation_token).await {
                Ok(change_result) => {
                    if change_result.documents.is_empty() {
                        consecutive_empty_reads += 1;
                        debug!("No changes available, empty read #{}", consecutive_empty_reads);
                        
                        // If we've had many empty reads, increase polling interval
                        if consecutive_empty_reads >= MAX_EMPTY_READS {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            consecutive_empty_reads = 0;
                        }
                        continue;
                    }

                    consecutive_empty_reads = 0;
                    self.continuation_token = change_result.continuation_token;

                    info!("Read {} changes from Cosmos DB", change_result.documents.len());

                    // Process each document
                    for document in change_result.documents {
                        // TODO: Deserialize document and emit to downstream
                        // let serialized = serde_json::to_vec(&document)?;
                        // if let Some(batch) = self.deserializer.as_mut().unwrap()
                        //     .deserialize_slice(&serialized, SystemTime::now(), None)
                        //     .await? 
                        // {
                        //     ctx.collect(batch).await;
                        // }
                        
                        debug!("Processed document with id: {}", document.get("id").unwrap_or(&serde_json::Value::Null));
                    }
                }
                Err(e) => {
                    error!("Failed to read from change feed: {}", e);
                    
                    // Implement exponential backoff for retries
                    let backoff_duration = Duration::from_millis(1000 * (consecutive_empty_reads + 1) as u64);
                    warn!("Backing off for {:?} before retry", backoff_duration);
                    tokio::time::sleep(backoff_duration).await;
                    
                    consecutive_empty_reads += 1;
                    if consecutive_empty_reads > 20 {
                        error!("Too many consecutive failures, stopping source");
                        break;
                    }
                }
            }
        }

        SourceFinishType::Final
    }

    async fn handle_checkpoint(&mut self, _: CheckpointBarrier, _: &mut SourceContext) {
        // Save current change feed position/continuation token
        if let Some(token) = &self.continuation_token {
            debug!("Saving continuation token to checkpoint: {}", token);
            // TODO: Persist to lease container or checkpoint storage
        }
    }
}

#[derive(Debug)]
pub struct ChangeFeedResult {
    pub documents: Vec<serde_json::Value>,
    pub continuation_token: Option<String>,
}

// Azure Cosmos DB source client
// TODO: Replace with actual azure_cosmos SDK integration
pub struct AzureCosmosDbSourceClient {
    endpoint_url: String,
    primary_key: String,
    database: String,
    container: String,
    leases_container: String,
}

impl AzureCosmosDbSourceClient {
    pub fn new(
        endpoint_url: String,
        primary_key: String,
        database: String,
        container: String,
        leases_container: String,
    ) -> Self {
        Self {
            endpoint_url,
            primary_key,
            database,
            container,
            leases_container,
        }
    }

    pub async fn initialize_change_feed(&self, start_from: StartFrom) -> Result<Option<String>> {
        info!(
            "Initializing change feed for {}/{} starting from {:?}",
            self.database, self.container, start_from
        );

        // TODO: Implement actual Azure Cosmos DB change feed initialization
        // let cosmos_client = CosmosClient::new(&self.endpoint_url, auth).await?;
        // let database = cosmos_client.database_client(&self.database);
        // let container = database.container_client(&self.container);
        
        // let change_feed_options = ChangeFeedOptions::new()
        //     .start_from(match start_from {
        //         StartFrom::Beginning => ChangeFeedStartFrom::Beginning,
        //         StartFrom::Now => ChangeFeedStartFrom::Now,
        //     });
        
        // let change_feed = container.change_feed(change_feed_options).await?;
        // Ok(change_feed.continuation_token())

        // For now, return a dummy token based on start position
        let initial_token = match start_from {
            StartFrom::Beginning => Some("beginning".to_string()),
            StartFrom::Now => Some(format!("now-{}", chrono::Utc::now().timestamp())),
        };

        Ok(initial_token)
    }

    pub async fn read_change_feed(&self, continuation_token: &Option<String>) -> Result<ChangeFeedResult> {
        debug!(
            "Reading change feed from token: {:?}",
            continuation_token
        );

        // TODO: Implement actual change feed reading
        // let cosmos_client = CosmosClient::new(&self.endpoint_url, auth).await?;
        // let database = cosmos_client.database_client(&self.database);
        // let container = database.container_client(&self.container);
        
        // let mut change_feed = container.change_feed(
        //     ChangeFeedOptions::new().continuation_token(continuation_token.clone())
        // ).await?;
        
        // let documents = change_feed.read_next().await?;
        // let new_token = change_feed.continuation_token();

        // Simulate change feed reading with empty result most of the time
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Return empty result for now
        Ok(ChangeFeedResult {
            documents: vec![],
            continuation_token: continuation_token.clone(),
        })
    }

    pub async fn connect(&self) -> Result<()> {
        info!(
            "Connecting to Azure Cosmos DB at {} for database {}",
            self.endpoint_url, self.database
        );
        
        // TODO: Implement actual connection validation
        // let cosmos_client = CosmosClient::new(&self.endpoint_url, auth).await?;
        // cosmos_client.database_client(&self.database).read().await?;
        // cosmos_client.database_client(&self.database)
        //     .container_client(&self.container).read().await?;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arroyo_rpc::formats::Format;

    #[test]
    fn test_source_func_creation() {
        let source = AzureCosmosDbSourceFunc {
            endpoint_url: "https://test.documents.azure.com:443/".to_string(),
            primary_key: VarStr::new("test_key".to_string()),
            database: "testdb".to_string(),
            container: "testcontainer".to_string(),
            start_from: StartFrom::Beginning,
            leases_container: "leases".to_string(),
            max_items_per_snapshot: 1000,
            format: Format::RawString(Default::default()),
            framing: None,
            bad_data: None,
            metadata_fields: vec![],
            client: None,
            continuation_token: None,
            deserializer: None,
        };

        assert_eq!(source.database, "testdb");
        assert_eq!(source.container, "testcontainer");
        assert!(matches!(source.start_from, StartFrom::Beginning));
    }

    #[test]
    fn test_source_name() {
        let source = AzureCosmosDbSourceFunc {
            endpoint_url: "https://test.documents.azure.com:443/".to_string(),
            primary_key: VarStr::new("test_key".to_string()),
            database: "testdb".to_string(),
            container: "testcontainer".to_string(),
            start_from: StartFrom::Now,
            leases_container: "leases".to_string(),
            max_items_per_snapshot: 1000,
            format: Format::RawString(Default::default()),
            framing: None,
            bad_data: None,
            metadata_fields: vec![],
            client: None,
            continuation_token: None,
            deserializer: None,
        };

        let name = source.name();
        assert_eq!(name, "azure-cosmos-db-source-testdb/testcontainer");
    }

    #[tokio::test]
    async fn test_source_client_creation() {
        let client = AzureCosmosDbSourceClient::new(
            "https://test.documents.azure.com:443/".to_string(),
            "test_key".to_string(),
            "testdb".to_string(),
            "testcontainer".to_string(),
            "leases".to_string(),
        );

        assert_eq!(client.database, "testdb");
        assert_eq!(client.container, "testcontainer");
        assert_eq!(client.leases_container, "leases");
    }

    #[tokio::test]
    async fn test_change_feed_initialization() {
        let client = AzureCosmosDbSourceClient::new(
            "https://test.documents.azure.com:443/".to_string(),
            "test_key".to_string(),
            "testdb".to_string(),
            "testcontainer".to_string(),
            "leases".to_string(),
        );

        // Test initialization from beginning
        let result = client.initialize_change_feed(StartFrom::Beginning).await;
        assert!(result.is_ok());
        let token = result.unwrap();
        assert_eq!(token, Some("beginning".to_string()));

        // Test initialization from now
        let result = client.initialize_change_feed(StartFrom::Now).await;
        assert!(result.is_ok());
        let token = result.unwrap();
        assert!(token.is_some());
        assert!(token.unwrap().starts_with("now-"));
    }

    #[tokio::test]
    async fn test_change_feed_reading() {
        let client = AzureCosmosDbSourceClient::new(
            "https://test.documents.azure.com:443/".to_string(),
            "test_key".to_string(),
            "testdb".to_string(),
            "testcontainer".to_string(),
            "leases".to_string(),
        );

        let token = Some("test_token".to_string());
        let result = client.read_change_feed(&token).await;
        
        assert!(result.is_ok());
        let change_result = result.unwrap();
        
        // Currently returns empty documents (placeholder implementation)
        assert!(change_result.documents.is_empty());
        assert_eq!(change_result.continuation_token, token);
    }
}