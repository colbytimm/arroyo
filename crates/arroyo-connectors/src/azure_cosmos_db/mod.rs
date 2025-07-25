pub mod source;
pub mod sink;
pub mod tests;

use anyhow::{anyhow, bail};
use arroyo_operator::connector::{Connection, Connector, MetadataDef};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::oneshot::{self, Receiver};
use typify::import_types;

use crate::azure_cosmos_db::source::AzureCosmosDbSourceFunc;
use crate::azure_cosmos_db::sink::AzureCosmosDbSinkFunc;
use arrow::datatypes::DataType;

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./azure_cosmos_db.svg");

import_types!(
    schema = "src/azure_cosmos_db/profile.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);

import_types!(schema = "src/azure_cosmos_db/table.json");

pub struct AzureCosmosDbConnector {}

impl AzureCosmosDbConnector {
    pub fn connection_from_options(options: &mut ConnectorOptions) -> anyhow::Result<AzureCosmosDbConfig> {
        Ok(AzureCosmosDbConfig {
            endpoint_url: options.pull_str("endpoint_url")?,
            primary_key: VarStr::new(options.pull_str("primary_key")?),
            database: options.pull_str("database")?,
            container: options.pull_str("container")?,
            throughput: options.pull_opt_u64("throughput")?.map(|t| t as i64),
        })
    }

    pub fn table_from_options(options: &mut ConnectorOptions) -> anyhow::Result<AzureCosmosDbTable> {
        let typ = options.pull_str("type")?;
        let table_type = match typ.as_str() {
            "source" => {
                let start_from = options.pull_opt_str("source.start_from")?;
                AzureCosmosDbTableType::Source {
                    start_from: match start_from.as_deref() {
                        Some("beginning") => StartFrom::Beginning,
                        None | Some("now") => StartFrom::Now,
                        Some(other) => bail!("invalid value for source.start_from '{}'", other),
                    },
                    leases_container: options
                        .pull_opt_str("source.leases_container")?
                        .unwrap_or_else(|| "leases".to_string()),
                    max_items_per_snapshot: options
                        .pull_opt_u64("source.max_items_per_snapshot")?
                        .map(|t| t as i64)
                        .unwrap_or(10000),
                }
            }
            "sink" => {
                AzureCosmosDbTableType::Sink {
                    upsert_mode: options.pull_opt_bool("sink.upsert_mode")?.unwrap_or(true),
                    id_field: options
                        .pull_opt_str("sink.id_field")?
                        .unwrap_or_else(|| "id".to_string()),
                    partition_key_field: options.pull_opt_str("sink.partition_key_field")?,
                    batch_size: options
                        .pull_opt_u64("sink.batch_size")?
                        .map(|t| t as i64)
                        .unwrap_or(25),
                }
            }
            _ => {
                bail!("type must be one of 'source' or 'sink'")
            }
        };

        Ok(AzureCosmosDbTable { r#type: table_type })
    }
}

impl Connector for AzureCosmosDbConnector {
    type ProfileT = AzureCosmosDbConfig;
    type TableT = AzureCosmosDbTable;

    fn name(&self) -> &'static str {
        "azure_cosmos_db"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "azure_cosmos_db".to_string(),
            name: "Azure Cosmos DB".to_string(),
            icon: ICON.to_string(),
            description: "Read from and write to Azure Cosmos DB".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn config_description(&self, config: Self::ProfileT) -> String {
        format!("{}/{}", config.database, config.container)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: AzureCosmosDbConfig,
        table: AzureCosmosDbTable,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let (typ, desc) = match table.r#type {
            AzureCosmosDbTableType::Source { .. } => (
                ConnectionType::Source,
                format!("AzureCosmosDbSource<{}/{}>", config.database, config.container),
            ),
            AzureCosmosDbTableType::Sink { .. } => (
                ConnectionType::Sink,
                format!("AzureCosmosDbSink<{}/{}>", config.database, config.container),
            ),
        };

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("No schema defined for Azure Cosmos DB connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for Azure Cosmos DB connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            typ,
            schema,
            &config,
            desc,
        ))
    }

    fn get_autocomplete(
        &self,
        _profile: Self::ProfileT,
    ) -> oneshot::Receiver<anyhow::Result<HashMap<String, Vec<String>>>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            // For now, return empty autocomplete
            // TODO: Implement container/database listing
            tx.send(Ok(HashMap::new())).unwrap();
        });

        rx
    }

    fn test_profile(&self, _profile: Self::ProfileT) -> Option<Receiver<TestSourceMessage>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            // TODO: Implement connection testing
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Azure Cosmos DB connector test not yet implemented".to_string(),
            };
            tx.send(message).unwrap();
        });

        Some(rx)
    }

    fn test(
        &self,
        _: &str,
        _config: Self::ProfileT,
        _table: Self::TableT,
        _schema: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let resp = TestSourceMessage {
                error: false,
                done: true,
                message: "Azure Cosmos DB connector test not yet implemented".to_string(),
            };
            tx.send(resp).await.unwrap();
        });
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.r#type {
            AzureCosmosDbTableType::Source { .. } => ConnectionType::Source,
            AzureCosmosDbTableType::Sink { .. } => ConnectionType::Sink,
        }
    }

    fn metadata_defs(&self) -> &'static [MetadataDef] {
        &[
            MetadataDef {
                name: "partition_key",
                data_type: DataType::Utf8,
            },
            MetadataDef {
                name: "item_id",
                data_type: DataType::Utf8,
            },
            MetadataDef {
                name: "timestamp",
                data_type: DataType::Int64,
            },
        ]
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let connection = profile
            .map(|p| {
                serde_json::from_value(p.config.clone()).map_err(|e| {
                    anyhow!("invalid config for profile '{}' in database: {}", p.id, e)
                })
            })
            .unwrap_or_else(|| Self::connection_from_options(options))?;

        let table = Self::table_from_options(options)?;

        self.from_config(None, name, connection, table, schema)
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        match &table.r#type {
            AzureCosmosDbTableType::Source {
                start_from,
                leases_container,
                max_items_per_snapshot,
            } => {
                Ok(ConstructedOperator::from_source(Box::new(
                    AzureCosmosDbSourceFunc {
                        endpoint_url: profile.endpoint_url,
                        primary_key: profile.primary_key,
                        database: profile.database,
                        container: profile.container,
                        start_from: *start_from,
                        leases_container: leases_container.clone(),
                        max_items_per_snapshot: *max_items_per_snapshot,
                        format: config.format.expect("Format must be set for Azure Cosmos DB source"),
                        framing: config.framing,
                        bad_data: config.bad_data,
                        metadata_fields: config.metadata_fields,
                    },
                )))
            }
            AzureCosmosDbTableType::Sink {
                upsert_mode,
                id_field,
                partition_key_field,
                batch_size,
            } => {
                Ok(ConstructedOperator::from_operator(Box::new(
                    AzureCosmosDbSinkFunc {
                        endpoint_url: profile.endpoint_url,
                        primary_key: profile.primary_key,
                        database: profile.database,
                        container: profile.container,
                        upsert_mode: *upsert_mode,
                        id_field: id_field.clone(),
                        partition_key_field: partition_key_field.clone(),
                        batch_size: *batch_size,
                        format: config.format.expect("Format must be set for Azure Cosmos DB sink"),
                    },
                )))
            }
        }
    }
}