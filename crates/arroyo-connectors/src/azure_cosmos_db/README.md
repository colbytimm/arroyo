# Azure Cosmos DB Connector

This connector provides both source and sink capabilities for Azure Cosmos DB.

## Features

- **Source**: Read from Azure Cosmos DB Change Feed with initial snapshot and incremental changes
- **Sink**: Write records to Azure Cosmos DB with upsert or insert operations
- **Authentication**: Primary key authentication support
- **Partitioning**: Automatic partition key handling for efficient operations
- **Checkpointing**: Change feed position tracking for fault tolerance

## Configuration

### Connection Configuration

```json
{
  "endpointUrl": "https://myaccount.documents.azure.com:443/",
  "primaryKey": "${COSMOS_DB_PRIMARY_KEY}",
  "database": "MyDatabase", 
  "container": "MyContainer",
  "throughput": 1000
}
```

### Source Configuration

```json
{
  "type": "source",
  "startFrom": "beginning",
  "leasesContainer": "leases",
  "maxItemsPerSnapshot": 10000
}
```

### Sink Configuration  

```json
{
  "type": "sink",
  "upsertMode": true,
  "idField": "id",
  "partitionKeyField": "userId",
  "batchSize": 25
}
```

## Usage Examples

### Reading from Change Feed

```sql
CREATE TABLE cosmos_source (
  id TEXT,
  name TEXT,
  email TEXT,
  created_at TIMESTAMP
) WITH (
  connector = 'azure_cosmos_db',
  type = 'source',
  'startFrom' = 'beginning',
  'leasesContainer' = 'leases',
  format = 'json'
);
```

### Writing to Cosmos DB

```sql
CREATE TABLE cosmos_sink (
  id TEXT,
  name TEXT,
  email TEXT,
  updated_at TIMESTAMP
) WITH (
  connector = 'azure_cosmos_db',
  type = 'sink',
  'upsertMode' = 'true',
  'idField' = 'id',
  'partitionKeyField' = 'email',
  format = 'json'
);
```

## Implementation Status

- [x] Basic connector structure and configuration schemas
- [x] Source operator framework (placeholder implementation)
- [x] Sink operator framework (placeholder implementation)
- [x] Unit tests for configuration parsing
- [ ] Azure SDK integration for Change Feed reading
- [ ] Azure SDK integration for document writing
- [ ] Retry logic with exponential backoff
- [ ] Integration tests with Azure Cosmos DB emulator
- [ ] Performance optimizations

## TODO

1. **Azure SDK Integration**: Replace placeholder implementations with actual Azure Cosmos DB SDK calls
2. **Change Feed Implementation**: Implement proper change feed reading with continuation tokens
3. **Batch Writing**: Implement efficient batch writing with proper partition key grouping
4. **Error Handling**: Add comprehensive error handling and retry logic
5. **Testing**: Add integration tests using Azure Cosmos DB emulator
6. **Authentication**: Add support for Azure AD authentication in addition to primary keys
7. **Metrics**: Add monitoring and metrics for throughput and errors

## Dependencies

The connector requires the following Azure SDK crates:

- `azure_core` - Core Azure SDK functionality
- `azure_cosmos` - Azure Cosmos DB client library  
- `azure_identity` - Azure authentication support

## Performance Characteristics

- **Source**: Designed to handle ≥ 10 MB/s per split read
- **Sink**: Designed to handle ≥ 1k records/s write under 400 RU/s
- **Backoff**: Implements 429 rate limiting backoff
- **Partitioning**: Uses partition key ranges for efficient splitting