# Glue

**Protocol:** JSON 1.1
**Endpoint:** `http://localhost:4566/`

Floci emulates the AWS Glue Data Catalog, Glue Schema Registry, and a local Glue Jobs control plane with Docker-backed job execution.

## Supported Actions

### Data Catalog

| Area | Actions |
|---|---|
| Databases | `CreateDatabase` · `GetDatabase` · `GetDatabases` · `DeleteDatabase` |
| Tables | `CreateTable` · `GetTable` · `GetTables` · `UpdateTable` · `DeleteTable` · `BatchDeleteTable` · `GetTableVersions` |
| Partitions | `CreatePartition` · `GetPartitions` |
| User-defined functions | `CreateUserDefinedFunction` · `GetUserDefinedFunction` · `GetUserDefinedFunctions` · `UpdateUserDefinedFunction` · `DeleteUserDefinedFunction` |

### Jobs

| Area | Actions |
|---|---|
| Jobs | `CreateJob` · `GetJob` · `GetJobs` · `UpdateJob` · `DeleteJob` |
| Runs | `StartJobRun` · `GetJobRun` · `GetJobRuns` · `BatchStopJobRun` |

### Schema Registry

| Area | Actions |
|---|---|
| Registries | `CreateRegistry` · `GetRegistry` · `ListRegistries` · `UpdateRegistry` · `DeleteRegistry` |
| Schemas | `CreateSchema` · `GetSchema` · `ListSchemas` · `UpdateSchema` · `DeleteSchema` |
| Versions | `RegisterSchemaVersion` · `GetSchemaByDefinition` · `GetSchemaVersion` · `ListSchemaVersions` · `DeleteSchemaVersions` · `GetSchemaVersionsDiff` · `CheckSchemaVersionValidity` |
| Metadata and tags | `PutSchemaVersionMetadata` · `RemoveSchemaVersionMetadata` · `QuerySchemaVersionMetadata` · `TagResource` · `UntagResource` · `GetTags` |

Supported schema formats are `AVRO`, `JSON`, and `PROTOBUF`. Compatibility modes are `NONE`, `DISABLED`, `BACKWARD`, `BACKWARD_ALL`, `FORWARD`, `FORWARD_ALL`, `FULL`, and `FULL_ALL`.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `FLOCI_SERVICES_GLUE_ENABLED` | `true` | Enable or disable the service |
| `FLOCI_SERVICES_GLUE_MOCK` | `false` | Complete job runs immediately without starting Docker containers |
| `FLOCI_SERVICES_GLUE_DEFAULT_JOB_IMAGE` | `public.ecr.aws/glue/aws-glue-libs:5` | Docker image used for local Glue job runs |
| `FLOCI_SERVICES_GLUE_GLUE4_JOB_IMAGE` | `public.ecr.aws/glue/aws-glue-libs:glue_libs_4.0.0_image_01` | Docker image used when `GlueVersion` starts with `4` |
| `FLOCI_SERVICES_GLUE_GLUE5_JOB_IMAGE` | `public.ecr.aws/glue/aws-glue-libs:5` | Docker image used when `GlueVersion` starts with `5` |
| `FLOCI_SERVICES_GLUE_DOCKER_NETWORK` | *(unset)* | Docker network for Glue job containers; falls back to the shared service Docker network |

## Glue Jobs

`StartJobRun` creates a job run record and launches a one-shot Docker container. Scripts can be referenced by `Command.ScriptLocation` as either `s3://bucket/key`, `file:///absolute/path`, or an absolute local path readable by the Floci process. For S3 script locations, Floci reads the object from the emulated S3 service and copies it into the job container before starting it.

If a job sets `GlueVersion` to `4.0`, Floci uses the Glue 4 image. If it sets `GlueVersion` to `5.0`, Floci uses the Glue 5 image. Jobs without `GlueVersion` use `FLOCI_SERVICES_GLUE_DEFAULT_JOB_IMAGE`.

The runner sets standard local AWS environment variables such as `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`. It uses `python3` or `python` for Python shell commands and otherwise discovers `spark-submit` or `gluesparksubmit` in either the Glue 5 `hadoop` layout or the Glue 4 `glue_user` Spark layout.

For Spark commands, the runner injects `fs.s3a.*` configuration pointing at the local S3 emulator (endpoint, path-style access, dummy credentials), so job scripts can read and write `s3a://` paths without configuring Hadoop settings in the job script. When the job's arguments include `--datalake-formats` containing `iceberg`, the runner additionally enables the Iceberg Spark session extensions and adds any Iceberg runtime jars found under `/usr/share/aws/datalake-formats/iceberg` in the image, mirroring the `--datalake-formats` behavior of managed Glue.

Docker stdout/stderr is forwarded to the local CloudWatch Logs emulator under `/aws-glue/jobs/output`, with streams named `<job-name>/<job-run-id>`. The `JobRun` response also includes `LogGroupName` and `LogStreamName`. `MaxRetries` is honored by starting a fresh one-shot container for each retry attempt and updating the `Attempt` field; unlike real Glue, retries reuse the original job run id instead of creating a separate run per attempt.

`UpdateJob` follows AWS replace semantics: fields omitted from `JobUpdate` are reset to their defaults rather than preserved. `GetJob` echoes the job's `Tags`, which real Glue only returns through `GetTags`.

This is a local execution shim, not a complete managed Glue runtime. Features such as Glue job bookmarks, Glue Studio transforms, IAM enforcement, and exact worker provisioning semantics are outside the scope of local emulation.

### Glue Image Integration Tests

The standard test suite does not pull or run the multi-GB AWS Glue Docker images. To verify that the configured Glue 4 and Glue 5 images run both Python shell and Spark commands through the same runner discovery Floci uses for job containers, run:

```bash
FLOCI_GLUE_DOCKER_IMAGE_TEST=true ./mvnw -Dtest=GlueDockerImageIntegrationTest test
```

### Job Example

```bash
export AWS_ENDPOINT_URL=http://localhost:4566

aws s3 mb s3://scripts --endpoint-url $AWS_ENDPOINT_URL
aws s3 cp ./jobs/orders_etl.py s3://scripts/orders_etl.py --endpoint-url $AWS_ENDPOINT_URL

aws glue create-job \
  --name orders-etl \
  --role arn:aws:iam::000000000000:role/glue-role \
  --glue-version 5.0 \
  --command Name=glueetl,ScriptLocation=s3://scripts/orders_etl.py \
  --endpoint-url $AWS_ENDPOINT_URL

RUN_ID=$(aws glue start-job-run \
  --job-name orders-etl \
  --arguments '{"--source":"s3://my-data-lake/orders/"}' \
  --query JobRunId --output text \
  --endpoint-url $AWS_ENDPOINT_URL)

aws glue get-job-run \
  --job-name orders-etl \
  --run-id "$RUN_ID" \
  --endpoint-url $AWS_ENDPOINT_URL
```

## Integration with Athena

The Glue Data Catalog is automatically used by **Athena** to resolve table names to S3 locations and formats. When you submit an Athena query, Floci reads all Glue tables for the target database and generates DuckDB views on top of the underlying S3 objects before executing the SQL.

Tables can reference a Schema Registry schema version through `StorageDescriptor.SchemaReference`. On `GetTable` and `GetTables`, Floci resolves the schema definition into Glue columns when possible.

The DuckDB read function is selected based on the table's parameters, `StorageDescriptor.InputFormat`, and `StorageDescriptor.SerdeInfo.SerializationLibrary`:

| Condition | DuckDB function |
|---|---|
| Table parameter `table_type` is `ICEBERG` | `iceberg_scan` |
| `InputFormat` or `SerializationLibrary` contains `parquet` | `read_parquet` |
| `InputFormat` or `SerializationLibrary` contains `json` | `read_json_auto` |
| `InputFormat` contains `hive` | `read_json_auto` |
| Anything else | `read_csv_auto` |

## Data Catalog Example

```bash
export AWS_ENDPOINT_URL=http://localhost:4566

# Create a database
aws glue create-database \
  --database-input '{"Name": "analytics"}' \
  --endpoint-url $AWS_ENDPOINT_URL

# Create a JSON table (standard AWS format for NDJSON data)
aws glue create-table \
  --database-name analytics \
  --table-input '{
    "Name": "orders",
    "StorageDescriptor": {
      "Location": "s3://my-bucket/orders/",
      "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
      "SerdeInfo": {
        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"
      },
      "Columns": [
        {"Name": "id",     "Type": "int"},
        {"Name": "amount", "Type": "double"}
      ]
    }
  }' \
  --endpoint-url $AWS_ENDPOINT_URL

# Create a Parquet table
aws glue create-table \
  --database-name analytics \
  --table-input '{
    "Name": "events",
    "StorageDescriptor": {
      "Location": "s3://my-bucket/events/",
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "SerdeInfo": {
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      },
      "Columns": [
        {"Name": "event_id", "Type": "string"},
        {"Name": "ts",       "Type": "bigint"}
      ]
    }
  }' \
  --endpoint-url $AWS_ENDPOINT_URL
```

## Schema Registry Example

```bash
export AWS_ENDPOINT_URL=http://localhost:4566

cat > /tmp/order.avsc <<'JSON'
{"type":"record","name":"Order","namespace":"example","fields":[{"name":"id","type":"long"}]}
JSON

cat > /tmp/order-v2.avsc <<'JSON'
{"type":"record","name":"Order","namespace":"example","fields":[{"name":"id","type":"long"},{"name":"amount","type":["null","double"],"default":null}]}
JSON

aws glue create-registry \
  --registry-name local-registry \
  --endpoint-url $AWS_ENDPOINT_URL

aws glue create-schema \
  --registry-id RegistryName=local-registry \
  --schema-name orders \
  --data-format AVRO \
  --compatibility BACKWARD \
  --schema-definition file:///tmp/order.avsc \
  --endpoint-url $AWS_ENDPOINT_URL

aws glue register-schema-version \
  --schema-id RegistryName=local-registry,SchemaName=orders \
  --schema-definition file:///tmp/order-v2.avsc \
  --endpoint-url $AWS_ENDPOINT_URL

aws glue list-schema-versions \
  --schema-id RegistryName=local-registry,SchemaName=orders \
  --endpoint-url $AWS_ENDPOINT_URL
```
