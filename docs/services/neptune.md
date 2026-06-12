# Neptune

**Protocol:** Query (XML) for management API + Gremlin / HTTP for data plane
**Management Endpoint:** `POST http://localhost:4566/`
**Data Endpoint:** `localhost:<proxy-port>` (TCP / WebSocket)

Floci manages real [Apache TinkerPop Gremlin Server](https://tinkerpop.apache.org/) Docker containers and proxies connections to them, providing an API-compatible Neptune emulation for local development and testing.

## Supported Actions

| Action | Description |
|--------|-------------|
| `CreateDBCluster` | Create a Neptune cluster and start a Gremlin Server container |
| `DescribeDBClusters` | List clusters and their connection details |
| `DeleteDBCluster` | Stop and remove a cluster |
| `ModifyDBCluster` | Update cluster settings |
| `CreateDBInstance` | Add an instance to a cluster |
| `DescribeDBInstances` | List instances |
| `DeleteDBInstance` | Remove an instance from a cluster |
| `ModifyDBInstance` | Update instance settings |

Each cluster's Gremlin endpoint also serves the Neptune bulk loader HTTP API (`POST /loader`, `GET /loader/{loadId}`) — see [Bulk loader](#bulk-loader-s3--neptune).

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FLOCI_SERVICES_NEPTUNE_ENABLED` | `true` | Enable or disable Neptune |
| `FLOCI_SERVICES_NEPTUNE_PROXY_BASE_PORT` | `8182` | First host port in the Gremlin proxy range |
| `FLOCI_SERVICES_NEPTUNE_PROXY_MAX_PORT` | `8282` | Last host port in the Gremlin proxy range |
| `FLOCI_SERVICES_NEPTUNE_DEFAULT_IMAGE` | `tinkerpop/gremlin-server:3.7.3` | Gremlin Server Docker image |
| `FLOCI_SERVICES_NEPTUNE_DOCKER_NETWORK` | _(host default)_ | Docker network for container connectivity |

### Docker Compose

Neptune requires the Docker socket and the Gremlin proxy port range to be exposed. The first cluster claims `PROXY_BASE_PORT`; each additional cluster increments the port.

```yaml
services:
  floci:
    image: floci/floci:latest
    ports:
      - "4566:4566"
      - "8182-8282:8182-8282"   # Neptune Gremlin proxy ports
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      FLOCI_SERVICES_DOCKER_NETWORK: my-project_default
```

For private registry authentication and other Docker settings see [Docker Configuration](../configuration/docker.md).

## Examples

### Management API (AWS CLI)

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

# Create a Neptune cluster
aws neptune create-db-cluster \
  --db-cluster-identifier my-neptune \
  --engine neptune

# Get cluster details and Gremlin endpoint port
aws neptune describe-db-clusters \
  --db-cluster-identifier my-neptune \
  --query 'DBClusters[0].{Endpoint:Endpoint,Port:Port}'

# Create an instance in the cluster
aws neptune create-db-instance \
  --db-instance-identifier my-neptune-instance \
  --db-cluster-identifier my-neptune \
  --db-instance-class db.r5.large \
  --engine neptune

# Delete instance and cluster
aws neptune delete-db-instance \
  --db-instance-identifier my-neptune-instance
aws neptune delete-db-cluster \
  --db-cluster-identifier my-neptune \
  --skip-final-snapshot
```

### Graph data plane (Python + gremlin-python)

```python
from gremlin_python.driver import client, serializer

# Use the port returned by DescribeDBClusters
gremlin = client.Client(
    "ws://localhost:8182/gremlin",
    "g",
    message_serializer=serializer.GraphSONSerializersV2d0(),
)

# Add a vertex
gremlin.submit("g.addV('person').property('name', 'Alice')").all().result()

# Query vertices
result = gremlin.submit("g.V().valueMap(true)").all().result()
print(result)

gremlin.close()
```

### Bulk loader (S3 → Neptune)

Each cluster's Gremlin endpoint also serves the Neptune bulk loader HTTP API. `POST /loader` reads [Gremlin load data format](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html) CSV files from the emulated S3 service and writes them to the cluster; `GET /loader/{loadId}` reports progress. Vertex files in a load are applied before edge files. Only `format: csv` is supported, and `iamRoleArn` is accepted but ignored.

```bash
# Upload Gremlin CSV files
printf '~id,~label,name:String\nc1,customer,Alice\n' | aws s3 cp - s3://graph/vertices.csv
printf '~id,~from,~to,~label\ne1,c1,c1,KNOWS\n' | aws s3 cp - s3://graph/edges.csv

# Start the load (port from DescribeDBClusters)
curl -s -X POST http://localhost:8182/loader \
  -H 'Content-Type: application/json' \
  -d '{"source": "s3://graph/", "format": "csv"}'

# Check load status
curl -s http://localhost:8182/loader/<loadId>
```

Vertices and edges keep the ids from the `~id`, `~from`, and `~to` columns, so loaded data can be queried with `g.V('c1')` just like on Neptune.

### Management API (Python / boto3)

```python
import boto3

neptune = boto3.client(
    "neptune",
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
)

cluster = neptune.create_db_cluster(
    DBClusterIdentifier="my-neptune",
    Engine="neptune",
)
print(cluster["DBCluster"]["Endpoint"])
```

## Out of Scope

- IAM database authentication for Gremlin connections.
- Neptune Analytics (vector search, graph analytics).
- Neptune Serverless auto-pause/resume.
- Snapshot and restore operations.
