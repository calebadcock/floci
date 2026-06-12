# Step Functions

**Protocol:** JSON 1.1 (`X-Amz-Target: AmazonStatesService.*`)
**Endpoint:** `POST http://localhost:4566/`

## Supported Actions

| Action | Description |
|---|---|
| `CreateStateMachine` | Create a state machine (Standard or Express) |
| `DescribeStateMachine` | Get state machine definition and metadata |
| `ListStateMachines` | List all state machines |
| `DeleteStateMachine` | Delete a state machine |
| `ValidateStateMachineDefinition` | Validate an ASL definition without creating a state machine |
| `StartExecution` | Start a new execution |
| `DescribeExecution` | Get execution status and output |
| `ListExecutions` | List executions for a state machine |
| `StopExecution` | Stop a running execution |
| `GetExecutionHistory` | Get the full event history of an execution |
| `SendTaskSuccess` | Report task success (for `.waitForTaskToken` tasks) |
| `SendTaskFailure` | Report task failure |
| `SendTaskHeartbeat` | Send a heartbeat for long-running tasks |

## Service Integrations

Floci supports a focused set of optimized service integrations, plus AWS SDK (`aws-sdk:`) variants for DynamoDB and SQS:

| Resource | Notes |
|---|---|
| `arn:aws:states:::lambda:invoke` | Invokes local Lambda functions |
| `arn:aws:states:::dynamodb:*` | Selected DynamoDB optimized integrations |
| `arn:aws:states:::aws-sdk:dynamodb:*` | AWS SDK DynamoDB integrations (camelCase action names) |
| `arn:aws:states:::sqs:sendMessage` | Sends a message to local SQS |
| `arn:aws:states:::aws-sdk:sqs:sendMessage` | AWS SDK variant of SQS `SendMessage` |
| `arn:aws:states:::states:startExecution`, `.sync`, `.sync:2` | Starts nested state machines |
| `arn:aws:states:::glue:startJobRun`, `.sync` | Starts local Glue job runs and optionally waits for completion |
| `arn:aws:states:::athena:*` | Selected Athena optimized integrations |

### Glue Job Integration

`arn:aws:states:::glue:startJobRun` starts a local Glue job run and returns `{"JobRunId": "..."}` without waiting for the run to finish. With the `.sync` suffix, the execution polls the run until it reaches a terminal state and returns the full `JobRun` on success. The `.sync` wait deadline is derived from the run's `Timeout` (the `Timeout` from the task input, falling back to the job's `Timeout`, default 2880 minutes) plus a short grace period; if the deadline passes, Floci stops the job run and fails the task with `States.Timeout`. Runs that end in any other non-`SUCCEEDED` terminal state fail the task with error name `States.TaskFailed` and the serialized `JobRun` as the cause.

### Athena Integration

`arn:aws:states:::athena:startQueryExecution` submits a local Athena query and returns `{"QueryExecutionId": "..."}` without waiting for it to finish. With the `.sync` suffix, the execution polls the query until it reaches a terminal state and returns `{"QueryExecution": {...}}` on success; queries that end `FAILED` or `CANCELLED` fail the task with error name `States.TaskFailed` and the serialized `QueryExecution` as the cause. The `.sync` wait is capped at 30 minutes (Athena's default DML query timeout); past the cap, Floci stops the query and fails the task with `States.Timeout`. `getQueryExecution`, `getQueryResults`, and `stopQueryExecution` mirror the corresponding Athena actions and use the same request and response shapes.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `FLOCI_SERVICES_STEPFUNCTIONS_ENABLED` | `true` | Enable or disable the service |

## Examples

```bash
export AWS_ENDPOINT_URL=http://localhost:4566

# Create a state machine
SM_ARN=$(aws stepfunctions create-state-machine \
  --name my-workflow \
  --definition '{
    "Comment": "Simple workflow",
    "StartAt": "HelloWorld",
    "States": {
      "HelloWorld": {
        "Type": "Pass",
        "Result": {"message": "Hello, World!"},
        "End": true
      }
    }
  }' \
  --role-arn arn:aws:iam::000000000000:role/step-functions-role \
  --query stateMachineArn --output text \
  --endpoint-url $AWS_ENDPOINT_URL)

# Start an execution
EXEC_ARN=$(aws stepfunctions start-execution \
  --state-machine-arn $SM_ARN \
  --input '{"key":"value"}' \
  --query executionArn --output text \
  --endpoint-url $AWS_ENDPOINT_URL)

# Check status
aws stepfunctions describe-execution \
  --execution-arn $EXEC_ARN \
  --endpoint-url $AWS_ENDPOINT_URL

# Get event history
aws stepfunctions get-execution-history \
  --execution-arn $EXEC_ARN \
  --endpoint-url $AWS_ENDPOINT_URL
```
