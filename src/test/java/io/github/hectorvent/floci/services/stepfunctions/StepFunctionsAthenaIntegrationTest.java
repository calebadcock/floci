package io.github.hectorvent.floci.services.stepfunctions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.testing.RestAssuredJsonUtils;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.response.Response;
import org.junit.jupiter.api.*;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the Step Functions Athena integration
 * (arn:aws:states:::athena:startQueryExecution with the .sync variant,
 * getQueryExecution, getQueryResults, and stopQueryExecution).
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StepFunctionsAthenaIntegrationTest {

    private static final String SFN_CONTENT_TYPE = "application/x-amz-json-1.0";
    private static final String ATHENA_CONTENT_TYPE = "application/x-amz-json-1.1";
    private static final String ROLE_ARN = "arn:aws:iam::000000000000:role/test-role";
    private static final ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    static void configureRestAssured() {
        RestAssuredJsonUtils.configureAwsContentTypes();
    }

    @Test
    @Order(0)
    void startQueryExecution_fireAndForget_returnsQueryExecutionIdOnly() throws Exception {
        String definition = buildStateMachineDefinition("arn:aws:states:::athena:startQueryExecution", """
                {"QueryString": "SELECT 1"}
                """);

        String smArn = createStateMachine("athena-fire-and-forget-" + System.currentTimeMillis(), definition);
        String execArn = startExecution(smArn, "{}");
        String output = waitForExecution(execArn);

        JsonNode result = mapper.readTree(output);
        assertTrue(result.has("QueryExecutionId"), "Fire-and-forget output must have QueryExecutionId");
        assertEquals(1, result.size(), "Fire-and-forget output must contain only QueryExecutionId");
        assertFalse(result.has("QueryExecution"), "Fire-and-forget output must not include the full QueryExecution shape");
    }

    @Test
    @Order(1)
    void startQueryExecutionSync_success_returnsQueryExecution() throws Exception {
        String definition = buildStateMachineDefinition("arn:aws:states:::athena:startQueryExecution.sync", """
                {
                    "QueryString": "SELECT 1",
                    "QueryExecutionContext": {"Database": "analytics"}
                }
                """);

        String smArn = createStateMachine("athena-sync-success-" + System.currentTimeMillis(), definition);
        String execArn = startExecution(smArn, "{}");
        String output = waitForExecution(execArn);

        JsonNode result = mapper.readTree(output);
        JsonNode execution = result.path("QueryExecution");
        assertFalse(execution.isMissingNode(), ".sync output must have QueryExecution");
        assertEquals("SELECT 1", execution.path("Query").asText());
        assertEquals("SUCCEEDED", execution.path("Status").path("State").asText());
        assertEquals("analytics", execution.path("QueryExecutionContext").path("Database").asText());
        assertFalse(result.has("QueryExecutionId"), ".sync output must not use the fire-and-forget shape");
    }

    @Test
    @Order(2)
    void getQueryExecution_returnsWrappedQueryExecution() throws Exception {
        String queryExecutionId = startQueryViaAthenaApi();
        String definition = buildStateMachineDefinition("arn:aws:states:::athena:getQueryExecution", """
                {"QueryExecutionId": "%s"}
                """.formatted(queryExecutionId));

        String smArn = createStateMachine("athena-get-query-execution-" + System.currentTimeMillis(), definition);
        String execArn = startExecution(smArn, "{}");
        String output = waitForExecution(execArn);

        JsonNode execution = mapper.readTree(output).path("QueryExecution");
        assertEquals(queryExecutionId, execution.path("QueryExecutionId").asText());
        assertTrue(execution.has("Status"));
    }

    @Test
    @Order(3)
    void getQueryResults_returnsWrappedResultSet() throws Exception {
        String queryExecutionId = startQueryViaAthenaApi();
        String definition = buildStateMachineDefinition("arn:aws:states:::athena:getQueryResults", """
                {"QueryExecutionId": "%s"}
                """.formatted(queryExecutionId));

        String smArn = createStateMachine("athena-get-query-results-" + System.currentTimeMillis(), definition);
        String execArn = startExecution(smArn, "{}");
        String output = waitForExecution(execArn);

        JsonNode result = mapper.readTree(output);
        assertTrue(result.has("ResultSet"), "Output must have ResultSet");
    }

    @Test
    @Order(4)
    void stopQueryExecution_cancelsQuery() throws Exception {
        String queryExecutionId = startQueryViaAthenaApi();
        String definition = buildStateMachineDefinition("arn:aws:states:::athena:stopQueryExecution", """
                {"QueryExecutionId": "%s"}
                """.formatted(queryExecutionId));

        String smArn = createStateMachine("athena-stop-query-" + System.currentTimeMillis(), definition);
        String execArn = startExecution(smArn, "{}");
        waitForExecution(execArn);

        Response resp = given()
                .header("X-Amz-Target", "AmazonAthena.GetQueryExecution")
                .contentType(ATHENA_CONTENT_TYPE)
                .body("""
                        {"QueryExecutionId": "%s"}
                        """.formatted(queryExecutionId))
                .when().post("/");
        resp.then().statusCode(200);
        assertEquals("CANCELLED", resp.jsonPath().getString("QueryExecution.Status.State"));
    }

    @Test
    @Order(5)
    void startQueryExecution_missingQueryString_failsExecution() throws Exception {
        String definition = buildStateMachineDefinition("arn:aws:states:::athena:startQueryExecution.sync", "{}");

        String smArn = createStateMachine("athena-missing-query-" + System.currentTimeMillis(), definition);
        String execArn = startExecution(smArn, "{}");
        Response resp = waitForExecutionFailure(execArn);

        assertEquals("Athena.InvalidRequestException", resp.jsonPath().getString("error"));
        assertTrue(resp.jsonPath().getString("cause").contains("QueryString"));
    }

    @Test
    @Order(6)
    void getQueryExecution_unknownId_isCatchableAsAthenaError() throws Exception {
        String definition = """
                {
                    "StartAt": "GetQuery",
                    "States": {
                        "GetQuery": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::athena:getQueryExecution",
                            "Parameters": {"QueryExecutionId": "no-such-query"},
                            "Catch": [
                                {"ErrorEquals": ["Athena.InvalidRequestException"], "Next": "Caught"}
                            ],
                            "End": true
                        },
                        "Caught": {"Type": "Pass", "End": true}
                    }
                }
                """;

        String smArn = createStateMachine("athena-catch-" + System.currentTimeMillis(), definition);
        String execArn = startExecution(smArn, "{}");
        String output = waitForExecution(execArn);

        JsonNode result = mapper.readTree(output);
        assertEquals("Athena.InvalidRequestException", result.path("Error").asText());
        assertTrue(result.path("Cause").asText().contains("no-such-query"));
    }

    // ──────────────── Helpers ────────────────

    private String startQueryViaAthenaApi() {
        Response resp = given()
                .header("X-Amz-Target", "AmazonAthena.StartQueryExecution")
                .contentType(ATHENA_CONTENT_TYPE)
                .body("""
                        {"QueryString": "SELECT 1"}
                        """)
                .when().post("/");
        resp.then().statusCode(200);
        return resp.jsonPath().getString("QueryExecutionId");
    }

    private String buildStateMachineDefinition(String resource, String parameters) {
        return """
                {
                    "StartAt": "Action",
                    "States": {
                        "Action": {
                            "Type": "Task",
                            "Resource": "%s",
                            "Parameters": %s,
                            "End": true
                        }
                    }
                }
                """.formatted(resource, parameters.strip());
    }

    private String createStateMachine(String name, String definition) {
        Response resp = given()
                .header("X-Amz-Target", "AWSStepFunctions.CreateStateMachine")
                .contentType(SFN_CONTENT_TYPE)
                .body("""
                        {
                            "name": "%s",
                            "definition": %s,
                            "roleArn": "%s"
                        }
                        """.formatted(name, quote(definition), ROLE_ARN))
                .when()
                .post("/");
        resp.then().statusCode(200);
        return resp.jsonPath().getString("stateMachineArn");
    }

    private String startExecution(String smArn, String input) {
        Response resp = given()
                .header("X-Amz-Target", "AWSStepFunctions.StartExecution")
                .contentType(SFN_CONTENT_TYPE)
                .body("""
                        {
                            "stateMachineArn": "%s",
                            "input": %s
                        }
                        """.formatted(smArn, quote(input)))
                .when()
                .post("/");
        resp.then().statusCode(200);
        return resp.jsonPath().getString("executionArn");
    }

    private String waitForExecution(String execArn) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            Response resp = describeExecution(execArn);
            String status = resp.jsonPath().getString("status");
            if ("SUCCEEDED".equals(status)) {
                return resp.jsonPath().getString("output");
            }
            if ("FAILED".equals(status) || "ABORTED".equals(status)) {
                fail("Execution " + status + ": " + resp.body().asString());
            }
            Thread.sleep(100);
        }
        fail("Execution did not complete within timeout");
        return null;
    }

    private Response waitForExecutionFailure(String execArn) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            Response resp = describeExecution(execArn);
            String status = resp.jsonPath().getString("status");
            if ("FAILED".equals(status)) {
                return resp;
            }
            if ("SUCCEEDED".equals(status)) {
                fail("Execution should have failed but succeeded: " + resp.body().asString());
            }
            Thread.sleep(100);
        }
        fail("Execution did not complete within timeout");
        return null;
    }

    private Response describeExecution(String execArn) {
        return given()
                .header("X-Amz-Target", "AWSStepFunctions.DescribeExecution")
                .contentType(SFN_CONTENT_TYPE)
                .body("""
                        {"executionArn": "%s"}
                        """.formatted(execArn))
                .when()
                .post("/");
    }

    private static String quote(String raw) {
        return "\"" + raw
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t")
                + "\"";
    }
}
