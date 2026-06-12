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
 * Integration tests for the Step Functions Glue integration
 * (arn:aws:states:::glue:startJobRun and the .sync variant).
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StepFunctionsGlueIntegrationTest {

    private static final String SFN_CONTENT_TYPE = "application/x-amz-json-1.0";
    private static final String GLUE_CONTENT_TYPE = "application/x-amz-json-1.1";
    private static final String ROLE_ARN = "arn:aws:iam::000000000000:role/test-role";
    private static final String JOB_NAME = "sfn-glue-integration-job";
    private static final ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    static void configureRestAssured() {
        RestAssuredJsonUtils.configureAwsContentTypes();
    }

    @Test
    @Order(0)
    void setup_createGlueJob() {
        given()
                .header("X-Amz-Target", "AWSGlue.CreateJob")
                .contentType(GLUE_CONTENT_TYPE)
                .body("""
                        {
                            "Name": "%s",
                            "Role": "arn:aws:iam::000000000000:role/glue-role",
                            "Command": {
                                "Name": "glueetl",
                                "ScriptLocation": "s3://scripts/etl.py"
                            }
                        }
                        """.formatted(JOB_NAME))
                .when().post("/")
                .then().statusCode(200);
    }

    @Test
    @Order(1)
    void startJobRun_fireAndForget_returnsJobRunIdOnly() throws Exception {
        String definition = buildStateMachineDefinition("arn:aws:states:::glue:startJobRun", """
                {"JobName": "%s"}
                """.formatted(JOB_NAME));

        String smArn = createStateMachine("glue-fire-and-forget-" + System.currentTimeMillis(), definition);
        String execArn = startExecution(smArn, "{}");
        String output = waitForExecution(execArn);

        JsonNode result = mapper.readTree(output);
        assertTrue(result.has("JobRunId"), "Fire-and-forget output must have JobRunId");
        assertTrue(result.get("JobRunId").asText().startsWith("jr_"));
        assertEquals(1, result.size(), "Fire-and-forget output must contain only JobRunId");
        assertFalse(result.has("Id"), "Fire-and-forget output must not include the full JobRun shape");
    }

    @Test
    @Order(2)
    void startJobRunSync_success_returnsFullJobRun() throws Exception {
        String definition = buildStateMachineDefinition("arn:aws:states:::glue:startJobRun.sync", """
                {
                    "JobName": "%s",
                    "Arguments": {"--DAY": "2026-06-11"}
                }
                """.formatted(JOB_NAME));

        String smArn = createStateMachine("glue-sync-success-" + System.currentTimeMillis(), definition);
        String execArn = startExecution(smArn, "{}");
        String output = waitForExecution(execArn);

        JsonNode result = mapper.readTree(output);
        assertTrue(result.has("Id"), ".sync output must have Id field");
        assertTrue(result.get("Id").asText().startsWith("jr_"));
        assertEquals(JOB_NAME, result.path("JobName").asText());
        assertEquals("SUCCEEDED", result.path("JobRunState").asText());
        assertTrue(result.has("StartedOn"), ".sync output must have StartedOn");
        assertTrue(result.has("CompletedOn"), ".sync output must have CompletedOn");
        assertEquals("2026-06-11", result.path("Arguments").path("--DAY").asText());
        assertFalse(result.has("JobRunId"), ".sync output must not use the fire-and-forget shape");
    }

    @Test
    @Order(3)
    void startJobRunSync_failure_isCatchableAsTaskFailed() throws Exception {
        String definition = """
                {
                    "StartAt": "StartJob",
                    "States": {
                        "StartJob": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {"JobName": "no-such-glue-job"},
                            "Catch": [
                                {"ErrorEquals": ["States.TaskFailed"], "Next": "Caught"}
                            ],
                            "End": true
                        },
                        "Caught": {"Type": "Pass", "End": true}
                    }
                }
                """;

        String smArn = createStateMachine("glue-sync-catch-" + System.currentTimeMillis(), definition);
        String execArn = startExecution(smArn, "{}");
        String output = waitForExecution(execArn);

        JsonNode result = mapper.readTree(output);
        assertEquals("Glue.EntityNotFoundException", result.path("Error").asText());
        assertTrue(result.path("Cause").asText().contains("no-such-glue-job"));
    }

    @Test
    @Order(4)
    void startJobRun_missingJobName_failsExecution() throws Exception {
        String definition = buildStateMachineDefinition("arn:aws:states:::glue:startJobRun.sync", "{}");

        String smArn = createStateMachine("glue-missing-jobname-" + System.currentTimeMillis(), definition);
        String execArn = startExecution(smArn, "{}");
        Response resp = waitForExecutionFailure(execArn);

        assertEquals("Glue.InvalidInputException", resp.jsonPath().getString("error"));
        assertTrue(resp.jsonPath().getString("cause").contains("JobName"));
    }

    // ──────────────── Helpers ────────────────

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
