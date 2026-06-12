package io.github.hectorvent.floci.services.glue;

import io.github.hectorvent.floci.testing.RestAssuredJsonUtils;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GlueJobIntegrationTest {

    private static final String CONTENT_TYPE = "application/x-amz-json-1.1";
    private static final String JOB_NAME = "wire-etl-job";
    private static final String ROLE = "arn:aws:iam::000000000000:role/glue-role";

    private static String runId;

    @BeforeAll
    static void configureRestAssured() {
        RestAssuredJsonUtils.configureAwsContentTypes();
    }

    @Test
    @Order(1)
    void createJob() {
        given()
            .header("X-Amz-Target", "AWSGlue.CreateJob")
            .contentType(CONTENT_TYPE)
            .body("""
                {
                    "Name": "%s",
                    "Description": "Integration test job",
                    "Role": "%s",
                    "Command": {
                        "Name": "glueetl",
                        "ScriptLocation": "s3://scripts/etl.py",
                        "PythonVersion": "3"
                    },
                    "DefaultArguments": {"--ENV": "test"},
                    "GlueVersion": "5.0",
                    "WorkerType": "G.1X",
                    "NumberOfWorkers": 2
                }
                """.formatted(JOB_NAME, ROLE))
        .when()
            .post("/")
        .then()
            .statusCode(200)
            .body("Name", equalTo(JOB_NAME));
    }

    @Test
    @Order(2)
    void createDuplicateJobFails() {
        given()
            .header("X-Amz-Target", "AWSGlue.CreateJob")
            .contentType(CONTENT_TYPE)
            .body("""
                {
                    "Name": "%s",
                    "Role": "%s",
                    "Command": {"Name": "glueetl", "ScriptLocation": "s3://scripts/etl.py"}
                }
                """.formatted(JOB_NAME, ROLE))
        .when()
            .post("/")
        .then()
            .statusCode(400)
            .body("__type", containsString("AlreadyExistsException"));
    }

    @Test
    @Order(3)
    void getJob() {
        given()
            .header("X-Amz-Target", "AWSGlue.GetJob")
            .contentType(CONTENT_TYPE)
            .body("""
                {"JobName": "%s"}
                """.formatted(JOB_NAME))
        .when()
            .post("/")
        .then()
            .statusCode(200)
            .body("Job.Name", equalTo(JOB_NAME))
            .body("Job.Description", equalTo("Integration test job"))
            .body("Job.Role", equalTo(ROLE))
            .body("Job.Command.ScriptLocation", equalTo("s3://scripts/etl.py"))
            .body("Job.MaxRetries", equalTo(0))
            .body("Job.Timeout", equalTo(2880))
            .body("Job.CreatedOn", notNullValue());
    }

    @Test
    @Order(4)
    void getNonexistentJobFails() {
        given()
            .header("X-Amz-Target", "AWSGlue.GetJob")
            .contentType(CONTENT_TYPE)
            .body("""
                {"JobName": "no-such-job"}
                """)
        .when()
            .post("/")
        .then()
            .statusCode(400)
            .body("__type", containsString("EntityNotFoundException"));
    }

    @Test
    @Order(5)
    void getJobs() {
        given()
            .header("X-Amz-Target", "AWSGlue.GetJobs")
            .contentType(CONTENT_TYPE)
            .body("{}")
        .when()
            .post("/")
        .then()
            .statusCode(200)
            .body("Jobs.Name", hasItem(JOB_NAME));
    }

    @Test
    @Order(6)
    void updateJob() {
        given()
            .header("X-Amz-Target", "AWSGlue.UpdateJob")
            .contentType(CONTENT_TYPE)
            .body("""
                {
                    "JobName": "%s",
                    "JobUpdate": {
                        "Description": "Updated description",
                        "Role": "%s",
                        "Command": {
                            "Name": "glueetl",
                            "ScriptLocation": "s3://scripts/etl-v2.py",
                            "PythonVersion": "3"
                        },
                        "DefaultArguments": {"--ENV": "test"},
                        "Timeout": 60
                    }
                }
                """.formatted(JOB_NAME, ROLE))
        .when()
            .post("/")
        .then()
            .statusCode(200)
            .body("JobName", equalTo(JOB_NAME));

        given()
            .header("X-Amz-Target", "AWSGlue.GetJob")
            .contentType(CONTENT_TYPE)
            .body("""
                {"JobName": "%s"}
                """.formatted(JOB_NAME))
        .when()
            .post("/")
        .then()
            .statusCode(200)
            .body("Job.Description", equalTo("Updated description"))
            .body("Job.Command.ScriptLocation", equalTo("s3://scripts/etl-v2.py"))
            .body("Job.Timeout", equalTo(60));
    }

    @Test
    @Order(7)
    void updateNonexistentJobFails() {
        given()
            .header("X-Amz-Target", "AWSGlue.UpdateJob")
            .contentType(CONTENT_TYPE)
            .body("""
                {
                    "JobName": "no-such-job",
                    "JobUpdate": {
                        "Role": "%s",
                        "Command": {"Name": "glueetl", "ScriptLocation": "s3://scripts/etl.py"}
                    }
                }
                """.formatted(ROLE))
        .when()
            .post("/")
        .then()
            .statusCode(400)
            .body("__type", containsString("EntityNotFoundException"));
    }

    @Test
    @Order(8)
    void startJobRun() {
        runId = given()
            .header("X-Amz-Target", "AWSGlue.StartJobRun")
            .contentType(CONTENT_TYPE)
            .body("""
                {
                    "JobName": "%s",
                    "Arguments": {"--DAY": "2026-06-11"}
                }
                """.formatted(JOB_NAME))
        .when()
            .post("/")
        .then()
            .statusCode(200)
            .body("JobRunId", startsWith("jr_"))
            .extract().path("JobRunId");
    }

    @Test
    @Order(9)
    void getJobRun() throws Exception {
        String state = awaitTerminalJobRunState(JOB_NAME, runId);
        assertEquals("SUCCEEDED", state);

        java.util.Map<String, String> arguments = given()
            .header("X-Amz-Target", "AWSGlue.GetJobRun")
            .contentType(CONTENT_TYPE)
            .body("""
                {"JobName": "%s", "RunId": "%s"}
                """.formatted(JOB_NAME, runId))
        .when()
            .post("/")
        .then()
            .statusCode(200)
            .body("JobRun.Id", equalTo(runId))
            .body("JobRun.JobName", equalTo(JOB_NAME))
            .body("JobRun.JobRunState", equalTo("SUCCEEDED"))
            .body("JobRun.Attempt", equalTo(0))
            .body("JobRun.StartedOn", notNullValue())
            .body("JobRun.CompletedOn", notNullValue())
            .extract().path("JobRun.Arguments");
        assertEquals("test", arguments.get("--ENV"));
        assertEquals("2026-06-11", arguments.get("--DAY"));
    }

    @Test
    @Order(10)
    void getNonexistentJobRunFails() {
        given()
            .header("X-Amz-Target", "AWSGlue.GetJobRun")
            .contentType(CONTENT_TYPE)
            .body("""
                {"JobName": "%s", "RunId": "jr_does_not_exist"}
                """.formatted(JOB_NAME))
        .when()
            .post("/")
        .then()
            .statusCode(400)
            .body("__type", containsString("EntityNotFoundException"));
    }

    @Test
    @Order(11)
    void getJobRuns() {
        given()
            .header("X-Amz-Target", "AWSGlue.GetJobRuns")
            .contentType(CONTENT_TYPE)
            .body("""
                {"JobName": "%s"}
                """.formatted(JOB_NAME))
        .when()
            .post("/")
        .then()
            .statusCode(200)
            .body("JobRuns", hasSize(greaterThanOrEqualTo(1)))
            .body("JobRuns.Id", hasItem(runId));
    }

    @Test
    @Order(12)
    void startJobRunForNonexistentJobFails() {
        given()
            .header("X-Amz-Target", "AWSGlue.StartJobRun")
            .contentType(CONTENT_TYPE)
            .body("""
                {"JobName": "no-such-job"}
                """)
        .when()
            .post("/")
        .then()
            .statusCode(400)
            .body("__type", containsString("EntityNotFoundException"));
    }

    @Test
    @Order(13)
    void startJobRunConcurrentRunsExceededFails() {
        given()
            .header("X-Amz-Target", "AWSGlue.CreateJob")
            .contentType(CONTENT_TYPE)
            .body("""
                {
                    "Name": "wire-concurrent-job",
                    "Role": "%s",
                    "Command": {"Name": "glueetl", "ScriptLocation": "s3://scripts/etl.py"},
                    "ExecutionProperty": {"MaxConcurrentRuns": 0}
                }
                """.formatted(ROLE))
        .when()
            .post("/")
        .then()
            .statusCode(200);

        given()
            .header("X-Amz-Target", "AWSGlue.StartJobRun")
            .contentType(CONTENT_TYPE)
            .body("""
                {"JobName": "wire-concurrent-job"}
                """)
        .when()
            .post("/")
        .then()
            .statusCode(400)
            .body("__type", containsString("ConcurrentRunsExceededException"));
    }

    @Test
    @Order(14)
    void batchStopJobRun() {
        given()
            .header("X-Amz-Target", "AWSGlue.BatchStopJobRun")
            .contentType(CONTENT_TYPE)
            .body("""
                {
                    "JobName": "%s",
                    "JobRunIds": ["%s", "jr_does_not_exist"]
                }
                """.formatted(JOB_NAME, runId))
        .when()
            .post("/")
        .then()
            .statusCode(200)
            .body("SuccessfulSubmissions", hasSize(0))
            .body("Errors", hasSize(2))
            .body("Errors[0].JobName", equalTo(JOB_NAME))
            .body("Errors[0].JobRunId", equalTo(runId))
            .body("Errors[0].ErrorDetail.ErrorCode", equalTo("InvalidInputException"))
            .body("Errors[1].JobRunId", equalTo("jr_does_not_exist"))
            .body("Errors[1].ErrorDetail.ErrorCode", equalTo("EntityNotFoundException"));
    }

    @Test
    @Order(15)
    void batchStopJobRunForNonexistentJobFails() {
        given()
            .header("X-Amz-Target", "AWSGlue.BatchStopJobRun")
            .contentType(CONTENT_TYPE)
            .body("""
                {"JobName": "no-such-job", "JobRunIds": ["jr_x"]}
                """)
        .when()
            .post("/")
        .then()
            .statusCode(400)
            .body("__type", containsString("EntityNotFoundException"));
    }

    @Test
    @Order(16)
    void deleteJob() {
        given()
            .header("X-Amz-Target", "AWSGlue.DeleteJob")
            .contentType(CONTENT_TYPE)
            .body("""
                {"JobName": "%s"}
                """.formatted(JOB_NAME))
        .when()
            .post("/")
        .then()
            .statusCode(200)
            .body("JobName", equalTo(JOB_NAME));

        given()
            .header("X-Amz-Target", "AWSGlue.GetJob")
            .contentType(CONTENT_TYPE)
            .body("""
                {"JobName": "%s"}
                """.formatted(JOB_NAME))
        .when()
            .post("/")
        .then()
            .statusCode(400)
            .body("__type", containsString("EntityNotFoundException"));
    }

    @Test
    @Order(17)
    void deleteNonexistentJobFails() {
        given()
            .header("X-Amz-Target", "AWSGlue.DeleteJob")
            .contentType(CONTENT_TYPE)
            .body("""
                {"JobName": "%s"}
                """.formatted(JOB_NAME))
        .when()
            .post("/")
        .then()
            .statusCode(400)
            .body("__type", containsString("EntityNotFoundException"));
    }

    private static String awaitTerminalJobRunState(String jobName, String runId) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            String state = given()
                .header("X-Amz-Target", "AWSGlue.GetJobRun")
                .contentType(CONTENT_TYPE)
                .body("""
                    {"JobName": "%s", "RunId": "%s"}
                    """.formatted(jobName, runId))
            .when()
                .post("/")
            .then()
                .statusCode(200)
                .extract().path("JobRun.JobRunState");
            if (GlueService.isTerminalJobRunState(state)) {
                return state;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("Job run did not reach a terminal state: " + jobName + "/" + runId);
    }
}
