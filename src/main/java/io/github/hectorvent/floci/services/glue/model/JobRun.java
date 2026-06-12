package io.github.hectorvent.floci.services.glue.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.time.Instant;
import java.util.Map;

@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobRun {
    @JsonProperty("Id")
    private String id;
    @JsonProperty("Attempt")
    private Integer attempt;
    @JsonProperty("JobName")
    private String jobName;
    @JsonProperty("JobRunState")
    private String jobRunState;
    @JsonProperty("Arguments")
    private Map<String, String> arguments;
    @JsonProperty("StartedOn")
    @JsonFormat(shape = JsonFormat.Shape.NUMBER)
    private Instant startedOn;
    @JsonProperty("LastModifiedOn")
    @JsonFormat(shape = JsonFormat.Shape.NUMBER)
    private Instant lastModifiedOn;
    @JsonProperty("CompletedOn")
    @JsonFormat(shape = JsonFormat.Shape.NUMBER)
    private Instant completedOn;
    @JsonProperty("ErrorMessage")
    private String errorMessage;
    @JsonProperty("StateDetail")
    private String stateDetail;
    @JsonProperty("Timeout")
    private Integer timeout;
    @JsonProperty("ExecutionTime")
    private Integer executionTime;
    @JsonProperty("GlueVersion")
    private String glueVersion;
    @JsonProperty("WorkerType")
    private String workerType;
    @JsonProperty("NumberOfWorkers")
    private Integer numberOfWorkers;
    @JsonProperty("LogGroupName")
    private String logGroupName;
    @JsonProperty("LogStreamName")
    private String logStreamName;

    public JobRun() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public Integer getAttempt() { return attempt; }
    public void setAttempt(Integer attempt) { this.attempt = attempt; }
    public String getJobName() { return jobName; }
    public void setJobName(String jobName) { this.jobName = jobName; }
    public String getJobRunState() { return jobRunState; }
    public void setJobRunState(String jobRunState) { this.jobRunState = jobRunState; }
    public Map<String, String> getArguments() { return arguments; }
    public void setArguments(Map<String, String> arguments) { this.arguments = arguments; }
    public Instant getStartedOn() { return startedOn; }
    public void setStartedOn(Instant startedOn) { this.startedOn = startedOn; }
    public Instant getLastModifiedOn() { return lastModifiedOn; }
    public void setLastModifiedOn(Instant lastModifiedOn) { this.lastModifiedOn = lastModifiedOn; }
    public Instant getCompletedOn() { return completedOn; }
    public void setCompletedOn(Instant completedOn) { this.completedOn = completedOn; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public String getStateDetail() { return stateDetail; }
    public void setStateDetail(String stateDetail) { this.stateDetail = stateDetail; }
    public Integer getTimeout() { return timeout; }
    public void setTimeout(Integer timeout) { this.timeout = timeout; }
    public Integer getExecutionTime() { return executionTime; }
    public void setExecutionTime(Integer executionTime) { this.executionTime = executionTime; }
    public String getGlueVersion() { return glueVersion; }
    public void setGlueVersion(String glueVersion) { this.glueVersion = glueVersion; }
    public String getWorkerType() { return workerType; }
    public void setWorkerType(String workerType) { this.workerType = workerType; }
    public Integer getNumberOfWorkers() { return numberOfWorkers; }
    public void setNumberOfWorkers(Integer numberOfWorkers) { this.numberOfWorkers = numberOfWorkers; }
    public String getLogGroupName() { return logGroupName; }
    public void setLogGroupName(String logGroupName) { this.logGroupName = logGroupName; }
    public String getLogStreamName() { return logStreamName; }
    public void setLogStreamName(String logStreamName) { this.logStreamName = logStreamName; }
}
