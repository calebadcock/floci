package io.github.hectorvent.floci.services.glue.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.time.Instant;
import java.util.Map;

@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Job {
    @JsonProperty("Name")
    private String name;
    @JsonProperty("Description")
    private String description;
    @JsonProperty("Role")
    private String role;
    @JsonProperty("CreatedOn")
    @JsonFormat(shape = JsonFormat.Shape.NUMBER)
    private Instant createdOn;
    @JsonProperty("LastModifiedOn")
    @JsonFormat(shape = JsonFormat.Shape.NUMBER)
    private Instant lastModifiedOn;
    @JsonProperty("ExecutionProperty")
    private Map<String, Object> executionProperty;
    @JsonProperty("Command")
    private JobCommand command;
    @JsonProperty("DefaultArguments")
    private Map<String, String> defaultArguments;
    @JsonProperty("NonOverridableArguments")
    private Map<String, String> nonOverridableArguments;
    @JsonProperty("MaxRetries")
    private Integer maxRetries;
    @JsonProperty("Timeout")
    private Integer timeout;
    @JsonProperty("GlueVersion")
    private String glueVersion;
    @JsonProperty("WorkerType")
    private String workerType;
    @JsonProperty("NumberOfWorkers")
    private Integer numberOfWorkers;
    @JsonProperty("Tags")
    private Map<String, String> tags;

    public Job() {}

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }
    public Instant getCreatedOn() { return createdOn; }
    public void setCreatedOn(Instant createdOn) { this.createdOn = createdOn; }
    public Instant getLastModifiedOn() { return lastModifiedOn; }
    public void setLastModifiedOn(Instant lastModifiedOn) { this.lastModifiedOn = lastModifiedOn; }
    public Map<String, Object> getExecutionProperty() { return executionProperty; }
    public void setExecutionProperty(Map<String, Object> executionProperty) { this.executionProperty = executionProperty; }
    public JobCommand getCommand() { return command; }
    public void setCommand(JobCommand command) { this.command = command; }
    public Map<String, String> getDefaultArguments() { return defaultArguments; }
    public void setDefaultArguments(Map<String, String> defaultArguments) { this.defaultArguments = defaultArguments; }
    public Map<String, String> getNonOverridableArguments() { return nonOverridableArguments; }
    public void setNonOverridableArguments(Map<String, String> nonOverridableArguments) { this.nonOverridableArguments = nonOverridableArguments; }
    public Integer getMaxRetries() { return maxRetries; }
    public void setMaxRetries(Integer maxRetries) { this.maxRetries = maxRetries; }
    public Integer getTimeout() { return timeout; }
    public void setTimeout(Integer timeout) { this.timeout = timeout; }
    public String getGlueVersion() { return glueVersion; }
    public void setGlueVersion(String glueVersion) { this.glueVersion = glueVersion; }
    public String getWorkerType() { return workerType; }
    public void setWorkerType(String workerType) { this.workerType = workerType; }
    public Integer getNumberOfWorkers() { return numberOfWorkers; }
    public void setNumberOfWorkers(Integer numberOfWorkers) { this.numberOfWorkers = numberOfWorkers; }
    public Map<String, String> getTags() { return tags; }
    public void setTags(Map<String, String> tags) { this.tags = tags; }
}
