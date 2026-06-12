package io.github.hectorvent.floci.services.glue.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobCommand {
    @JsonProperty("Name")
    private String name;
    @JsonProperty("ScriptLocation")
    private String scriptLocation;
    @JsonProperty("PythonVersion")
    private String pythonVersion;

    public JobCommand() {}

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getScriptLocation() { return scriptLocation; }
    public void setScriptLocation(String scriptLocation) { this.scriptLocation = scriptLocation; }
    public String getPythonVersion() { return pythonVersion; }
    public void setPythonVersion(String pythonVersion) { this.pythonVersion = pythonVersion; }
}
