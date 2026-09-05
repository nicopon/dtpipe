using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Voie 4 §6 (suite 2) lot E1: <see cref="PlanProgress"/> is a pure state machine fed by the tool
/// calls the agent makes — a <c>yamlContent</c> argument drafts the plan, validate / execute
/// results move it forward. No console, no LLM; a scripted sequence lands on a deterministic badge.
/// </summary>
public class PlanProgressTests
{
    private const string YamlA = "jobs:\n  main:\n    input: csv:a.csv\n    output: csv:b.csv\n";
    private const string YamlB = "jobs:\n  main:\n    input: csv:a.csv\n    output: pg:out\n";

    private const string ValidateOk = "{\"success\":true,\"message\":\"YAML job configuration and topology are valid.\"}";
    private const string ValidateKo = "{\"success\":false,\"errors\":[\"Branch 'main': Unknown transformer type 'bogus'.\"]}";
    private const string ExecuteSampleOk = "{\"success\":true,\"exitCode\":0,\"applied\":false,\"mode\":\"sample\",\"rowsRequested\":10,\"error\":null}";
    private const string ExecuteWriteOk = "{\"success\":true,\"exitCode\":0,\"applied\":true,\"mode\":\"write\",\"durationMs\":42,\"safety\":\"ok\"}";
    private const string ExecuteSafetyKo = "{\"success\":false,\"stage\":\"safety\",\"applied\":false,\"violation\":[\"DROP is a destructive verb\"],\"message\":\"Execution blocked by the SQL safety policy.\"}";
    private const string ExecuteRunKo = "{\"success\":false,\"stage\":\"execution\",\"applied\":false,\"errors\":[\"connection refused\"]}";

    [Fact]
    public void A_Fresh_Progress_Is_None()
    {
        var p = new PlanProgress();
        Assert.Equal(PlanState.None, p.State);
        Assert.Null(p.Yaml);
        Assert.Null(p.Message);
    }

    [Fact]
    public void A_Yaml_Argument_Drafts_The_Plan()
    {
        var p = new PlanProgress();
        p.OnPlanUpdated(YamlA);

        Assert.Equal(PlanState.Drafted, p.State);
        Assert.Equal(YamlA, p.Yaml);
    }

    [Fact]
    public void An_Empty_Yaml_Argument_Is_Ignored()
    {
        var p = new PlanProgress();
        p.OnPlanUpdated("   ");
        p.OnPlanUpdated(null);

        Assert.Equal(PlanState.None, p.State);
    }

    [Fact]
    public void Validate_Ok_Moves_A_Draft_To_Validated()
    {
        var p = new PlanProgress();
        p.OnPlanUpdated(YamlA);
        p.OnToolResult("validate-yaml-job", isError: false, ValidateOk);

        Assert.Equal(PlanState.Validated, p.State);
        Assert.Null(p.Message);
    }

    [Fact]
    public void Validate_Ko_Marks_The_Plan_Invalid_With_The_Reason()
    {
        var p = new PlanProgress();
        p.OnPlanUpdated(YamlA);
        p.OnToolResult("validate-yaml-job", isError: true, ValidateKo);

        Assert.Equal(PlanState.Invalid, p.State);
        Assert.Contains("Unknown transformer type", p.Message);
    }

    [Fact]
    public void Execute_Dry_Run_Moves_A_Validated_Plan_To_DryRunOk()
    {
        var p = new PlanProgress();
        p.OnPlanUpdated(YamlA);
        p.OnToolResult("validate-yaml-job", isError: false, ValidateOk);
        p.OnToolResult("execute-yaml-job", isError: false, ExecuteSampleOk);

        Assert.Equal(PlanState.DryRunOk, p.State);
    }

    [Fact]
    public void Execute_With_A_Real_Write_Moves_The_Plan_To_Applied()
    {
        var p = new PlanProgress();
        p.OnPlanUpdated(YamlA);
        p.OnToolResult("validate-yaml-job", isError: false, ValidateOk);
        p.OnToolResult("execute-yaml-job", isError: false, ExecuteWriteOk);

        Assert.Equal(PlanState.Applied, p.State);
    }

    [Fact]
    public void Execute_Reads_Applied_From_The_Write_Mode_When_The_Flag_Is_Absent()
    {
        var p = new PlanProgress();
        p.OnPlanUpdated(YamlA);
        p.OnToolResult("execute-yaml-job", isError: false, "{\"success\":true,\"mode\":\"write\"}");

        Assert.Equal(PlanState.Applied, p.State);
    }

    [Fact]
    public void An_Execute_Failure_On_The_Safety_Gate_Marks_The_Plan_Failed()
    {
        var p = new PlanProgress();
        p.OnPlanUpdated(YamlA);
        p.OnToolResult("validate-yaml-job", isError: false, ValidateOk);
        p.OnToolResult("execute-yaml-job", isError: true, ExecuteSafetyKo);

        Assert.Equal(PlanState.Failed, p.State);
        Assert.Contains("destructive verb", p.Message);
    }

    [Fact]
    public void An_Execute_Runtime_Failure_Marks_The_Plan_Failed_With_The_Error()
    {
        var p = new PlanProgress();
        p.OnPlanUpdated(YamlA);
        p.OnToolResult("execute-yaml-job", isError: true, ExecuteRunKo);

        Assert.Equal(PlanState.Failed, p.State);
        Assert.Contains("connection refused", p.Message);
    }

    [Fact]
    public void A_Changed_Plan_Falls_Back_To_Drafted_Even_After_Validation()
    {
        var p = new PlanProgress();
        p.OnPlanUpdated(YamlA);
        p.OnToolResult("validate-yaml-job", isError: false, ValidateOk);
        Assert.Equal(PlanState.Validated, p.State);

        p.OnPlanUpdated(YamlB);

        Assert.Equal(PlanState.Drafted, p.State);
        Assert.Equal(YamlB, p.Yaml);
        Assert.Null(p.Message);
    }

    [Fact]
    public void Re_Submitting_The_Same_Plan_Is_A_No_Op()
    {
        var p = new PlanProgress();
        p.OnPlanUpdated(YamlA);
        p.OnToolResult("validate-yaml-job", isError: false, ValidateOk);

        p.OnPlanUpdated(YamlA);

        Assert.Equal(PlanState.Validated, p.State);
    }

    [Fact]
    public void Tool_Results_Before_The_First_Draft_Are_Ignored()
    {
        var p = new PlanProgress();
        p.OnToolResult("validate-yaml-job", isError: false, ValidateOk);
        p.OnToolResult("execute-yaml-job", isError: false, ExecuteWriteOk);

        Assert.Equal(PlanState.None, p.State);
    }

    [Fact]
    public void Unrelated_Tools_Never_Move_The_Plan_State()
    {
        var p = new PlanProgress();
        p.OnPlanUpdated(YamlA);
        p.OnToolResult("validate-yaml-job", isError: false, ValidateOk);

        p.OnToolResult("inspect", isError: false, "[{\"Name\":\"id\"}]");
        p.OnToolResult("preview-data", isError: true, "{\"error\":\"nope\"}");

        Assert.Equal(PlanState.Validated, p.State);
        Assert.Null(p.Message);
    }

    [Fact]
    public void A_Dry_Run_After_A_Real_Write_Does_Not_Walk_The_Badge_Back()
    {
        var p = new PlanProgress();
        p.OnPlanUpdated(YamlA);
        p.OnToolResult("execute-yaml-job", isError: false, ExecuteWriteOk);
        Assert.Equal(PlanState.Applied, p.State);

        p.OnToolResult("execute-yaml-job", isError: false, ExecuteSampleOk);

        Assert.Equal(PlanState.Applied, p.State);
    }

    [Fact]
    public void Execute_Can_Move_A_Draft_Straight_To_DryRunOk_Without_An_Explicit_Validate()
    {
        // execute-yaml-job validates internally; a success implies the plan parsed.
        var p = new PlanProgress();
        p.OnPlanUpdated(YamlA);
        p.OnToolResult("execute-yaml-job", isError: false, ExecuteSampleOk);

        Assert.Equal(PlanState.DryRunOk, p.State);
    }

    [Fact]
    public void A_Re_Validation_That_Fails_Moves_A_Validated_Plan_To_Invalid()
    {
        var p = new PlanProgress();
        p.OnPlanUpdated(YamlA);
        p.OnToolResult("validate-yaml-job", isError: false, ValidateOk);
        p.OnToolResult("validate-yaml-job", isError: true, ValidateKo);

        Assert.Equal(PlanState.Invalid, p.State);
    }
}
