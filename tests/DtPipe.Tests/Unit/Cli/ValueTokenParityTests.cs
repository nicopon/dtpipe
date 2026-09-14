using DtPipe.Cli.Pipeline;
using Xunit;
using System.Linq;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// F8 third axis — unified value-token semantics (arity-driven consumption, Strategy B).
/// A scalar/repeatable flag ALWAYS consumes the next token as its value, even when it
/// starts with '-'. Boolean flags never do.
/// </summary>
public class ValueTokenParityTests
{
    private static PipelineLexer MakeLexer()
    {
        var registry = new FlagRegistry();
        CoreFlagRegistry.RegisterCoreFlags(registry);
        foreach (var def in new DtPipe.Cli.Infrastructure.PipelineOptionsCliContributor().GetFlagDefs())
            registry.Register(def with { Stage = FlagStage.All });

        registry.Register(new FlagDef("--mask", System.Array.Empty<string>(), FlagArity.Repeatable, FlagScope.PerBranch, "mask", FlagStage.Pipeline));
        registry.Register(new FlagDef("--filter", System.Array.Empty<string>(), FlagArity.Repeatable, FlagScope.PerBranch, "filter", FlagStage.Pipeline));
        registry.Register(new FlagDef("--auto-column-types", System.Array.Empty<string>(), FlagArity.Boolean, FlagScope.PerBranch, "auto types", FlagStage.Reader));

        return new PipelineLexer(registry);
    }

    [Fact]
    public void Negative_Number_Consumed_As_Value_After_Scalar_Flag()
    {
        var pipeline = MakeLexer().Parse(new[] { "-i", "generate:5", "--sampling-seed", "-5", "--sampling-rate", "-0.5", "-o", "out.csv" });

        Assert.Equal("-5", pipeline.Globals.AllFlags["--sampling-seed"]);
        Assert.Equal("-0.5", pipeline.Globals.AllFlags["--sampling-rate"]);
    }

    [Fact]
    public void Dash_Leading_String_Consumed_As_Value_After_Scalar_Flag()
    {
        // This case distinguishes Strategy B from a numeric-shape heuristic:
        // "-###-" is not a number yet must be consumed as the mask pattern.
        var pipeline = MakeLexer().Parse(new[] { "-i", "generate:5", "--mask", "-###-", "-o", "out.csv" });

        Assert.Single(pipeline.Branches); // "-###-" must not have been mistaken for a flag/positional
        Assert.Contains("--mask", pipeline.Branches[0].RawArgs);
        Assert.Contains("-###-", pipeline.Branches[0].RawArgs);
        Assert.Equal(1, System.Array.IndexOf(pipeline.Branches[0].RawArgs, "-###-")
                     - System.Array.IndexOf(pipeline.Branches[0].RawArgs, "--mask"));
    }

    [Fact]
    public void Dash_Leading_Token_Is_Flag_After_Boolean_Flag()
    {
        // After a boolean flag, a dash-leading token is still a FLAG lookup, not a value.
        var pipeline = MakeLexer().Parse(new[] { "-i", "in.csv", "--auto-column-types", "--filter", "row.A > 1", "-o", "out.csv" });

        Assert.Single(pipeline.Branches);
        Assert.Contains("--filter", pipeline.Branches[0].RawArgs);
        Assert.Contains("row.A > 1", pipeline.Branches[0].RawArgs);
    }

    [Fact]
    public void OptionBinder_Consumes_DashLeading_Value_After_Scalar_Flag()
    {
        var registry = new FlagRegistry();
        registry.Register(new FlagDef("--sep", System.Array.Empty<string>(), FlagArity.Scalar, FlagScope.PerBranch, "separator"));

        var target = new BindTarget();
        OptionBinder.BindCli(target, new[] { "--sep", "-x" }, registry);

        Assert.Equal("-x", target.Sep);
    }

    [Fact]
    public void OptionBinder_Unknown_Flag_Strict_Throws_And_Lenient_Skips()
    {
        var registry = new FlagRegistry();
        var target = new BindTarget();

        OptionBinder.BindCli(target, new[] { "--nope" }, registry); // lenient: skipped

        Assert.Throws<InvalidOperationException>(() =>
            OptionBinder.BindCli(target, new[] { "--nope" }, registry, strict: true, lineFlags: LineFlags()));
    }

    /// <summary>
    /// The vocabulary of a whole command line: structural flags and engine controls (no component
    /// owner), plus one other component's option.
    /// </summary>
    private static FlagRegistry LineFlags()
    {
        var line = new FlagRegistry();
        CoreFlagRegistry.RegisterCoreFlags(line);
        foreach (var def in new DtPipe.Cli.Infrastructure.PipelineOptionsCliContributor().GetFlagDefs())
            line.Register(def with { Stage = FlagStage.All });
        line.Register(new FlagDef("--ora-fetch-size", System.Array.Empty<string>(), FlagArity.Scalar,
            FlagScope.PerBranch, "fetch size", FlagStage.Reader, ComponentName: "ora"));
        return line;
    }

    [Fact]
    public void Strict_Accepts_The_Structural_Token_That_Opens_A_Stage_Slice()
    {
        // A reader slice always starts with -i and carries the engine controls written beside it.
        var registry = new FlagRegistry();
        registry.Register(new FlagDef("--sep", System.Array.Empty<string>(), FlagArity.Scalar, FlagScope.PerBranch, "separator"));

        var target = new BindTarget();
        OptionBinder.BindCli(target, new[] { "-i", "csv:x.csv", "--sep", ";", "--limit", "5" },
            registry, "csv", strict: true, lineFlags: LineFlags());

        Assert.Equal(";", target.Sep);
    }

    [Fact]
    public void A_Foreign_Scalar_Flag_Takes_Its_Value_Token_With_It()
    {
        // --limit swallows "--sep": without arity-aware skipping the next token would be read
        // as a flag of this component and bind the one after it.
        var registry = new FlagRegistry();
        registry.Register(new FlagDef("--sep", System.Array.Empty<string>(), FlagArity.Scalar, FlagScope.PerBranch, "separator"));

        var target = new BindTarget();
        OptionBinder.BindCli(target, new[] { "--limit", "--sep", ";" }, registry, "csv", lineFlags: LineFlags());

        Assert.Equal("", target.Sep);
    }

    private sealed class BindTarget : DtPipe.Core.Options.IOptionSet
    {
        public static string Prefix => "tgt";
        public static string DisplayName => "Target";
        [DtPipe.Core.Attributes.ComponentOption("--sep")]
        public string Sep { get; set; } = "";
    }
}
