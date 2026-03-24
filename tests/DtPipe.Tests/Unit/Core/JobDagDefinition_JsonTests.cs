using DtPipe.Core.Pipelines.Dag;
using DtPipe.Tests.Unit.Core;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

public class JobDagDefinition_JsonTests
{
    private static void EnsureDirectory(string path)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
            Directory.CreateDirectory(dir);
    }

    private void WriteGoldenJson(string name, JobDagDefinition dag)
    {
        var repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
        var path = Path.Combine(repoRoot, "tests", "DtPipe.Tests", "Data", "GoldenDags", $"{name}.json");
        // Note: In actual CI we wouldn't write, but here we use it to bootstrap the Golden files
        EnsureDirectory(path);
        File.WriteAllText(path, dag.ToJson());
    }

    [Fact]
    public void GenerateAndVerify_GoldenJsons()
    {
        // Bootstrap Phase: Generate the files
        WriteGoldenJson("linear_single_branch", GoldenDagDefinitions.Linear_SingleBranch);
        WriteGoldenJson("dag_two_inputs", GoldenDagDefinitions.Dag_TwoInputs_OneOutput);
        WriteGoldenJson("dag_sql_processor", GoldenDagDefinitions.Dag_SourcePlusSqlProcessor);
        WriteGoldenJson("dag_fanout", GoldenDagDefinitions.Dag_FanOut_OneSourceTwoConsumers);
        WriteGoldenJson("dag_sql_with_ref", GoldenDagDefinitions.Dag_SqlProcessor_WithRef);

        // Verification Phase: Round-trip
        VerifyRoundTrip(GoldenDagDefinitions.Linear_SingleBranch);
        VerifyRoundTrip(GoldenDagDefinitions.Dag_TwoInputs_OneOutput);
        VerifyRoundTrip(GoldenDagDefinitions.Dag_SourcePlusSqlProcessor);
        VerifyRoundTrip(GoldenDagDefinitions.Dag_FanOut_OneSourceTwoConsumers);
        VerifyRoundTrip(GoldenDagDefinitions.Dag_SqlProcessor_WithRef);
    }

    private void VerifyRoundTrip(JobDagDefinition original)
    {
        var json = original.ToJson();
        var deserialized = JobDagDefinition.FromJson(json);
        
        Assert.NotNull(deserialized);
        Assert.Equal(original.Branches.Count, deserialized.Branches.Count);
        for (int i = 0; i < original.Branches.Count; i++)
        {
            Assert.Equal(original.Branches[i].Alias, deserialized.Branches[i].Alias);
            Assert.Equal(original.Branches[i].Input, deserialized.Branches[i].Input);
            Assert.Equal(original.Branches[i].Output, deserialized.Branches[i].Output);
            Assert.Equal(original.Branches[i].StreamingAliases, deserialized.Branches[i].StreamingAliases);
            Assert.Equal(original.Branches[i].ProcessorName, deserialized.Branches[i].ProcessorName);
            Assert.Equal(original.Branches[i].HasStreamTransformer, deserialized.Branches[i].HasStreamTransformer);
        }
    }
}
