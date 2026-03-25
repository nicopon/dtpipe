using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Tests.Unit.Core;

public static class GoldenDagDefinitions
{
    // Case 1: Simple linear pipeline (single branch, non-DAG)
    public static JobDagDefinition Linear_SingleBranch => new()
    {
        Branches = new[]
        {
            new BranchDefinition
            {
                Alias = "main",
                Input = "generate:10",
                Output = "csv:/tmp/out.csv",
                Arguments = Array.Empty<string>()
            }
        }
    };

    // Case 2: DAG with 2 branches, source → terminal
    public static JobDagDefinition Dag_TwoInputs_OneOutput => new()
    {
        Branches = new[]
        {
            new BranchDefinition
            {
                Alias = "source1",
                Input = "generate:5",
                Output = null,
                Arguments = Array.Empty<string>()
            },
            new BranchDefinition
            {
                Alias = "output1",
                Input = "generate:5",
                Output = "csv:/tmp/out.csv",
                Arguments = Array.Empty<string>()
            }
        }
    };

    // Case 3: DAG with SQL stream transformer (--sql + --from)
    public static JobDagDefinition Dag_SourcePlusSqlProcessor => new()
    {
        Branches = new[]
        {
            new BranchDefinition
            {
                Alias = "src",
                Input = "generate:100",
                Output = null,
                Arguments = Array.Empty<string>()
            },
            new BranchDefinition
            {
                Alias = "processed",
                StreamingAliases = new[] { "src" },
                ProcessorName = "sql",
                Output = "csv:/tmp/processed.csv",
                Arguments = Array.Empty<string>()
            }
        }
    };

    // Case 4: Fan-out (tee) — one source, two consumers
    public static JobDagDefinition Dag_FanOut_OneSourceTwoConsumers => new()
    {
        Branches = new[]
        {
            new BranchDefinition
            {
                Alias = "src",
                Input = "generate:50",
                Output = null,
                Arguments = Array.Empty<string>()
            },
            new BranchDefinition
            {
                Alias = "consumer_a",
                StreamingAliases = new[] { "src" },
                Output = "csv:/tmp/consumer_a.csv",
                Arguments = Array.Empty<string>()
            },
            new BranchDefinition
            {
                Alias = "consumer_b",
                StreamingAliases = new[] { "src" },
                Output = "csv:/tmp/consumer_b.csv",
                Arguments = Array.Empty<string>()
            }
        }
    };

    // Case 5: SQL stream transformer with ref (semantic JOIN)
    public static JobDagDefinition Dag_SqlProcessor_WithRef => new()
    {
        Branches = new[]
        {
            new BranchDefinition { Alias = "main_stream", Input = "generate:100", Output = null, Arguments = Array.Empty<string>() },
            new BranchDefinition { Alias = "ref_data",    Input = "generate:10",  Output = null, Arguments = Array.Empty<string>() },
            new BranchDefinition
            {
                Alias = "result",
                StreamingAliases = new[] { "main_stream" },
                RefAliases = new[] { "ref_data" },
                ProcessorName = "sql",
                Output = "csv:/tmp/result.csv",
                Arguments = Array.Empty<string>()
            }
        }
    };

    // Case 6: Fan-out + SQL transformer consuming the same source (tests fan-out alias resolution)
    public static JobDagDefinition Dag_FanOut_WithSqlProcessor => new()
    {
        Branches = new[]
        {
            new BranchDefinition
            {
                Alias = "src",
                Input = "generate:50",
                Output = null,
                Arguments = Array.Empty<string>()
            },
            new BranchDefinition
            {
                Alias = "sink_a",
                StreamingAliases = new[] { "src" },
                Output = "csv:/tmp/sink_a.csv",
                Arguments = Array.Empty<string>()
            },
            new BranchDefinition
            {
                Alias = "result",
                StreamingAliases = new[] { "src" },
                ProcessorName = "sql",
                Output = "csv:/tmp/result.csv",
                Arguments = Array.Empty<string>()
            }
        }
    };

    // Case 7: Merge (UNION ALL) — two sources streamed via --from a,b --merge
    public static JobDagDefinition Dag_Merge_TwoSources => new()
    {
        Branches = new[]
        {
            new BranchDefinition { Alias = "stream_a", Input = "generate:5", Output = null, Arguments = Array.Empty<string>() },
            new BranchDefinition { Alias = "stream_b", Input = "generate:5", Output = null, Arguments = Array.Empty<string>() },
            new BranchDefinition
            {
                Alias = "merged",
                StreamingAliases = new[] { "stream_a", "stream_b" },
                ProcessorName = "merge",
                Output = "csv:/tmp/merged.csv",
                Arguments = Array.Empty<string>()
            }
        }
    };
}
