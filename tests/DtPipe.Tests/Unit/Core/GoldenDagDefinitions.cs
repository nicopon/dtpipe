using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Tests.Unit.Core;

public static class GoldenDagDefinitions
{
    // Cas 1 : Pipeline linéaire simple (1 seule branche, non-DAG)
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

    // Cas 2 : DAG 2 branches, source → terminal
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

    // Cas 3 : DAG avec SQL stream transformer (--sql + --from)
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

    // Cas 4 : Fan-out (tee) — une source, deux consommateurs
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

    // Cas 5 : SQL stream transformer avec ref (JOIN sémantique)
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

    // Cas 6 : Fan-out + SQL transformer consommant la même source (teste la résolution des alias fan-out)
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

    // Cas 7 : Merge (UNION ALL) — deux sources streamées via --from a,b --merge
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
