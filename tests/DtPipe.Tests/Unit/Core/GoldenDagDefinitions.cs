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
                FromAlias = "src",
                SqlQuery = "SELECT * FROM src LIMIT 10",
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
                FromAlias = "src",
                Output = "csv:/tmp/consumer_a.csv",
                Arguments = Array.Empty<string>()
            },
            new BranchDefinition
            {
                Alias = "consumer_b",
                FromAlias = "src",
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
                FromAlias = "main_stream",
                RefAliases = new[] { "ref_data" },
                SqlQuery = "SELECT m.* FROM main_stream m JOIN ref_data r ON m.id = r.id",
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
                FromAlias = "src",
                Output = "csv:/tmp/sink_a.csv",
                Arguments = Array.Empty<string>()
            },
            new BranchDefinition
            {
                Alias = "result",
                FromAlias = "src",
                SqlQuery = "SELECT * FROM src",
                Output = "csv:/tmp/result.csv",
                Arguments = Array.Empty<string>()
            }
        }
    };
}
