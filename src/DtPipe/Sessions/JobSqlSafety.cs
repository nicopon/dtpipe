using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Security;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

namespace DtPipe.Sessions;

/// <summary>
/// The safety check of a job that is about to run for real: <c>execute-yaml-job</c> and the studio's run.
///
/// It classifies the SQL the job carries, never the YAML around it. A key, a comment or a transformer
/// option (<c>drop: Col</c> of <c>project</c>) is not SQL, and a text scan reads them as verbs. The
/// values come from the options each provider block binds to, so the rule that picks them is
/// <see cref="SampleModeSafetyGate"/>'s, and the policy that judges them is the one the sample run uses.
///
/// Unlike a sample run, a real run executes the writer's hooks, so they are classified too. The jobs
/// are the parsed ones, whose <c>@file</c> references and <c>${{…}}</c> are already resolved: the text
/// judged is the text that runs, and a file that cannot be read stops the parse before this check.
/// </summary>
public static class JobSqlSafety
{
    public static SqlSafetyResult Evaluate(
        IReadOnlyDictionary<string, JobDefinition> jobs,
        IEnumerable<IStreamReaderFactory> readerFactories,
        IEnumerable<IDataWriterFactory> writerFactories,
        ISqlSafetyPolicy policy,
        SqlSafetyOptions options)
    {
        var violations = new List<string>();

        var factories = readerFactories.Cast<IDataFactory>().Concat(writerFactories).ToList();

        // An endpoint is a connection string: it reaches the network on its own (s3://), and a verb
        // spelled inside it (Database=update) is a name, not a statement.
        var endpointOptions = new SqlSafetyOptions { AllowDestructive = true, AllowNetwork = options.AllowNetwork };

        foreach (var (alias, job) in jobs)
        {
            foreach (var endpoint in new[] { job.Input, job.Output })
                if (!string.IsNullOrWhiteSpace(endpoint)) violations.AddRange(policy.Analyze(endpoint, endpointOptions).Violations);

            foreach (var (block, values) in job.ProviderOptions ?? new())
            {
                if (values is null) continue;

                foreach (var value in SqlValuesOf(block, values, factories))
                    violations.AddRange(SampleModeSafetyGate.Evaluate(policy, options, new[] { value }, dialect: null).Violations);
            }
        }

        return violations.Count == 0 ? SqlSafetyResult.Ok() : SqlSafetyResult.Blocked(violations.Distinct().ToArray());
    }

    /// <summary>
    /// The SQL-bearing values of one <c>provider-options</c> block. A block that binds to a reader or a
    /// writer yields what those options declare as SQL; one that binds to nothing (a stream processor's,
    /// or a component this process does not know) or that does not bind yields all its text values,
    /// since what it holds cannot be told apart.
    /// </summary>
    private static IEnumerable<string?> SqlValuesOf(
        string block, Dictionary<string, object?> values, IReadOnlyList<IDataFactory> factories)
    {
        var (component, writerOnly, readerOnly) =
            block.EndsWith("-writer", StringComparison.OrdinalIgnoreCase) ? (block[..^7], true, false)
            : block.EndsWith("-reader", StringComparison.OrdinalIgnoreCase) ? (block[..^7], false, true)
            : (block, false, false);

        var targets = factories
            .Where(f => f.ComponentName.Equals(component, StringComparison.OrdinalIgnoreCase))
            .Where(f => !(writerOnly && f is not IDataWriterFactory) && !(readerOnly && f is IDataWriterFactory))
            .ToList();

        if (targets.Count == 0)
            return TextValues(values);

        var bound = new List<string?>();
        foreach (var target in targets)
        {
            try
            {
                var instance = Activator.CreateInstance(target.OptionsType)!;
                OptionBinder.BindYaml(instance, values, strict: false, ignoreUnknownKeys: true);
                bound.AddRange(SampleModeSafetyGate.SqlBearingValues(instance, includeWriterHooks: target is IDataWriterFactory));
            }
            catch (Exception)
            {
                return TextValues(values);
            }
        }
        return bound;
    }

    private static IEnumerable<string?> TextValues(Dictionary<string, object?> values) =>
        values.Values.OfType<string>();
}
