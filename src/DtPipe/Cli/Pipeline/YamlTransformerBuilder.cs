using System;
using System.Linq;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Pipelines;

namespace DtPipe.Cli.Pipeline;

/// <summary>
/// Builds a transformer from a YAML <c>transformers:</c> entry, in two halves.
///
/// <para>
/// The <c>mappings:</c> half is per-transformer — keys only for <c>null</c>, "key:value" pairs for
/// <c>compute</c>, values joined into a script for <c>window</c> — so each factory encodes its own
/// (<see cref="IDataTransformerFactory.CreateOptionsFromYaml"/>). The <c>options:</c> half is not:
/// it is bound here by <see cref="OptionBinder.BindYaml"/>, against the very properties
/// <c>get-transformer-help</c> prints.
/// </para>
///
/// <para>
/// Both halves used to live in each factory as a hand-written list of key names, and the list drifted
/// from the properties the help advertises: <c>compute-types</c> worked on the command line, bound
/// nothing in YAML, and warned about neither — while an unknown key was dropped without a word.
/// Binding reflectively is what keeps the help and the loader describing one surface.
/// </para>
/// </summary>
public static class YamlTransformerBuilder
{
    public static IDataTransformer? Build(IDataTransformerFactory factory, TransformerConfig config, bool strict = false)
    {
        var options = factory.CreateOptionsFromYaml(config);
        if (options is null)
        {
            // A block that sets options and produces nothing is refused, not skipped — even outside
            // strict mode, and unlike an unrecognised key, which leaves the rest of the block
            // working. Here the whole transformer disappears, so the pipeline that runs is not the
            // one that was written, and nothing afterwards can show the difference.
            if (config.Options is { Count: > 0 })
                throw new InvalidOperationException(
                    $"Transformer '{config.Type}' sets options ({string.Join(", ", config.Options.Keys)}) "
                  + "but has no 'mappings:', so it produces no transformer. This transformer is "
                  + "configured through 'mappings:'; the options block only refines it.");

            return null;
        }

        if (config.Options is { Count: > 0 })
            OptionBinder.BindYaml(options, config.Options.ToDictionary(kv => kv.Key, kv => (object?)kv.Value), strict);

        return factory.CreateFromOptions(options);
    }
}
