using System;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// A generic descriptor for any pipeline component (Adapter, Transformer, Processor),
/// providing its identity and configuration shape decoupled from the invocation medium (CLI/YAML).
/// </summary>
public interface IComponentDescriptor
{
    /// <summary>
    /// The unique name of the component, such as 'csv', 'fake', or 'native-join'.
    /// Used as the identifier in configuration files and the CLI prefix.
    /// </summary>
    string ComponentName { get; }

    /// <summary>
    /// The category grouping of this component, e.g., 'Readers', 'Writers', 'Transformers'.
    /// </summary>
    string Category { get; }

    /// <summary>
    /// The underlying configuration schema typifying the options this component accepts.
    /// Must implement IOptionSet.
    /// </summary>
    Type OptionsType { get; }
}
