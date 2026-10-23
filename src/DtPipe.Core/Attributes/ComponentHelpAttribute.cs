using System;

namespace DtPipe.Core.Attributes;

/// <summary>
/// Specifies structured help information for a pipeline component options class (usage notes and syntax examples).
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public class ComponentHelpAttribute : Attribute
{
    public string UsageNotes { get; }
    public string[] Examples { get; }

    public ComponentHelpAttribute(string usageNotes = "", string[]? examples = null)
    {
        UsageNotes = usageNotes;
        Examples = examples ?? Array.Empty<string>();
    }
}
