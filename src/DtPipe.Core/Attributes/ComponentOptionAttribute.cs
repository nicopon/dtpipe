using System;

namespace DtPipe.Core.Attributes;

[AttributeUsage(AttributeTargets.Property)]
public class ComponentOptionAttribute : Attribute
{
    public string? Name { get; }
    public string? Description { get; set; }
    public string[]? Aliases { get; set; }
    /// <summary>Never read: the help lists every option.</summary>
    [Obsolete("Never read: every option is listed in the help. Remove the argument.")]
    public bool Hidden { get; set; }
    public bool Required { get; set; }

    public ComponentOptionAttribute() { }

    public ComponentOptionAttribute(string name)
    {
        Name = name;
    }
}
