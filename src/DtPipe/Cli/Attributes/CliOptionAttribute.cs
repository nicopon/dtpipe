using System;

namespace DtPipe.Cli.Attributes;

[AttributeUsage(AttributeTargets.Property)]
public class CliOptionAttribute : Attribute
{
    public string? Name { get; }
    public string? Description { get; set; }
    public string[]? Aliases { get; set; }

    public CliOptionAttribute() { }

    public CliOptionAttribute(string name)
    {
        Name = name;
    }
}
