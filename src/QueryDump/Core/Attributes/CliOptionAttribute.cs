using System;

namespace QueryDump.Core.Attributes;

[AttributeUsage(AttributeTargets.Property)]
public class CliOptionAttribute : Attribute
{
    public string Name { get; }
    public string? Description { get; set; }

    public CliOptionAttribute(string name)
    {
        Name = name;
    }
}
