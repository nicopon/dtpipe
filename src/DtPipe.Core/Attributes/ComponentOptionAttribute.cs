using System;

namespace DtPipe.Core.Attributes;

[AttributeUsage(AttributeTargets.Property)]
public class ComponentOptionAttribute : Attribute
{
	public string? Name { get; }
	public string? Description { get; set; }
	public string[]? Aliases { get; set; }
	public bool Hidden { get; set; }
	public bool Required { get; set; }

	public ComponentOptionAttribute() { }

	public ComponentOptionAttribute(string name)
	{
		Name = name;
	}
}
