namespace DtPipe.Core.Options;

/// <summary>
/// Helper to extract the option type from a component implementing IRequiresOptions.
/// </summary>
public static class ComponentOptionsHelper
{
	public static Type GetOptionsType<TComponent>()
	{
		var type = typeof(TComponent);
		var interfaceType = type.GetInterfaces()
			.FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IRequiresOptions<>));

		if (interfaceType == null)
		{
			throw new InvalidOperationException($"Type {type.Name} does not implement IRequiresOptions<T>");
		}

		return interfaceType.GetGenericArguments()[0];
	}
}

/// <summary>
/// Marker interface for components that require specific CLI options.
/// </summary>
public interface IRequiresOptions<TOptions> where TOptions : class, IOptionSet, new() { }

/// <summary>
/// Base interface for any option set.
/// </summary>
public interface IOptionSet
{
	/// <summary>
	/// The CLI prefix for this option set (e.g. "ora", "csv").
	/// </summary>
	static abstract string Prefix { get; }

	/// <summary>
	/// Display name for help grouping (e.g. "Oracle Reader", "CSV Writer").
	/// </summary>
	static abstract string DisplayName { get; }
}

/// <summary>
/// Options specific to a data provider (Oracle, SQL Server, etc.).
/// </summary>
public interface IProviderOptions : IOptionSet { }

/// <summary>
/// Options specific to a data writer (CSV, Parquet, etc.).
/// </summary>
public interface IWriterOptions : IOptionSet { }

/// <summary>
/// Options specific to a data transformer (Faker, etc.).
/// </summary>
public interface ITransformerOptions : IOptionSet { }

/// <summary>
/// Implemented by reader options that accept a SQL query.
/// Allows CliStreamReaderFactory to propagate the global --query without reflection.
/// </summary>
public interface IQueryAwareOptions
{
	string? Query { get; set; }
}

/// <summary>
/// Implemented by writer options that accept a keyring alias.
/// Allows JobService to propagate the global --key without reflection.
/// </summary>
public interface IKeyAwareOptions
{
	string? Key { get; set; }
}
