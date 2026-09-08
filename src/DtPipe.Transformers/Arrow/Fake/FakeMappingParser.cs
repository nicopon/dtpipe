using System.Text.RegularExpressions;

namespace DtPipe.Transformers.Arrow.Fake;

/// <summary>
/// Parses --fake option mappings in format COLUMN:faker.method or COLUMN:{OTHER_COLUMN}.
/// Validates faker paths against FakerRegistry.
/// </summary>
public sealed partial class FakeMappingParser
{
	private readonly DtPipe.Transformers.Arrow.Fake.FakerRegistry _registry;
	private readonly Dictionary<string, string> _mappings = new(StringComparer.OrdinalIgnoreCase);

	// Regex to match {COLUMN_NAME} patterns - unified syntax
	[GeneratedRegex(@"\{([^{}]+)\}", RegexOptions.Compiled)]
	private static partial Regex TemplatePattern();

	public FakeMappingParser(DtPipe.Transformers.Arrow.Fake.FakerRegistry registry)
	{
		_registry = registry;
	}

	/// <summary>
	/// Gets the parsed mappings (column name -> faker path or template).
	/// </summary>
	public IReadOnlyDictionary<string, string> Mappings => _mappings;

	/// <summary>
	/// Indicates whether any mappings are configured.
	/// </summary>
	public bool HasMappings => _mappings.Count > 0;

	/// <summary>
	/// Parses a mapping string in format COLUMN:value.
	/// </summary>
	/// <summary>
	/// The dataset the caller already named, spelled out. Pointing at a listing instead names a
	/// surface that has to exist on every side the transformer is reachable from — the message
	/// sent people to '--fake-list', which is not a flag of this binary and never was.
	/// </summary>
	private InvalidOperationException UnknownMethod(string path, string datasetName)
	{
		var methods = _registry.ListAll()
			.Where(g => g.Dataset.Equals(datasetName, StringComparison.OrdinalIgnoreCase))
			.SelectMany(g => g.Methods.Select(m => $"{g.Dataset}.{m.Method}"))
			.ToList();

		return new InvalidOperationException(
			$"Unknown faker method '{path}' for dataset '{datasetName}'. "
		  + (methods.Count == 0
				? $"Dataset '{datasetName}' has no methods."
				: $"Available in '{datasetName}': {string.Join(", ", methods)}."));
	}

	/// <summary>
	/// A value this transformer cannot resolve to a generator is refused rather than written into
	/// every row. Silence made a Bogus method name given without its dataset — 'firstName' for
	/// 'name.firstName' — fill the column with that word, and a recorded session shipped four
	/// columns of 'firstName', 'lastName', 'safeEmail' and 'membership' as anonymised data.
	/// Writing a constant is another transformer's job.
	/// </summary>
	private InvalidOperationException UnknownPath(string path) =>
		new($"Unknown faker path '{path}'. A 'fake' mapping value is '<dataset>.<method>' "
		  + "(e.g. 'name.firstName'), optionally suffixed '#variant', or a '{Column}' template. "
		  + $"Datasets: {string.Join(", ", _registry.ListAll().Select(g => g.Dataset))}.");

	public void Parse(string mapping)
	{
		// Format: COLUMN:dataset.method or COLUMN:{OTHER_COLUMN} template
		var separatorIndex = mapping.IndexOf(':');
		if (separatorIndex <= 0 || separatorIndex >= mapping.Length - 1)
		{
			Console.Error.WriteLine($"Warning: Invalid mapping format '{mapping}'. Expected 'COLUMN:value'");
			return;
		}

		var column = mapping[..separatorIndex].Trim();
		var value = mapping[(separatorIndex + 1)..].Trim();

		if (string.IsNullOrEmpty(column) || string.IsNullOrEmpty(value))
		{
			Console.Error.WriteLine($"Warning: Invalid mapping '{mapping}'. Column and value cannot be empty.");
			return;
		}

		// For templates, store as-is
		if (IsTemplate(value))
		{
			_mappings[column] = value;
			return;
		}

		// For fakers/strings, apply validation logic
		// Extract variant suffix (#xxx) if present - used for same-faker different values
		var hashIndex = value.IndexOf('#');
		var baseFakerPath = hashIndex >= 0 ? value[..hashIndex] : value;
		var variant = hashIndex >= 0 ? value[(hashIndex + 1)..] : null;

		var parts = baseFakerPath.Split('.', 2);
		var datasetName = parts.Length > 0 ? parts[0] : string.Empty;

		if (_registry.HasDataset(datasetName))
		{
			if (!_registry.HasGenerator(baseFakerPath))
				throw UnknownMethod(baseFakerPath, datasetName);

			// Store full path including variant for distinct hashing
			_mappings[column] = value;
			return;
		}

		// A colon where the dot belongs ("finance:iban") spells the same path.
		if (baseFakerPath.Contains(':'))
		{
			var normalized = baseFakerPath.Replace(':', '.');
			if (_registry.HasGenerator(normalized))
			{
				// Keep variant if present
				_mappings[column] = variant is not null ? $"{normalized}#{variant}" : normalized;
				return;
			}

			var normalizedDataset = normalized.Split('.', 2)[0];
			throw _registry.HasDataset(normalizedDataset)
				? UnknownMethod(normalized, normalizedDataset)
				: UnknownPath(baseFakerPath);
		}

		throw UnknownPath(baseFakerPath);
	}

	/// <summary>
	/// Parses multiple mappings.
	/// </summary>
	/// <summary>
	/// Parses every mapping and reports every one it rejects, not the first.
	///
	/// <para>
	/// Stopping at the first cost a round trip per wrong path. A recorded session spent five of its
	/// eleven iterations discovering faker paths one dataset at a time — seven mappings, five of
	/// them wrong — and answered the fifth refusal by abandoning 'fake' for JavaScript, which
	/// accepts anything.
	/// </para>
	/// </summary>
	public void ParseAll(IEnumerable<string>? mappings)
	{
		if (mappings is null) return;

		List<string>? refused = null;
		foreach (var mapping in mappings)
		{
			try
			{
				Parse(mapping);
			}
			catch (InvalidOperationException ex)
			{
				(refused ??= []).Add(ex.Message);
			}
		}

		if (refused is { Count: > 0 })
			throw new InvalidOperationException(string.Join(" ", refused));
	}

	/// <summary>
	/// Determines if a value is a template (contains {COLUMN} references).
	/// </summary>
	public static bool IsTemplate(string value) => TemplatePattern().IsMatch(value);

	/// <summary>
	/// Extracts referenced column names from a template.
	/// </summary>
	public static HashSet<string> ExtractReferencedColumns(string template)
	{
		var matches = TemplatePattern().Matches(template);
		var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		foreach (Match match in matches)
		{
			result.Add(match.Groups[1].Value);
		}
		return result;
	}
}
