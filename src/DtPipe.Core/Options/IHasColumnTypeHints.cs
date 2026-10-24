namespace DtPipe.Core.Options;

/// <summary>
/// Implemented by reader options that take explicit column types as <c>name:hint</c> pairs
/// (<c>--column-types</c>), in the spellings of <see cref="Helpers.TypeHelper"/>. The engine fills
/// them from a loaded schema when the options take no <see cref="IHasSchemaOverride"/>.
/// </summary>
public interface IHasColumnTypeHints
{
    string ColumnTypes { get; set; }
}
