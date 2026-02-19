using DtPipe.Core.Models;

namespace DtPipe.DryRun;

/// <summary>
/// Observes runtime CLR types during Dry Run to provide performance optimization hints
/// for JS transformers.
/// </summary>
public class DynamicTypeObserver
{
    // Map of columnName -> set of observed runtime CLR types
    private readonly Dictionary<string, HashSet<Type>> _observedTypes = new(StringComparer.OrdinalIgnoreCase);

    public void ObserveRow(IReadOnlyList<PipeColumnInfo> schema, object?[] row)
    {
        for (int i = 0; i < schema.Count && i < row.Length; i++)
        {
            var val = row[i];
            if (val != null)
            {
                var runtimeType = val.GetType();
                var declaredType = schema[i].ClrType;

                // Identify columns declared as string but containing other primitive types.
                if (declaredType == typeof(string) && runtimeType != typeof(string))
                {
                    var colName = schema[i].Name;
                    if (!_observedTypes.TryGetValue(colName, out var types))
                    {
                        types = new HashSet<Type>();
                        _observedTypes[colName] = types;
                    }
                    types.Add(runtimeType);
                }
            }
        }
    }

    /// <summary>
    /// Generates strictly consistent type hints for columns.
    /// If a column has multiple conflicting observed types, it will not yield a hint.
    /// </summary>
    public IReadOnlyDictionary<string, string> GenerateHints()
    {
        var hints = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var (colName, types) in _observedTypes)
        {
            if (types.Count == 1)
            {
                var type = types.First();
                var hint = GetTypeHintFromClrType(type);
                if (hint != null)
                {
                    hints[colName] = hint;
                }
            }
        }

        return hints;
    }

    private static string? GetTypeHintFromClrType(Type type)
    {
        if (type == typeof(int)) return "int";
        if (type == typeof(long)) return "long";
        if (type == typeof(double)) return "double";
        if (type == typeof(decimal)) return "decimal";
        if (type == typeof(bool)) return "bool";
        if (type == typeof(DateTime)) return "datetime";
        if (type == typeof(Guid)) return "guid";
        return null;
    }
}
