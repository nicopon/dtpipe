using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Validation;

namespace DtPipe.Contracts;

/// <summary>What one column's change is, between two versions of a contract.</summary>
public enum ContractChangeKind
{
    Added,
    Removed,
    TypeChanged,
    TypeWidened,
    PrecisionReduced,
    TimezoneChanged,
    ExtensionRemoved,
    NullabilityRelaxed,
    NullabilityTightened,
}

/// <param name="Column">The column the change is about.</param>
/// <param name="Kind">What changed.</param>
/// <param name="Breaking">Whether a consumer the old contract satisfied is no longer satisfied.</param>
/// <param name="Detail">One line, naming both sides.</param>
public sealed record ContractChange(string Column, ContractChangeKind Kind, bool Breaking, string Detail);

/// <summary>
/// Compares two contracts in one direction: <b>does the new one still satisfy a consumer the old
/// one satisfied?</b>
///
/// <para>
/// The direction is the whole point and it is not symmetric. A column the producer adds is
/// harmless — a consumer ignores what it does not read. A column the producer drops is not, and
/// no amount of care on the consumer's side recovers it.
/// </para>
/// </summary>
/// <remarks>
/// The rules live here once, over Arrow types, because that is what a contract carries. Precision,
/// scale, timestamp zone and extension metadata have no CLR equivalent to compare — mapping to CLR
/// first would silently answer "decimal vs decimal: fine" for a change from
/// <c>decimal128:38:18</c> to <c>decimal128:9:2</c>.
///
/// <para>
/// Numeric widening is the exception, and it is <b>delegated</b> to
/// <see cref="SchemaCompatibilityAnalyzer.IsNumericUpcast"/> rather than restated. Two widening
/// tables would eventually disagree about whether a producer's change is breaking, and only one of
/// them gates a pull request.
/// </para>
/// </remarks>
public static class ContractDiff
{
    private const string ExtensionKey = "ARROW:extension:name";

    /// <summary>Every change from <paramref name="older"/> to <paramref name="newer"/>.</summary>
    public static IReadOnlyList<ContractChange> Compare(DataContract older, DataContract newer)
        => Compare(older.ToArrowSchema(), newer.ToArrowSchema());

    /// <summary>Every change from <paramref name="older"/> to <paramref name="newer"/>.</summary>
    public static IReadOnlyList<ContractChange> Compare(Schema older, Schema newer)
    {
        var changes = new List<ContractChange>();

        // By name, never by position: a writer resolves columns by name, so a reordering is not a
        // change. Comparing positionally would report every column of a reordered schema.
        var before = older.FieldsList.ToDictionary(f => f.Name, StringComparer.OrdinalIgnoreCase);
        var after = newer.FieldsList.ToDictionary(f => f.Name, StringComparer.OrdinalIgnoreCase);

        foreach (var name in before.Keys.OrderBy(n => n, StringComparer.Ordinal))
        {
            if (!after.TryGetValue(name, out var now))
            {
                changes.Add(new ContractChange(name, ContractChangeKind.Removed, true,
                    "dropped by the producer"));
                continue;
            }

            changes.AddRange(CompareField(before[name], now));
        }

        foreach (var name in after.Keys.OrderBy(n => n, StringComparer.Ordinal))
            if (!before.ContainsKey(name))
                changes.Add(new ContractChange(name, ContractChangeKind.Added, false,
                    $"new, {Describe(after[name].DataType)} — a consumer ignores what it does not read"));

        return changes;
    }

    /// <summary>True when nothing in <paramref name="changes"/> breaks a consumer.</summary>
    public static bool IsCompatible(IEnumerable<ContractChange> changes) => !changes.Any(c => c.Breaking);

    private static IEnumerable<ContractChange> CompareField(Field before, Field after)
    {
        // A producer that starts sending NULL where it never did breaks a consumer that relied on
        // it. The reverse only narrows what the producer emits, which no consumer can notice.
        if (!before.IsNullable && after.IsNullable)
            yield return new ContractChange(after.Name, ContractChangeKind.NullabilityRelaxed, true,
                "was never null, may now be null");
        else if (before.IsNullable && !after.IsNullable)
            yield return new ContractChange(after.Name, ContractChangeKind.NullabilityTightened, false,
                "may no longer be null");

        if (Extension(before) is { } wasExtension && Extension(after) != wasExtension)
            yield return new ContractChange(after.Name, ContractChangeKind.ExtensionRemoved, true,
                Extension(after) is { } nowExtension
                    ? $"extension type changed, {wasExtension} → {nowExtension}"
                    : $"extension type {wasExtension} dropped — the value arrives as raw storage");

        foreach (var change in CompareType(after.Name, before.DataType, after.DataType))
            yield return change;
    }

    private static IEnumerable<ContractChange> CompareType(string column, IArrowType before, IArrowType after)
    {
        if (Describe(before) == Describe(after)) yield break;

        // Decimals carry precision and scale, and losing either loses data. Checked before the
        // numeric lattice, which sees both sides as 'decimal' and would call this compatible.
        if (before is Decimal128Type or Decimal256Type && after is Decimal128Type or Decimal256Type)
        {
            var (wasPrecision, wasScale) = DecimalShape(before);
            var (nowPrecision, nowScale) = DecimalShape(after);
            if (nowPrecision < wasPrecision || nowScale < wasScale)
                yield return new ContractChange(column, ContractChangeKind.PrecisionReduced, true,
                    $"{Describe(before)} → {Describe(after)}, narrower");
            else
                yield return new ContractChange(column, ContractChangeKind.TypeWidened, false,
                    $"{Describe(before)} → {Describe(after)}, wider");
            yield break;
        }

        // A timestamp's zone is part of what the value MEANS, not of how wide it is: dropping it
        // turns an instant into a wall clock, and nothing downstream can tell which it received.
        if (before is TimestampType wasTimestamp && after is TimestampType nowTimestamp
            && !string.Equals(wasTimestamp.Timezone, nowTimestamp.Timezone, StringComparison.Ordinal))
        {
            yield return new ContractChange(column, ContractChangeKind.TimezoneChanged, true,
                string.IsNullOrEmpty(nowTimestamp.Timezone)
                    ? $"time zone {wasTimestamp.Timezone} dropped — an instant becomes a wall clock"
                    : $"time zone {wasTimestamp.Timezone ?? "none"} → {nowTimestamp.Timezone}");
            yield break;
        }

        var wasClr = ArrowTypeMapper.GetClrType(before);
        var nowClr = ArrowTypeMapper.GetClrType(after);

        if (wasClr != nowClr && SchemaCompatibilityAnalyzer.IsNumericUpcast(wasClr, nowClr))
        {
            yield return new ContractChange(column, ContractChangeKind.TypeWidened, false,
                $"{Describe(before)} → {Describe(after)}, wider");
            yield break;
        }

        yield return new ContractChange(column, ContractChangeKind.TypeChanged, true,
            $"{Describe(before)} → {Describe(after)}");
    }

    private static (int Precision, int Scale) DecimalShape(IArrowType type) => type switch
    {
        Decimal128Type d => (d.Precision, d.Scale),
        Decimal256Type d => (d.Precision, d.Scale),
        _ => (0, 0),
    };

    private static string? Extension(Field field)
        => field.Metadata is not null && field.Metadata.TryGetValue(ExtensionKey, out var name) ? name : null;

    /// <summary>
    /// The type as the contract spells it, so a message names what the file says rather than a
    /// .NET class the reader never saw.
    /// </summary>
    private static string Describe(IArrowType type)
    {
        var field = new Field("x", type, true);
        var json = ArrowSchemaSerializer.SerializeCompact(new Schema([field], null));
        var marker = "\"type\":\"";
        var start = json.IndexOf(marker, StringComparison.Ordinal) + marker.Length;
        var end = json.IndexOf('"', start);
        return json[start..end];
    }
}
