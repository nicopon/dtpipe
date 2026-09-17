using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Contracts;
using Xunit;

namespace DtPipe.Tests.Unit.Contracts;

/// <summary>
/// The diff's rules, one test each.
///
/// <para>
/// It answers one question — <i>does the new contract still satisfy a consumer the old one
/// satisfied?</i> — and the question is not symmetric. Every case below fixes which side of the
/// asymmetry a change falls on, because getting one backwards is invisible: the command still
/// prints a table and still exits with a code, just the wrong one.
/// </para>
/// </summary>
public class ContractDiffTests
{
    private static Schema Of(params Field[] fields) => new(fields, null);

    private static Field F(string name, IArrowType type, bool nullable = true,
                           IReadOnlyDictionary<string, string>? metadata = null)
        => new(name, type, nullable, metadata);

    private static ContractChange Single(Schema before, Schema after)
        => Assert.Single(ContractDiff.Compare(before, after));

    // ── what a consumer survives ─────────────────────────────────────────────

    [Fact]
    public void An_Added_Column_Is_Not_Breaking()
    {
        var change = Single(
            Of(F("id", Int32Type.Default)),
            Of(F("id", Int32Type.Default), F("extra", StringType.Default)));

        Assert.Equal(ContractChangeKind.Added, change.Kind);
        Assert.False(change.Breaking);
    }

    [Fact]
    public void Reordering_Is_Not_A_Change_At_All()
    {
        var changes = ContractDiff.Compare(
            Of(F("a", Int32Type.Default), F("b", StringType.Default)),
            Of(F("b", StringType.Default), F("a", Int32Type.Default)));

        Assert.Empty(changes);
    }

    [Fact]
    public void A_Widened_Number_Is_Not_Breaking()
    {
        var change = Single(Of(F("n", Int32Type.Default)), Of(F("n", Int64Type.Default)));

        Assert.Equal(ContractChangeKind.TypeWidened, change.Kind);
        Assert.False(change.Breaking);
    }

    [Fact]
    public void A_Producer_That_Stops_Sending_Null_Is_Not_Breaking()
    {
        var change = Single(
            Of(F("id", Int32Type.Default, nullable: true)),
            Of(F("id", Int32Type.Default, nullable: false)));

        Assert.Equal(ContractChangeKind.NullabilityTightened, change.Kind);
        Assert.False(change.Breaking);
    }

    // ── what a consumer does not survive ─────────────────────────────────────

    [Fact]
    public void A_Removed_Column_Is_Breaking()
    {
        var change = Single(
            Of(F("id", Int32Type.Default), F("gone", StringType.Default)),
            Of(F("id", Int32Type.Default)));

        Assert.Equal(ContractChangeKind.Removed, change.Kind);
        Assert.True(change.Breaking);
    }

    /// <summary>A rename is a removal plus an addition, and the diff does not guess otherwise.</summary>
    [Fact]
    public void A_Renamed_Column_Reads_As_Removed_Plus_Added()
    {
        var changes = ContractDiff.Compare(
            Of(F("customer_id", Int32Type.Default)),
            Of(F("client_id", Int32Type.Default)));

        Assert.Equal(2, changes.Count);
        Assert.Contains(changes, c => c.Kind == ContractChangeKind.Removed && c.Breaking);
        Assert.Contains(changes, c => c.Kind == ContractChangeKind.Added && !c.Breaking);
        Assert.False(ContractDiff.IsCompatible(changes));
    }

    [Fact]
    public void A_Narrowed_Type_Is_Breaking()
    {
        var change = Single(Of(F("n", Int64Type.Default)), Of(F("n", Int32Type.Default)));

        Assert.Equal(ContractChangeKind.TypeChanged, change.Kind);
        Assert.True(change.Breaking);
    }

    [Fact]
    public void A_Change_Of_Family_Is_Breaking()
    {
        var change = Single(Of(F("n", Int32Type.Default)), Of(F("n", StringType.Default)));

        Assert.Equal(ContractChangeKind.TypeChanged, change.Kind);
        Assert.True(change.Breaking);
    }

    [Fact]
    public void A_Producer_That_Starts_Sending_Null_Is_Breaking()
    {
        var change = Single(
            Of(F("id", Int32Type.Default, nullable: false)),
            Of(F("id", Int32Type.Default, nullable: true)));

        Assert.Equal(ContractChangeKind.NullabilityRelaxed, change.Kind);
        Assert.True(change.Breaking);
    }

    /// <summary>
    /// Both sides map to <c>decimal</c>, so the numeric lattice alone would call this compatible.
    /// </summary>
    [Fact]
    public void A_Reduced_Precision_Is_Breaking()
    {
        var change = Single(
            Of(F("amount", new Decimal128Type(38, 18))),
            Of(F("amount", new Decimal128Type(9, 2))));

        Assert.Equal(ContractChangeKind.PrecisionReduced, change.Kind);
        Assert.True(change.Breaking);
    }

    [Fact]
    public void A_Widened_Decimal_Is_Not_Breaking()
    {
        var change = Single(
            Of(F("amount", new Decimal128Type(9, 2))),
            Of(F("amount", new Decimal128Type(38, 18))));

        Assert.Equal(ContractChangeKind.TypeWidened, change.Kind);
        Assert.False(change.Breaking);
    }

    /// <summary>
    /// Dropping the zone turns an instant into a wall clock. Both sides are still
    /// <c>timestamp</c>, so nothing but an explicit rule catches it.
    /// </summary>
    [Fact]
    public void Dropping_A_Timestamp_Zone_Is_Breaking()
    {
        var change = Single(
            Of(F("at", new TimestampType(TimeUnit.Microsecond, "UTC"))),
            Of(F("at", new TimestampType(TimeUnit.Microsecond, (string?)null))));

        Assert.Equal(ContractChangeKind.TimezoneChanged, change.Kind);
        Assert.True(change.Breaking);
    }

    /// <summary>
    /// Losing <c>arrow.uuid</c> leaves the same 16 bytes and a consumer that no longer gets a
    /// <c>Guid</c> — the storage is identical, which is exactly why this needs its own rule.
    /// </summary>
    [Fact]
    public void Dropping_An_Extension_Type_Is_Breaking()
    {
        var uuid = new Dictionary<string, string> { ["ARROW:extension:name"] = "arrow.uuid" };
        var changes = ContractDiff.Compare(
            Of(F("id", new FixedSizeBinaryType(16), metadata: uuid)),
            Of(F("id", new FixedSizeBinaryType(16))));

        var change = Assert.Single(changes);
        Assert.Equal(ContractChangeKind.ExtensionRemoved, change.Kind);
        Assert.True(change.Breaking);
    }

    // ── the verdict ──────────────────────────────────────────────────────────

    [Fact]
    public void An_Unchanged_Contract_Has_Nothing_To_Report()
    {
        var schema = Of(F("id", Int32Type.Default), F("label", StringType.Default));
        Assert.Empty(ContractDiff.Compare(schema, schema));
        Assert.True(ContractDiff.IsCompatible(ContractDiff.Compare(schema, schema)));
    }

    [Fact]
    public void One_Breaking_Change_Among_Harmless_Ones_Decides()
    {
        var changes = ContractDiff.Compare(
            Of(F("id", Int32Type.Default), F("gone", StringType.Default)),
            Of(F("id", Int64Type.Default), F("added", StringType.Default)));

        Assert.False(ContractDiff.IsCompatible(changes));
    }
}
