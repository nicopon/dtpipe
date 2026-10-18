using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Processors.Merge;
using Xunit;

namespace DtPipe.Tests.Unit.Processors;

public class MergeTransformerSchemaTests
{
    private static Schema Of(params (string Name, IArrowType Type)[] columns)
    {
        var builder = new Schema.Builder();
        foreach (var (name, type) in columns)
            builder.Field(new Field(name, type, true));
        return builder.Build();
    }

    private static MergeTransformer Merge(Schema first, Schema second)
    {
        var registry = new MemoryChannelRegistry();
        foreach (var (alias, schema) in new[] { ("a", first), ("b", second) })
        {
            registry.RegisterArrowChannel(alias, Channel.CreateUnbounded<RecordBatch>(), schema);
            registry.UpdateArrowChannelSchema(alias, schema);
        }
        return new MergeTransformer(registry, ["a", "b"]);
    }

    [Fact]
    public async Task Open_SameColumns_TakesTheFirstChannelsSchema()
    {
        var first = Of(("id", Int32Type.Default), ("name", StringType.Default));
        var merge = Merge(first, Of(("id", Int32Type.Default), ("name", StringType.Default)));

        await merge.OpenAsync();

        Assert.Same(first, merge.Schema);
        Assert.Equal(["id", "name"], merge.Columns!.Select(c => c.Name));
    }

    [Fact]
    public async Task Open_TimestampsThatDifferOnlyByZone_AreTheSameColumn()
    {
        var merge = Merge(
            Of(("at", new TimestampType(TimeUnit.Microsecond, (string?)null))),
            Of(("at", new TimestampType(TimeUnit.Microsecond, "UTC"))));

        await merge.OpenAsync();
    }

    [Fact]
    public async Task Open_ColumnOfAnotherType_NamesTheColumnAndTheChannels()
    {
        var merge = Merge(
            Of(("a", Int32Type.Default), ("b", StringType.Default)),
            Of(("a", StringType.Default), ("b", new DoubleType())));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => merge.OpenAsync());

        Assert.Contains("--merge", ex.Message);
        Assert.Contains("column 'a'", ex.Message);
        Assert.Contains("'a'", ex.Message);
        Assert.Contains("'b'", ex.Message);
    }

    [Fact]
    public async Task Open_TimestampsOfAnotherUnit_AreRefused()
    {
        var merge = Merge(
            Of(("at", new TimestampType(TimeUnit.Microsecond, (string?)null))),
            Of(("at", new TimestampType(TimeUnit.Nanosecond, (string?)null))));

        await Assert.ThrowsAsync<InvalidOperationException>(() => merge.OpenAsync());
    }

    [Fact]
    public async Task Open_NestedTypesWithAnotherChild_AreRefused()
    {
        var merge = Merge(
            Of(("tags", new ListType(new Field("item", StringType.Default, true)))),
            Of(("tags", new ListType(new Field("item", Int64Type.Default, true)))));

        await Assert.ThrowsAsync<InvalidOperationException>(() => merge.OpenAsync());
    }

    [Fact]
    public async Task Open_DifferentColumnCount_IsRefused()
    {
        var merge = Merge(
            Of(("id", Int32Type.Default), ("name", StringType.Default)),
            Of(("id", Int32Type.Default)));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => merge.OpenAsync());

        Assert.Contains("1 columns where 'a' has 2", ex.Message);
    }

    [Fact]
    public async Task Open_DifferentColumnName_IsRefused()
    {
        var merge = Merge(
            Of(("id", Int32Type.Default)),
            Of(("key", Int32Type.Default)));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => merge.OpenAsync());

        Assert.Contains("column 0", ex.Message);
    }
}
