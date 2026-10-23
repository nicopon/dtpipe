using Apache.Arrow;
using Apache.Arrow.Serialization.Reflection;
using Apache.Arrow.Types;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.DryRun;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.DryRun;

/// <summary>
/// The tap sits on the hot path, so what it must NOT do matters as much as what it records:
/// it must not take a batch's memory with it, and it must stop asking once it has enough.
/// </summary>
public class SampleTapRecorderTests
{
    private static readonly IReadOnlyList<PipeColumnInfo> IdSchema = new List<PipeColumnInfo> { new("Id", typeof(int), false) };

    [Fact]
    public void Quota_Reached_Sets_WantsMore_False()
    {
        var tap = new SampleTapRecorder(quota: 3);
        tap.OnStageSchema(0, "reader", IdSchema, isColumnar: false);

        tap.WantsMore.Should().BeTrue("no row has been offered yet");

        for (var i = 0; i < 3; i++) tap.OnRow(0, new object?[] { i });

        tap.WantsMore.Should().BeFalse("every declared stage has its quota");
    }

    [Fact]
    public void WantsMore_Stays_True_While_Any_Stage_Is_Short()
    {
        var tap = new SampleTapRecorder(quota: 2);
        tap.OnStageSchema(0, "reader", IdSchema, isColumnar: false);
        tap.OnStageSchema(1, "filter", IdSchema, isColumnar: false);

        tap.OnRow(0, new object?[] { 1 });
        tap.OnRow(0, new object?[] { 2 });

        tap.WantsMore.Should().BeTrue("stage 1 has seen nothing — a filter that drops early rows still emits later ones");
    }

    [Fact]
    public void TotalSeen_Counts_Beyond_Quota()
    {
        var tap = new SampleTapRecorder(quota: 5);
        tap.OnStageSchema(0, "reader", IdSchema, isColumnar: false);

        for (var i = 0; i < 100; i++) tap.OnRow(0, new object?[] { i });

        var stage = tap.Build(rowsRead: 100, rowsWritten: 100).Stages.Single();
        stage.Rows.Should().HaveCount(5, "the quota bounds what is kept");
        stage.TotalSeen.Should().Be(100, "but not what is counted — this is how a cardinality change becomes visible");
    }

    [Fact]
    public void OnBatch_Does_Not_Dispose_Or_Retain_The_Input()
    {
        var tap = new SampleTapRecorder(quota: 2);
        tap.OnStageSchema(0, "reader", IdSchema, isColumnar: true);

        var schema = new Schema.Builder().Field(f => f.Name("Id").DataType(Int32Type.Default).Nullable(false)).Build();
        var array = new Int32Array.Builder().AppendRange([10, 20, 30]).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { array }, 3);

        tap.OnBatch(0, batch);

        // Still usable: the segment runner owns it and will dispose it later.
        batch.Length.Should().Be(3);
        ((Int32Array)batch.Column(0)).GetValue(2).Should().Be(30);
        batch.Dispose();
    }

    [Fact]
    public void OnBatch_Reads_Values_Through_Field_Metadata()
    {
        var guid = Guid.Parse("550e8400-e29b-41d4-a716-446655440000");

        var field = ArrowTypeMapper.GetField("Ref", typeof(Guid), isNullable: false);
        var schema = new Schema.Builder().Field(field).Build();
        var builder = new FixedSizeBinaryArrayBuilder(16);
        builder.Append(ArrowTypeMapper.ToArrowUuidBytes(guid));
        using var batch = new RecordBatch(schema, new IArrowArray[] { builder.Build() }, 1);

        var tap = new SampleTapRecorder(quota: 1);
        tap.OnStageSchema(0, "reader", new List<PipeColumnInfo> { new("Ref", typeof(Guid), false) }, isColumnar: true);
        tap.OnBatch(0, batch);

        var value = tap.Build(1, 1).Stages.Single().Rows.Single()[0];

        // Storage-only reading would hand back byte[16] here. That is the type loss the old
        // row-mode dry-run fallback caused, and it must not come back inside the unified path.
        value.Should().BeOfType<Guid>().And.Be(guid);
    }

    [Fact]
    public void Unknown_Stage_Is_Ignored_Rather_Than_Throwing()
    {
        var tap = new SampleTapRecorder(quota: 1);

        var act = () => tap.OnRow(7, new object?[] { 1 });

        act.Should().NotThrow("a tap must never be able to fail the run it observes");
    }

    [Fact]
    public void Quota_Is_Clamped_To_MaxQuota()
    {
        var tap = new SampleTapRecorder(quota: 10_000);
        tap.OnStageSchema(0, "reader", IdSchema, isColumnar: false);

        for (var i = 0; i < SampleTapRecorder.MaxQuota + 50; i++) tap.OnRow(0, new object?[] { i });

        tap.Build(0, 0).Stages.Single().Rows.Should().HaveCount(SampleTapRecorder.MaxQuota);
    }
}
