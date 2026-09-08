using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Core.Models;
using DtPipe.DryRun;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A recorded session faked <c>join_date</c> with <c>date.past</c> and then overwrote it, in the
/// next transformer, with today's date for all thousand rows — while doing the same thing to
/// <c>email</c>, where deriving it from the faked name was the point. The two are the same shape,
/// so the report states the fact and leaves the judgement to the author.
/// </summary>
public class SampleReplacementTests
{
    private static StageCapture Stage(int index, string name, string[] columns, params object?[][] rows)
        => new(index, name,
            columns.Select(c => new PipeColumnInfo(c, typeof(string), true)).ToList(),
            IsColumnar: true, rows, rows.Length);

    private static SampleRun Run(params StageCapture[] stages) => new(stages, stages[0].Rows.Count, 0);

    [Fact]
    public void A_Column_A_Transformer_Made_And_A_Later_One_Wrote_Over_Is_Reported()
    {
        var run = Run(
            Stage(0, "generate", ["id"], ["1"], ["2"]),
            Stage(1, "fake", ["id", "join_date"], ["1", "2019-04-02"], ["2", "2021-11-30"]),
            Stage(2, "compute", ["id", "join_date"], ["1", "2026-09-09"], ["2", "2026-09-09"]));

        var found = Assert.Single(SampleRunExtensions.ProducedThenReplaced(run));
        Assert.Equal("join_date", found.Column);
        Assert.Equal("fake", found.ProducedBy);
        Assert.Equal("compute", found.ReplacedBy);
    }

    /// <summary>Overwriting a column the source supplied is what anonymisation is.</summary>
    [Fact]
    public void A_Source_Column_Overwritten_Is_Not_Reported()
    {
        var run = Run(
            Stage(0, "csv", ["email"], ["a@x.com"], ["b@x.com"]),
            Stage(1, "fake", ["email"], ["f@y.com"], ["g@y.com"]),
            Stage(2, "mask", ["email"], ["***@y.com"], ["***@y.com"]));

        Assert.Empty(SampleRunExtensions.ProducedThenReplaced(run));
    }

    [Fact]
    public void A_Produced_Column_Left_Alone_Is_Not_Reported()
    {
        var run = Run(
            Stage(0, "generate", ["id"], ["1"]),
            Stage(1, "fake", ["id", "name"], ["1", "Ada"]),
            Stage(2, "compute", ["id", "name", "greeting"], ["1", "Ada", "hi Ada"]));

        Assert.Empty(SampleRunExtensions.ProducedThenReplaced(run));
    }

    /// <summary>After an expand there is no row j running through both stages, so nothing is compared.</summary>
    [Fact]
    public void Stages_Of_Different_Length_Are_Not_Compared()
    {
        var run = Run(
            Stage(0, "generate", ["id"], ["1"]),
            Stage(1, "fake", ["id", "tag"], ["1", "a"]),
            Stage(2, "expand", ["id", "tag"], ["1", "x"], ["1", "y"]));

        Assert.Empty(SampleRunExtensions.ProducedThenReplaced(run));
    }
}
