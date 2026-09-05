using System.Collections.Generic;
using System.Linq;
using System.Text;
using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// §6 feedback UX: a model that marks its reasoning with &lt;think&gt;…&lt;/think&gt; in the content
/// stream must have that reasoning routed to the thinking channel, even when a tag is split across
/// two streamed deltas.
/// </summary>
public class ThinkTagSplitterTests
{
    private sealed class Recorder : ILlmStreamObserver
    {
        public readonly StringBuilder Thinking = new();
        public readonly StringBuilder Content = new();
        public void OnThinking(string delta) => Thinking.Append(delta);
        public void OnContent(string delta) => Content.Append(delta);
        public void OnToolCall(string toolName) { }
    }

    private static (string thinking, string content) Run(IEnumerable<string> deltas)
    {
        var r = new Recorder();
        var s = new ThinkTagSplitter();
        foreach (var d in deltas) s.Push(d, r);
        s.Flush(r);
        return (r.Thinking.ToString(), r.Content.ToString());
    }

    [Fact]
    public void Whole_Block_In_One_Delta()
    {
        var (thinking, content) = Run(new[] { "<think>reasoning here</think>the answer" });
        Assert.Equal("reasoning here", thinking);
        Assert.Equal("the answer", content);
    }

    [Fact]
    public void No_Tags_Is_All_Content()
    {
        var (thinking, content) = Run(new[] { "just", " a plain", " answer" });
        Assert.Equal("", thinking);
        Assert.Equal("just a plain answer", content);
    }

    [Fact]
    public void Tag_Split_Across_Deltas_Is_Still_Recognised()
    {
        var (thinking, content) = Run(new[] { "<thi", "nk>the reason", "ing</thi", "nk>done" });
        Assert.Equal("the reasoning", thinking);
        Assert.Equal("done", content);
    }

    [Fact]
    public void Token_By_Token_Stream()
    {
        var (thinking, content) = Run("<think>ab</think>cd".ToCharArray().Select(c => c.ToString()));
        Assert.Equal("ab", thinking);
        Assert.Equal("cd", content);
    }

    [Fact]
    public void Unclosed_Think_Keeps_Everything_As_Thinking()
    {
        var (thinking, content) = Run(new[] { "<think>still reasoning when the stream ends" });
        Assert.Equal("still reasoning when the stream ends", thinking);
        Assert.Equal("", content);
    }

    [Fact]
    public void Content_Before_A_Think_Block_Stays_Content()
    {
        var (thinking, content) = Run(new[] { "prefix <think>mid</think> suffix" });
        Assert.Equal("mid", thinking);
        Assert.Equal("prefix  suffix", content);
    }
}
