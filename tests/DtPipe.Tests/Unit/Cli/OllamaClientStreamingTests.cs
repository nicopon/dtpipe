using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The Ollama client streams a response, separating reasoning from answer,
/// assembling tool calls, reporting token usage, and treating <c>_chatTimeout</c> as a max silence
/// between lines rather than a total wall-clock budget.
/// </summary>
public class OllamaClientStreamingTests
{
    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Func<CancellationToken, HttpResponseMessage> _make;
        public StubHandler(Func<CancellationToken, HttpResponseMessage> make) => _make = make;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(_make(ct));
    }

    private sealed class RecordingObserver : ILlmStreamObserver
    {
        public readonly StringBuilder Thinking = new();
        public readonly StringBuilder Content = new();
        public readonly List<string> Tools = new();
        public void OnThinking(string delta) => Thinking.Append(delta);
        public void OnContent(string delta) => Content.Append(delta);
        public void OnToolCall(string toolName) => Tools.Add(toolName);
    }

    private static readonly List<ChatMessage> Msgs = new() { new ChatMessage("user", "hi") };
    private static readonly List<ToolDefinition> NoTools = new();

    private static HttpResponseMessage Ndjson(params string[] lines) => new(HttpStatusCode.OK)
    {
        Content = new StringContent(string.Join("\n", lines) + "\n", Encoding.UTF8, "application/x-ndjson")
    };

    [Fact]
    public async Task Streams_Thinking_Field_And_Content_Separately_With_Usage()
    {
        var handler = new StubHandler(_ => Ndjson(
            "{\"message\":{\"role\":\"assistant\",\"thinking\":\"let me \"},\"done\":false}",
            "{\"message\":{\"role\":\"assistant\",\"thinking\":\"think\"},\"done\":false}",
            "{\"message\":{\"role\":\"assistant\",\"content\":\"the \"},\"done\":false}",
            "{\"message\":{\"role\":\"assistant\",\"content\":\"answer\"},\"done\":false}",
            "{\"message\":{\"role\":\"assistant\",\"content\":\"\"},\"done\":true,\"prompt_eval_count\":11,\"eval_count\":7,\"eval_duration\":1000000000}"));
        var client = new OllamaClient(handler, TimeSpan.FromSeconds(5));
        var obs = new RecordingObserver();

        var resp = await client.ChatStreamAsync("http://x", "m", Msgs, NoTools, obs);

        Assert.Null(resp.Error);
        Assert.Equal("the answer", resp.Message.Content);
        Assert.Equal("let me think", resp.Thinking);
        Assert.Equal("the answer", obs.Content.ToString());
        Assert.Equal("let me think", obs.Thinking.ToString());
        Assert.NotNull(resp.Usage);
        Assert.Equal(11, resp.Usage!.PromptTokens);
        Assert.Equal(7, resp.Usage.CompletionTokens);
        Assert.Equal(7.0, resp.Usage.TokensPerSecond!.Value, 3);
    }

    [Fact]
    public async Task Routes_Inline_Think_Tags_To_Thinking()
    {
        var handler = new StubHandler(_ => Ndjson(
            "{\"message\":{\"content\":\"<think>reason\"},\"done\":false}",
            "{\"message\":{\"content\":\"ing</think>done\"},\"done\":true,\"eval_count\":3}"));
        var client = new OllamaClient(handler, TimeSpan.FromSeconds(5));
        var obs = new RecordingObserver();

        var resp = await client.ChatStreamAsync("http://x", "m", Msgs, NoTools, obs);

        Assert.Equal("reasoning", resp.Thinking);
        Assert.Equal("done", resp.Message.Content);
    }

    [Fact]
    public async Task Captures_A_Streamed_Tool_Call()
    {
        var handler = new StubHandler(_ => Ndjson(
            "{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"inspect\",\"arguments\":{\"input\":\"csv:a.csv\"}}}]},\"done\":false}",
            "{\"message\":{\"role\":\"assistant\",\"content\":\"\"},\"done\":true,\"eval_count\":5}"));
        var client = new OllamaClient(handler, TimeSpan.FromSeconds(5));
        var obs = new RecordingObserver();

        var resp = await client.ChatStreamAsync("http://x", "m", Msgs, NoTools, obs);

        Assert.NotNull(resp.Message.ToolCalls);
        Assert.Single(resp.Message.ToolCalls!);
        Assert.Equal("inspect", resp.Message.ToolCalls![0].Name);
        Assert.Equal(new[] { "inspect" }, obs.Tools);
    }

    [Fact]
    public async Task An_Error_Line_Becomes_An_Error_Response()
    {
        var handler = new StubHandler(_ => Ndjson("{\"error\":\"model not found\"}"));
        var client = new OllamaClient(handler, TimeSpan.FromSeconds(5));

        var resp = await client.ChatStreamAsync("http://x", "m", Msgs, NoTools, new RecordingObserver());

        Assert.Equal("model not found", resp.Error);
    }

    // A stream that drips one line every `gap`, for `count` lines, then a done line.
    private sealed class DripStream : Stream
    {
        private readonly Queue<byte[]> _chunks;
        private readonly TimeSpan _gap;
        public DripStream(TimeSpan gap, IEnumerable<string> lines)
        {
            _gap = gap;
            _chunks = new Queue<byte[]>(lines.Select(l => Encoding.UTF8.GetBytes(l + "\n")));
        }
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            if (_chunks.Count == 0) return 0;
            await Task.Delay(_gap, ct);
            var chunk = _chunks.Dequeue();
            chunk.CopyTo(buffer);
            return chunk.Length;
        }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => 0; set { } }
        public override void Flush() { }
        public override long Seek(long o, SeekOrigin r) => throw new NotSupportedException();
        public override void SetLength(long v) { }
        public override void Write(byte[] b, int o, int c) { }
    }

    [Fact]
    public async Task A_Live_Stream_Is_Not_Cut_Off_While_Lines_Keep_Arriving()
    {
        // The idle ceiling must reset on every line: total delivery (~40 * 50 ms = 2 s) runs well
        // past it, so a ceiling that did not reset would trip. The gap is 40x under the ceiling —
        // `build.sh` deliberately saturates the CPU next to this test, and even a 50 ms delay
        // stretched several times over stays far inside 2 s. (Earlier 150 ms / 1 s — 6.7x — and
        // 50 ms / 1 s — 20x — both still flaked there; the real fix is a virtual clock in
        // OllamaClient's idle timer, out of scope here.)
        const int lineCount = 40;
        var expected = new string(Enumerable.Range(0, lineCount).Select(i => (char)('a' + i % 26)).ToArray());
        var lines = expected.Take(lineCount - 1)
            .Select(c => $"{{\"message\":{{\"content\":\"{c}\"}},\"done\":false}}")
            .Append($"{{\"message\":{{\"content\":\"{expected[^1]}\"}},\"done\":true,\"eval_count\":10}}")
            .ToArray();
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StreamContent(new DripStream(TimeSpan.FromMilliseconds(50), lines))
        });
        var client = new OllamaClient(handler, TimeSpan.FromSeconds(2));

        var resp = await client.ChatStreamAsync("http://x", "m", Msgs, NoTools, new RecordingObserver());

        Assert.Null(resp.Error);
        Assert.Equal(expected, resp.Message.Content);
    }

    [Fact]
    public async Task A_Stalled_Stream_Trips_The_Idle_Ceiling()
    {
        var lines = new[]
        {
            "{\"message\":{\"content\":\"a\"},\"done\":false}",
            "{\"message\":{\"content\":\"b\"},\"done\":true,\"eval_count\":2}",
        };
        // A 1 s gap is far past the 100 ms idle ceiling — a Task.Delay never completes early, so
        // the ceiling always trips first regardless of load.
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StreamContent(new DripStream(TimeSpan.FromSeconds(1), lines))
        });
        var client = new OllamaClient(handler, TimeSpan.FromMilliseconds(100));

        var resp = await client.ChatStreamAsync("http://x", "m", Msgs, NoTools, new RecordingObserver());

        Assert.NotNull(resp.Error);
        Assert.Contains("no output", resp.Error);
    }

    // Two NDJSON lines whose content alternates forever — the source a model stuck repeating
    // itself would produce. Never signals EOF, so the test proves the client stops on its own
    // rather than exhausting the (here, unbounded) source.
    private sealed class InfiniteRepeatingStream : Stream
    {
        private readonly byte[] _cycle;
        private int _pos;
        public long TotalBytesProduced { get; private set; }

        public InfiniteRepeatingStream(string cycleNdjson) => _cycle = Encoding.UTF8.GetBytes(cycleNdjson);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            if (_pos >= _cycle.Length) _pos = 0;
            int n = Math.Min(buffer.Length, _cycle.Length - _pos);
            _cycle.AsSpan(_pos, n).CopyTo(buffer.Span);
            _pos += n;
            TotalBytesProduced += n;
            return ValueTask.FromResult(n);
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => 0; set { } }
        public override void Flush() { }
        public override long Seek(long o, SeekOrigin r) => throw new NotSupportedException();
        public override void SetLength(long v) { }
        public override void Write(byte[] b, int o, int c) { }
    }

    [Fact]
    public async Task A_Repeating_Model_Is_Stopped_Without_Draining_The_Whole_Stream()
    {
        const string cycleA = "{\"message\":{\"content\":\"En fait, je vais simplement modifier le YAML pour ceci \"},\"done\":false}\n";
        const string cycleB = "{\"message\":{\"content\":\"Je vais modifier le YAML pour cela avant de vérifier \"},\"done\":false}\n";
        var source = new InfiniteRepeatingStream(cycleA + cycleB);

        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StreamContent(source)
        });
        var client = new OllamaClient(handler, TimeSpan.FromSeconds(30));

        var task = client.ChatStreamAsync("http://x", "m", Msgs, NoTools, new RecordingObserver());
        var winner = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(10)));

        Assert.Same(task, winner); // did not hang waiting on an endless source
        var resp = await task;
        Assert.Equal(RepetitionGuard.DetectedMessage, resp.Error);

        // The text it looped on is the whole evidence. Returning an empty message left the session
        // trace saying a run died of repetition without saying on what — the one question the
        // trace exists to answer.
        Assert.Contains("modifier le YAML", resp.Message.Content);

        // The cycle is ~180 bytes; stopping within a handful of repeats means low hundreds of
        // bytes read, nowhere near what "kept draining an unbounded stream" would look like.
        Assert.True(source.TotalBytesProduced < 5000,
            $"expected the client to stop early, but it read {source.TotalBytesProduced} bytes");
    }

    [Fact]
    public async Task Genuine_Cancellation_Propagates_From_A_Stream()
    {
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StreamContent(new DripStream(TimeSpan.FromSeconds(10), new[] { "{\"done\":true}" }))
        });
        var client = new OllamaClient(handler, TimeSpan.FromSeconds(30));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => client.ChatStreamAsync("http://x", "m", Msgs, NoTools, new RecordingObserver(), ct: cts.Token));
    }
}
