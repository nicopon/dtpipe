using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// §6 feedback UX: a model stuck regenerating the same text (observed on a temperature-0 run,
/// alternating between two sentences without end) must be caught and stopped — Ollama's own
/// repeat_penalty only looks back 64 tokens, far short of a multi-sentence cycle.
/// </summary>
public class RepetitionGuardTests
{
    private const string CycleA = "En fait, je vais simplement modifier le YAML pour ceci ";
    private const string CycleB = "Je vais modifier le YAML pour cela avant de vérifier ";

    [Fact]
    public void Detects_An_Alternating_Two_Sentence_Loop()
    {
        var guard = new RepetitionGuard(windowChars: 3000, matchChars: 50, minRepeats: 3);
        bool detected = false;

        // Feed word-by-word, the way a streamed token would arrive, cycling A/B many times.
        for (int cycle = 0; cycle < 10 && !detected; cycle++)
        {
            foreach (var chunk in Chunks(CycleA + CycleB))
            {
                if (guard.Feed(chunk)) { detected = true; break; }
            }
        }

        Assert.True(detected, "expected the alternating loop to be caught");
    }

    [Fact]
    public void Stops_Well_Before_The_Loop_Runs_Away()
    {
        var guard = new RepetitionGuard(windowChars: 3000, matchChars: 50, minRepeats: 3);
        int charsFed = 0;
        bool detected = false;

        for (int cycle = 0; cycle < 50 && !detected; cycle++)
        {
            var text = CycleA + CycleB;
            charsFed += text.Length;
            detected = guard.Feed(text);
        }

        Assert.True(detected);
        // Three repeats of a ~110-char cycle is ~330 chars; give generous headroom without
        // pinning an exact number — the point is "caught in a handful of cycles", not "eventually".
        Assert.True(charsFed < 1000, $"expected early detection, but consumed {charsFed} chars first");
    }

    [Fact]
    public void Varied_Prose_Never_Trips_The_Guard()
    {
        var guard = new RepetitionGuard(windowChars: 3000, matchChars: 50, minRepeats: 3);
        string[] sentences =
        {
            "First I will inspect the schema of the source file to understand its columns.",
            "The email column looks like a good anonymization target for this pipeline.",
            "Next, validate the YAML job before running a dry-run over a few sample rows.",
            "The dry-run confirms the mapping is correct and no rows are being dropped here.",
            "Finally, deliver the validated YAML as the plan for the user to review and run.",
        };

        foreach (var s in sentences)
            foreach (var chunk in Chunks(s + " "))
                Assert.False(guard.Feed(chunk));
    }

    [Fact]
    public void A_Short_Legitimately_Repeated_Phrase_Does_Not_Trip_The_Guard()
    {
        // A YAML config naturally repeats short structural fragments across columns
        // ("      mappings:\n        "); the match window (50 chars here) must be long enough
        // that this alone does not look like a loop.
        var guard = new RepetitionGuard(windowChars: 3000, matchChars: 50, minRepeats: 3);
        string[] blocks =
        {
            "    - type: fake\n      mappings:\n        first_name: name.firstname\n",
            "    - type: fake\n      mappings:\n        last_name: name.lastname\n",
            "    - type: fake\n      mappings:\n        address: address.fulladdress\n",
        };

        foreach (var b in blocks)
            Assert.False(guard.Feed(b));
    }

    [Fact]
    public void Two_Repeats_Are_Not_Enough()
    {
        var guard = new RepetitionGuard(windowChars: 3000, matchChars: 20, minRepeats: 3);
        string chunk = "abcdefghijklmnopqrstuvwxyz0123456789";

        Assert.False(guard.Feed(chunk));
        Assert.False(guard.Feed(chunk)); // second occurrence — still short of minRepeats
    }

    private static System.Collections.Generic.IEnumerable<string> Chunks(string text)
    {
        // Simulate streamed word-sized deltas rather than feeding one giant string at a time.
        foreach (var word in text.Split(' '))
            yield return word + " ";
    }
}
