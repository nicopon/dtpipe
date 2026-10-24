using System;

namespace DtPipe.Cli.Mcp;

/// <summary>
/// Marks an MCP tool that executes a run: it reaches <c>JobService.ExecutePipelineAsync</c>,
/// through <see cref="DtPipe.Cli.JobRunner"/>, sample runs included.
///
/// <para>
/// F5 — the agent runs every call of a turn, and the calls carrying this mark one after the other,
/// in the order the model emitted them. A process runs one pipeline at a time, so marked calls
/// started together would only queue on that gate, in whatever order they reached it. The set is
/// derived from this attribute, like <see cref="WritesToTargetAttribute"/>'s, so a new tool that
/// runs a pipeline is serialised the day it is written.
/// </para>
///
/// <para>
/// A tool that only parses, validates or reads a source without the engine does not carry it.
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Method, Inherited = false)]
public sealed class RunsPipelineAttribute : Attribute;
