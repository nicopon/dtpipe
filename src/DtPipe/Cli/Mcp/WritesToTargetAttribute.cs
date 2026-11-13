using System;

namespace DtPipe.Cli.Mcp;

/// <summary>
/// Marks an MCP tool that can write to a pipeline's target.
///
/// <para>
/// F1 — the agent is never offered a writing tool. The set it leaves out is derived from this
/// attribute, so the capability travels with the method that has it. Carried in a list somewhere
/// else, a second writing tool would be offered to the model until someone remembered to edit that
/// list, and nothing would report it.
/// </para>
///
/// <para>
/// The criterion is <b>writes to the target</b>, not <b>touches the engine</b>. <c>dry-run</c>
/// really executes the pipeline with the writer neutralised and must NOT carry this: leaving it out
/// would take step 4 away from the agent's own role prompt.
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Method, Inherited = false)]
public sealed class WritesToTargetAttribute : Attribute;
