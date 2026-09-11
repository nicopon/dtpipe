using System;

namespace DtPipe.Cli.Mcp;

/// <summary>
/// Marks an MCP tool that can write to a pipeline's target.
///
/// <para>
/// F1 — the planner/executor split — hides every writing tool in plan mode. The set it hides is
/// derived from this attribute, so the capability travels with the method that has it. Carried in
/// a list somewhere else, a second writing tool would be offered to the planner until someone
/// remembered to edit that list, and nothing would report it.
/// </para>
///
/// <para>
/// The criterion is <b>writes to the target</b>, not <b>touches the engine</b>. <c>dry-run</c>
/// really executes the pipeline with the writer neutralised and must NOT carry this: hiding it in
/// plan mode would take step 4 away from the planner's own role prompt.
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Method, Inherited = false)]
public sealed class WritesToTargetAttribute : Attribute;
