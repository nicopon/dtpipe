using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Services;
using Microsoft.Extensions.Logging;

namespace DtPipe.Engine;

/// <summary>
/// The services an <see cref="ExportRunState"/> calls through its phases, and nothing of the
/// <see cref="ExportService"/> that starts it.
/// </summary>
internal sealed record ExportRunServices(
    IExportObserver Observer,
    ILogger Logger,
    HookExecutor Hooks,
    MetricsService Metrics,
    SchemaValidationService SchemaValidator,
    PipelineExecutor PipelineExecutor,
    IMemoryChannelRegistry? ChannelRegistry,
    DtPipe.DryRun.SampleReportCollector? Reports);
