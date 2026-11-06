namespace DtPipe.Core.Abstractions;

/// <summary>
/// A writer that takes rows and Arrow batches alike, neither being a faster road into its target. The
/// engine hands it what the pipeline holds at its boundary and never bridges to reach it, where a writer
/// that merely has both contracts (a database with a bulk path for batches) pulls the pipeline to Arrow.
/// That only holds while a reader reads a cell alike on both its paths.
/// </summary>
public interface IEitherShapeWriter : IRowDataWriter, IColumnarDataWriter;
