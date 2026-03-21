using DtPipe.Core.Models;
using System.Threading.Channels;
using Apache.Arrow;

namespace DtPipe.Core.Abstractions.Dag;

/// <summary>
/// A full registry for sharing in-memory channels between pipeline branches.
/// Composes <see cref="INativeChannelRegistry"/> (row-based) and <see cref="IArrowChannelRegistry"/> (columnar).
/// Components that only need one protocol should declare the narrower interface in their dependencies.
/// </summary>
public interface IMemoryChannelRegistry : INativeChannelRegistry, IArrowChannelRegistry
{
}
