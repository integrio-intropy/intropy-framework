using Intropy.Framework.Adapters.File;

namespace Intropy.Framework.Hosting.FileSweeps;

/// <summary>
/// The component's source: the port a file-driven component (an extractor or a transactional
/// integration) sweeps once per run, and what happens to each file it handled.
/// </summary>
/// <param name="Name">The port name; the key of the <see cref="IFileAdapter"/> registered for it.</param>
/// <param name="Completion">What happens to a handled source file.</param>
public sealed record SourcePort(string Name, FileCompletion Completion);
