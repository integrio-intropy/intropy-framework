using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.Sweeps;

namespace Intropy.Framework.Hosting.FileSweeps;

/// <summary>
/// A file source: a port whose files a component sweeps once per run through the port's
/// <see cref="IFileAdapter"/>, and what happens to each file it handled.
/// </summary>
/// <param name="Name">The port name; the key of the <see cref="IFileAdapter"/> registered for it.</param>
/// <param name="Completion">What happens to a handled source file.</param>
public sealed record FileSourcePort(string Name, FileCompletion Completion) : SourcePort(Name)
{
    /// <summary>What happens to a handled source file.</summary>
    public FileCompletion Completion { get; } = Completion ?? throw new ArgumentNullException(nameof(Completion));

    /// <inheritdoc />
    public override ISourceSweep CreateSweep(SourceSweepContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return new FileSweep(context.Services, Name, context.ComponentName, Completion, context.LoggerFactory);
    }
}
