namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>Tests that read or write process-wide state (<see cref="Environment.ExitCode"/>, the
/// <c>APP_PORT</c> environment variable) run one at a time.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ProcessStateCollection
{
    public const string Name = "Process state";
}
