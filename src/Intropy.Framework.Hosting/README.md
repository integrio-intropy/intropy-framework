# Intropy.Framework.Hosting

Runtime orchestration for [Intropy.Framework](https://github.com/integrio-intropy/intropy-framework) pipelines.

Handles the integration host lifecycle: waits for the Dapr sidecar, runs the pipeline, subscribes to message topics, applies idle timeouts, and shuts down cleanly.

## Install

```bash
dotnet add package Intropy.Framework.Hosting
```

## Requirements

A Dapr sidecar. Used together with `Intropy.Framework.Blocks` to run TransactionalIntegration and similar long-running pipelines.

## Run-to-completion jobs

`RunToCompletionRunner` hosts workloads that do one sweep and exit — scheduled externally (system host locally, Kubernetes CronJob in production). It waits for the sidecar, executes an `IRunToCompletionJob` inside a traced activity, shuts the sidecar down (bounded, so a wedged sidecar cannot hang the job), and maps the outcome to an exit code.

Jobs are expected to be idempotent. Implement `IRunToCompletionJob` and return a `JobRunSummary`:

```csharp
public class MyExtractorJob : IRunToCompletionJob
{
    public async Task<JobRunSummary> ExecuteAsync(CancellationToken ct)
    {
        // ... one sweep of work ...
        return new JobRunSummary(Processed: 42, Failed: 0, Cancelled: 3);
    }
}
```

Wire it up in `Program.cs`. The component owns the cancellation token (e.g. SIGTERM wiring); the framework deliberately does not abstract console-host plumbing.

```csharp
var services = new ServiceCollection();
services.AddDaprClient();
services.AddSingleton<MyExtractorJob>();
services.AddRunToCompletionJob<MyExtractorJob>(o => o.JobName = "my-extractor");

await using var provider = services.BuildServiceProvider();
return await provider.GetRequiredService<RunToCompletionRunner>().RunAsync(ct);
```

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success, nothing to do, or cancelled. Cancellation is success **by design**: the job is idempotent and decided it does not need to process (e.g. duplicates detected), so the scheduler must not retry. |
| 1 | Job failure: the job threw, or `JobRunSummary.Failed` was greater than zero. |
| 2 | Infrastructure failure: the Dapr sidecar never became available. The job never ran. |

A failed sidecar shutdown is logged but never changes the exit code — the job's outcome stands.

### Counting cancelled items

The framework cannot infer duplicate detection — component code must count `StepResult<T>.Cancelled` outcomes itself and report them in `JobRunSummary.Cancelled`. Cancelled items never affect the exit code, but keep the summary log line and trace tags honest.

## License

MIT. See the [repository](https://github.com/integrio-intropy/intropy-framework) for source and documentation.
