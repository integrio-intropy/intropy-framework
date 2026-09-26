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
        return new JobRunSummary(Processed: 42, Failed: 0, Skipped: 3);
    }
}
```

Wire it up in `Program.cs`. The component owns the cancellation token (e.g. SIGTERM wiring); the framework deliberately does not abstract console-host plumbing.

```csharp
var services = new ServiceCollection();
services.AddDaprClient();
services.AddIntropyFramework(o => { o.ComponentName = "my-extractor"; o.ServiceNamespace = "example"; });
services.AddRunToCompletionJob<MyExtractorJob>(); // job name: the component name, or set o.JobName

await using var provider = services.BuildServiceProvider();
return await provider.GetRequiredService<RunToCompletionRunner>().RunAsync(ct);
```

With a generic host, `host.RunToCompletionAsync(ct)` does this for you: it starts the host,
cancels the job when the host is asked to stop (SIGTERM), and stops and disposes the host
afterwards. Use it when you export telemetry through the OpenTelemetry hosting integration: its
providers are only created when the host starts, and only flushed when the host is disposed.

`AddRunToCompletionJob` registers the job as a singleton if you haven't. Register it yourself first if you need control over construction — your registration takes precedence.

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success, nothing to do, or cancelled. Cancellation is success **by design**: the job is idempotent and decided it does not need to process (e.g. duplicates detected), so the scheduler must not retry. |
| 1 | Job failure: the job threw, or `JobRunSummary.Failed` was greater than zero. For a Transactional Integration, `Failed` counts files left in place and messages the run left for redelivery. |
| 2 | Infrastructure failure: the Dapr sidecar never became available. The job never ran. |

A failed sidecar shutdown is logged but never changes the exit code — the job's outcome stands.

### Counting skipped items

The framework cannot infer duplicate detection — component code must count `StepResult<T>.Cancelled`
outcomes itself and report them in `JobRunSummary.Skipped`. Skipped items (e.g. duplicates
detected by idempotency) never affect the exit code, but keep the summary log line and trace
tags honest.

## Extractors

`AddExtractor` registers a file-driven extractor as a run-to-completion job. Each
run sweeps the source once: every file goes through the extractor pipeline (deserialize →
validate → enrichments → idempotency check → transform → CloudEvent serialize → publish →
record idempotency → incident routing) in its own DI scope. The file is completed (deleted by
default, or archived, or a custom `SweepCompletion`) only after it has been handled.

Component code supplies the process steps (the existing `DeserializeStep`, `ValidateStep`,
`ExtractStep` and `TransformStep` base classes) and configures the pipeline with the existing
`ExtractorBuilder`, the same way a Transactional Integration configures its pipelines:

```csharp
// Caller-owned: the component identity, logging, DaprClient, IIdempotencyServiceClient,
// IBusinessIncidentServiceClient, the process steps, and the source port.
services.AddIntropyFramework(o => { o.ComponentName = "orders-extractor"; o.ServiceNamespace = "example"; });
services.AddSourcePort("orders-source", configuration, SweepCompletion.Archive("archive")); // completion optional; default: delete
services.AddScoped<OrderDeserializer>();
services.AddScoped<OrderValidator>();
services.AddScoped<OrderTransformer>();

services.AddExtractor<SourceOrder, Order, OrderContext>(
    (builder, sp) => builder
        .WithDeserializer(sp.GetRequiredService<OrderDeserializer>())
        .WithValidator(sp.GetRequiredService<OrderValidator>())
        .WithIdempotency((input, _) => input.SourceId, (input, _) => input.VersionDate)
        .WithTransformer(sp.GetRequiredService<OrderTransformer>())
        .WithSerializer(new CloudEventSerializeStep<Order, OrderContext>(o => o.OrderId, o => o.OrderDate))
        .WithDaprTopicPublisher("pubsub", "orders.accepted", new Uri("urn:example:orders"), "orders.accepted")
        .WithBusinessIncidents(ctx => ctx.Metadata["file_name"], ctx => ctx.Metadata["file_name"]),
    // A fresh context per file; metadata already holds file_name.
    (metadata, isRetry) => new OrderContext(metadata, isRetry));

await using var provider = services.BuildServiceProvider(
    new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
return await provider.GetRequiredService<RunToCompletionRunner>().RunAsync(ct);
```

The pipeline is built in each file's own DI scope, so the `sp` passed to the callback is that
scope and the steps may be scoped. `AddSourcePort` registers the port's file adapter from
`Ports:<port>` and declares it the port the job sweeps. Before listing any file, the job checks
that the source port and its adapter are registered and builds the pipeline once, so a missing registration fails the run
once, not once per file. The component's identity comes from `AddIntropyFramework` (or the
`INTROPY_COMPONENT_NAME` environment variable), as for a Transactional Integration. The job name
defaults to the component name; it and the sidecar timeouts are set through the optional
`Action<RunToCompletionOptions>`. One extractor is supported per service provider.

The framework writes the source file name into each context's metadata under
`SourceContextKeys.FileName` (`"file_name"`) before any step runs, so incident selectors can
name a file that failed to deserialize.

### Replacing dependencies

Everything the pipeline uses comes from DI, so tests swap in the `Intropy.Framework.Testing`
fakes with ordinary registrations. To replace the publisher too, configure the pipeline with
`.WithSenderFromServices()` and register the sender as `SendStep<TContext>`:

```csharp
services.RemoveAllKeyed<IFileAdapter>("orders-source");
services.AddKeyedSingleton<IFileAdapter>("orders-source", new InMemoryFileAdapter());
services.AddSingleton<SendStep<OrderContext>>(new FakeTopic<OrderContext>());
services.AddSingleton<IIdempotencyServiceClient>(new FakeIdempotencyServiceClient());
services.AddSingleton<IBusinessIncidentServiceClient>(new FakeBusinessIncidentServiceClient());
```

### File outcomes

| Pipeline result | Source file | Counted as |
|----------------|-------------|---------|
| `Success` (published, or incident routed) | Completed | `Processed` |
| `Cancelled` (idempotent duplicate) | Completed | `Skipped` |
| `TechnicalFailure`, `BusinessFailure`, empty content, exception | Kept | `Failed` |
| Completion fails after success or duplicate | Kept | `Failed` |
| `Aborted` with a host cancellation | Kept | Not counted; the sweep stops |
| `Aborted` without a host cancellation | Kept | `Failed` |
| Listing fails or composition is invalid | — | The job fails |

Each file is its own trace: a `process <source port>` root span, linked to the job's span,
holding the file's read, its pipeline and its completion. It is tagged with the file name and
its outcome, and marked as an error when the file is kept. Logs written while a file is processed
carry `FileName` and `SourcePort` as a logging scope. Any `Failed` file makes the run exit 1; the kept file is retried on the next run. Host
cancellation is checked before listing and before each file; in-flight work is always awaited.
Delivery is at-least-once: a crash after publishing but before the idempotency record means the
next run may publish the file again.

## Sweeping a source

`FileSweep` processes every file in a keyed source exactly once per run, independent of the
component that uses it: it lists the source, hands each file to a handler inside its own DI
scope, and completes the file — `SweepCompletion.Delete` or `SweepCompletion.Archive(basePath)` —
only after the handler reports it `Consumed` or a `Duplicate`. A `Failed` or throwing file stays
for the next run and is counted; delivery is at-least-once. The sweep is in the
`Intropy.Framework.Hosting.Sweep` namespace; the adapters behind the ports (SFTP, local, Azure
Blob) come from `Intropy.Framework.Adapters`:

```csharp
using Intropy.Framework.Hosting.Sweep;

var sweep = new FileSweep(provider, "orders-source", "order-import", SweepCompletion.Archive("archive"), loggerFactory);
var summary = await sweep.SweepAsync(async (file, ct) =>
{
    var content = await file.ReadAsync();
    return await PublishAsync(content, ct) ? SweepOutcome.Consumed : SweepOutcome.Failed;
}, cancellationToken);
```

`SweepAsync` returns a `JobRunSummary`: consumed files as `Processed`, duplicates as `Skipped`,
and files left in place as `Failed`.

### Completion modes

`SweepCompletion.Delete` and `SweepCompletion.Archive(basePath)` are built in. For anything else,
derive from `SweepCompletion`. The completion receives the source adapter, the file (its content
is read once and kept, and `file.Services` is the file's scope), and whether the file was
`Consumed` or a `Duplicate`:

```csharp
public sealed class ArchiveToOtherStore(string key) : SweepCompletion
{
    public override async Task CompleteAsync(IFileAdapter source, SweptFile file, SweepOutcome outcome)
    {
        var archive = file.Services.GetRequiredKeyedService<IFileAdapter>(key);
        await archive.WriteAsync(file.Name, await file.ReadAsync());
        await source.DeleteAsync(file.Name);
    }
}
```

A completion that throws keeps the file in the source: it counts as `Failed` and the next run
handles it again. Completion is always awaited, even when the host cancels the sweep.

The source adapter must be registered as a singleton (`AddSourcePort` / `AddFileAdapter` do).

## Cancellation contract

Every `IFileAdapter` method takes an optional `CancellationToken` last, and the sweep and file
jobs flow the host's cancellation into listing and reads. One deliberate exception:
`SweepCompletion.CompleteAsync` takes no token — completion of an already handled file is always
awaited to the end. The full contract, including the on-demand accessors, is in the
[Cancellation section](../Intropy.Framework.Adapters/README.md#cancellation) of the
`Intropy.Framework.Adapters` README.

A Transactional Integration never acknowledges an interrupted message: a send pipeline that
returns `Aborted` (or throws a cancellation) returns the message for redelivery. When the host
is stopping, the message is not counted, just like an interrupted file. When it was interrupted
without the host stopping (for example by `MaxMessageProcessingTime`), it counts as failed and
its span is an error. Messages still in flight when the grace period ends count as failed only
when the run ended on its idle timeout, not when the host stopped it.

## License

MIT. See the [repository](https://github.com/integrio-intropy/intropy-framework) for source and documentation.
