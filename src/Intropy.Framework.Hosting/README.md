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

`JobRunner` hosts workloads that do one sweep and exit — scheduled externally (system host locally, Kubernetes CronJob in production). It waits for the sidecar, executes an `IJob` inside a traced activity, shuts the sidecar down (bounded, so a wedged sidecar cannot hang the job), and maps the outcome to an exit code.

Jobs are expected to be idempotent. Implement `IJob` and return a `RunSummary`:

```csharp
public class MyExtractorJob : IJob
{
    public async Task<RunSummary> ExecuteAsync(CancellationToken ct)
    {
        // ... one sweep of work ...
        return new RunSummary(Processed: 42, Failed: 0, Skipped: 3);
    }
}
```

Wire it up in `Program.cs`. The component owns the cancellation token (e.g. SIGTERM wiring); the framework deliberately does not abstract console-host plumbing.

```csharp
var services = new ServiceCollection();
services.AddDaprClient();
services.AddIntropyFramework(o => { o.ComponentName = "my-extractor"; o.ServiceNamespace = "example"; });
services.AddJob<MyExtractorJob>(); // job name: the component name, or set o.JobName

await using var provider = services.BuildServiceProvider();
return await provider.GetRequiredService<JobRunner>().RunAsync(ct);
```

With a bare `ServiceProvider` like this, nothing flushes the OpenTelemetry providers before the
process exits. With [Intropy.Telemetry](https://github.com/integrio-intropy/intropy-telemetry), wrap
the run in its `OpenTelemetryScope`, which flushes them when it is disposed:

```csharp
int exitCode;
using (new OpenTelemetryScope(provider))
    exitCode = await provider.GetRequiredService<JobRunner>().RunAsync(ct);
return exitCode;
```

With a generic host, `host.RunToCompletionAsync(ct)` does this for you: it starts the host,
cancels the job when the host is asked to stop (SIGTERM), and stops and disposes the host
afterwards. Use it when you export telemetry through the OpenTelemetry hosting integration, which
Intropy.Telemetry uses: its providers are only created when the host starts, and only flushed when
the host is disposed.

`AddJob` registers the job as a singleton if you haven't. Register it yourself first if you need control over construction — your registration takes precedence.

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success, nothing to do, or cancelled. Cancellation is success **by design**: the job is idempotent and decided it does not need to process (e.g. duplicates detected), so the scheduler must not retry. |
| 1 | Job failure: the job threw, or `RunSummary.Failed` was greater than zero. For a Transactional Integration, `Failed` counts files left in place and messages the run left for redelivery. |
| 2 | Infrastructure failure: the Dapr sidecar never became available, or the Transactional Integration's internal queue never delivered a probe before the readiness deadline. In the latter case, no source file is swept. |

A failed sidecar shutdown is logged but never changes the exit code — the job's outcome stands.

### Sidecar shutdown and the restart policy

`JobOptions.SidecarShutdown` decides when the runner shuts the sidecar down. Set it to match how the
scheduler retries a failed run. A deployment sets it with `INTROPY_JOB_SIDECAR_SHUTDOWN`; a value
set in code wins, and any other value fails at startup.

| Kubernetes Job | Setting | Why |
|---|---|---|
| `restartPolicy: Never` | `Always` (default) | The retry is a new pod. This pod only completes once the sidecar has stopped. |
| `restartPolicy: OnFailure` | `OnSuccess` | The retry restarts the app in the same pod. A sidecar that exited cleanly is not restarted, so every retry would wait for a sidecar that never comes. |

`OnSuccess` under `restartPolicy: Never` leaves the sidecar running after a failed run, and the pod
never completes.

### Counting skipped items

The framework cannot infer duplicate detection — component code must count `StepResult<T>.Cancelled`
outcomes itself and report them in `RunSummary.Skipped`. Skipped items (e.g. duplicates
detected by idempotency) never affect the exit code, but keep the summary log line and trace
tags honest.

## Transactional Integration internal queue readiness

Before sweeping source files, the built-in Dapr enqueuer publishes harmless probes through the
same internal queue as the files. Only delivery of a probe issued by the current run unlocks the
sweep; old business messages and leftover probes cannot prove the current publisher reaches the
queue. Retries use fresh message ids to avoid broker duplicate detection. Probes are acknowledged
without running the send pipeline and excluded from the run summary.

`TransactionalIntegrationOptions.InternalQueueReadyTimeout` defaults to 60 seconds (also configurable as
`InternalQueueReadyTimeout`). It must be positive and within the .NET timer range. A timeout fails the job
with exit code 2 and leaves all source files untouched; host cancellation also leaves the source
untouched while probing and follows the normal cancellation contract.

Run **one active job instance per internal queue**. Use `concurrencyPolicy: Forbid` for a
Kubernetes CronJob, and prevent other schedulers or manual jobs from overlapping it. Competing
consumers can receive each other's probes and make a healthy job time out. This check proves
startup routing, not durable storage for every publish: a queue disappearing after readiness is
not covered. Configure broker persistence and publisher confirmations separately.

Custom enqueuers are probed only when they implement `IInternalQueueProbe`. Otherwise the job logs a warning
and bypasses readiness; the custom enqueuer must ensure a successful enqueue is a safe handoff.
A custom receive pipeline must publish through the registered enqueuer for the probe to verify
its actual path.

## Extractors

`AddExtractor` registers a file-driven extractor as a run-to-completion job. Each
run sweeps the source once: every file goes through the extractor pipeline (deserialize →
validate → enrichments → idempotency check → transform → CloudEvent serialize → publish →
record idempotency → incident routing) in its own DI scope. Idempotency is optional: without
`WithIdempotency` the check and record are skipped and every file is published. The file is completed (deleted by
default, or archived, or a custom `FileCompletion`) only after it has been handled.

Component code supplies the process steps (the existing `DeserializeStep`, `ValidateStep`,
`ExtractStep` and `TransformStep` base classes) and configures the pipeline with the existing
`ExtractorBuilder`, the same way a Transactional Integration configures its pipelines:

```csharp
// Caller-owned: the component identity, logging, DaprClient, IIdempotencyServiceClient (only
// with WithIdempotency), IBusinessIncidentServiceClient, the process steps, and the source port.
services.AddIntropyFramework(o => { o.ComponentName = "orders-extractor"; o.ServiceNamespace = "example"; });
services.AddSourcePort("orders-source", configuration, FileCompletion.Archive("archive")); // completion optional; default: delete
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
return await provider.GetRequiredService<JobRunner>().RunAsync(ct);
```

To classify the same payload into different event types, pass the optional `type` extractor to
`CloudEventSerializeStep` — e.g. `type: (o, _) => o.Status == 0 ? "io.intropy.orders.new" : "io.intropy.orders.cancelled"`.
The publisher's configured type then applies only as the fallback when no extractor is supplied.

The pipeline is built in each file's own DI scope, so the `sp` passed to the callback is that
scope and the steps may be scoped. `AddSourcePort` registers the port's file adapter from
`Ports:<port>` and declares it the port the job sweeps. Before listing any file, the job checks
that the source port and its adapter are registered and builds the pipeline once, so a missing registration fails the run
once, not once per file. The component's identity comes from `AddIntropyFramework` (or the
`INTROPY_COMPONENT_NAME` environment variable), as for a Transactional Integration. The job name
defaults to the component name; it and the sidecar timeouts are set through the optional
`Action<JobOptions>`. One extractor is supported per service provider.

To sweep several source ports, each through the pipeline for its data, register the extractor with
sources and routes instead: `AddExtractor(sources => sources.Port("orders-in").Port("invoices-in"),
routes => routes.On<OrderFile, Order>(["orders-in"], ...).On<InvoiceFile, Invoice>(["invoices-in"], ...))`.
See "Several source ports and routes" in the Extractor block docs (`docs/blocks/extractor.md`).

The framework writes the source file name into each context's metadata under
`SourceContextKeys.FileName` (`"file_name"`) before any step runs, so incident selectors can
name a file that failed to deserialize. The port the file was swept from is written next to it,
under `SourceContextKeys.SourcePort` (`"source_port"`). Both keys are the framework's: a value
the context factory sets is overwritten.

### Replacing dependencies

Everything the pipeline uses comes from DI, so tests swap in the `Intropy.Framework.Testing`
fakes with ordinary registrations. To replace the publisher too, configure the pipeline with
`.WithSenderFromServices()` and register the sender as `SendStep<TContext>`:

```csharp
services.RemoveAllKeyed<IFileAdapter>("orders-source");
services.AddKeyedSingleton<IFileAdapter>("orders-source", new InMemoryFileAdapter());
services.AddSingleton<SendStep<OrderContext>>(new FakeTopic<OrderContext>());
services.AddSingleton<IIdempotencyServiceClient>(new FakeIdempotencyServiceClient()); // only with WithIdempotency
services.AddSingleton<IBusinessIncidentServiceClient>(new FakeBusinessIncidentServiceClient());
```

### File outcomes

| Pipeline result | Source file | Counted as |
|----------------|-------------|---------|
| `Success` (published, or incident routed) | Completed | `Processed` |
| `Cancelled` (idempotent duplicate; only with `WithIdempotency`) | Completed | `Skipped` |
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
next run may publish the file again. Without idempotency, a crash after publishing but before
the file is completed has the same effect, and so does a source that delivers the same file twice.

## Sweeping a source

`FileSweep` processes every file in a keyed source exactly once per run, independent of the
component that uses it: it lists the source, hands each file to a handler inside its own DI
scope, and completes the file — `FileCompletion.Delete` or `FileCompletion.Archive(basePath)` —
only after the handler reports it `Consumed` or a `Duplicate`. A `Failed` or throwing file stays
for the next run and is counted; delivery is at-least-once. The sweep is in the
`Intropy.Framework.Hosting.FileSweeps` namespace; the adapters behind the ports (SFTP, SMB, local, Azure
Blob) come from `Intropy.Framework.Adapters`:

```csharp
using Intropy.Framework.Hosting.FileSweeps;

var sweep = new FileSweep(provider, "orders-source", "order-import", FileCompletion.Archive("archive"), loggerFactory);
var summary = await sweep.SweepAsync(async (file, ct) =>
{
    var content = await file.ReadAsync();
    return await PublishAsync(content, ct) ? FileOutcome.Consumed : FileOutcome.Failed;
}, cancellationToken);
```

`SweepAsync` returns a `RunSummary`: consumed files as `Processed`, duplicates as `Skipped`,
and files left in place as `Failed`.

### Completion modes

`FileCompletion.Delete` and `FileCompletion.Archive(basePath)` are built in. On an SMB source,
`Archive` moves the file with the binding's rename instead of copying it; the archive never
overwrites, so a name it already holds is archived as `order_20261026T101500123Z.xml`. For anything else,
derive from `FileCompletion`. The completion receives the source adapter, the file (its content
is read once and kept, and `file.Services` is the file's scope), and whether the file was
`Consumed` or a `Duplicate`:

```csharp
public sealed class ArchiveToOtherStore(string key) : FileCompletion
{
    public override async Task CompleteAsync(IFileAdapter source, SweptFile file, FileOutcome outcome)
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
`FileCompletion.CompleteAsync` takes no token — completion of an already handled file is always
awaited to the end. The full contract, including the on-demand accessors, is in the
[Cancellation section](../Intropy.Framework.Adapters/README.md#cancellation) of the
`Intropy.Framework.Adapters` README.

A Transactional Integration never acknowledges an interrupted message: a send pipeline that
returns `Aborted` (or throws a cancellation) returns the message for redelivery. When the host
is stopping, the message is not counted, just like an interrupted file. When it was interrupted
without the host stopping (for example by `MaxMessageProcessingTime`), it counts as failed and
its span is an error. Messages still in flight when the grace period ends count as failed only
when the run ended on its idle timeout, not when the host stopped it.

## Gateways

`AddGateway` registers an extractor whose items are pushed to it over HTTP instead of swept from a
source, and `MapGateway` maps it. Its endpoints take the place of an extractor's source ports: each
endpoint is declared as one unit — its port, its path, and the pipeline its items go through — and
each request body runs through that pipeline as one item, and the pipeline's result is the caller's
answer. The caller is the only retry mechanism: nothing stays behind for a next run.

```csharp
builder.Services.AddGateway(new GatewayDefinition
{
    Endpoints = endpoints => endpoints
        .Endpoint<WebshopOrder, OrderPlaced>("order-gateway-webshop", "/orders/webshop", (pipeline, services) => pipeline...)
        .Endpoint<EdiOrder, OrderPlaced>("order-gateway-edi", "/orders/edi", (pipeline, services) => pipeline...),
});

var app = builder.Build();
app.MapGateway();
```

An endpoint is a port, as the system's topology names it, and the path callers `POST` to. The port
is stamped on every event the endpoint's items publish and scopes their idempotency records, exactly
as for a swept port. Registration requires at least one endpoint and rejects a path that does not
start with `/` and a duplicate port or path (ASP.NET Core matches paths case-insensitively). A body
reaches its pipeline's deserializer as undecoded bytes, as a swept file does.

`Intropy.Framework.Components` wraps this in a declarative `GatewayComponent`; use this registration
when you compose the web host yourself.

### Outcomes

| Outcome | Default response | Resend? |
|---|---|---|
| Published | `202`, `{ requestId, eventId }` | No |
| Duplicate (idempotency check) | `200`, `{ requestId, duplicate: true }` | Safe; nothing is published again |
| Rejected (business incident routed) | `422` problem details | No — it fails again |
| Too large (`MaxRequestBodyBytes`) | `413` problem details | No |
| Unavailable (not ready, sidecar, pub/sub or platform service down) | `503` + `Retry-After` | Yes |
| Failed (a technical failure a resend cannot cure) | `500` problem details | No — fix the gateway |

A technical failure is transient only when its exception says so: a gRPC `Unavailable`,
`DeadlineExceeded`, `ResourceExhausted`, `Aborted`, `Internal` or `Unknown`; an HTTP failure
without a status or with a `5xx`, `408` or `429`; a timeout, socket or IO error; a Dapr or platform
service client failure. Anything else — a step that throws on the item, a technical failure without
an exception, an output event without a subject — answers `500`, so the caller does not resend an
item that fails the same way every time.

`GatewayDefinition.Respond` replaces the mapping; `GatewayResponses.Default` is the default to
delegate to. `ConfigureEndpoints` applies conventions — authorization, rate limiting — to the
endpoints as one group. Every response carries `X-Request-Id`: the caller's, or one the gateway assigned. It is
also `SourceContextKeys.RequestId` on the context, set before any step runs.

A routed business incident ends the pipeline as a success; the gateway tells it apart from a publish
by the successful result's subtype, `StepResult<T>.RoutedIncident`, which the incident router
returns — a discriminator the result's other consumers deliberately do not read: to a file sweep or
a send pipeline the routed item is the handled success it ends the pipeline as.

### Startup and probes

Startup composes every endpoint's pipeline once, so a misconfigured gateway fails to start instead
of failing its first request. The gateway then waits for its sidecar in the background and never gives up: until
the sidecar answers, requests get `503` and `/readyz` answers `503`. `/healthz` answers `200` while
the process serves requests.

### Options

| `GatewayOptions` | Default |
|---|---|
| `MaxRequestBodyBytes` | 1 MiB |
| `RetryAfter` | 5 seconds |
| `WaitForSidecar` | `true` |
| `SidecarTimeout` | 60 seconds per wait; the gateway waits again after it |

Requests are counted by `intropy.gateway.requests` and timed by `intropy.gateway.duration`, tagged
with the component, the endpoint's port and `intropy.gateway.outcome`.

## License

MIT. See the [repository](https://github.com/integrio-intropy/intropy-framework) for source and documentation.
