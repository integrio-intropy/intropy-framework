# Transactional Integration

> A two-pipeline system that reads source items, publishes them to Dapr pub/sub, and processes them through a send pipeline.

## How it works

```mermaid
graph LR
    subgraph Receive side
        SW[Sweep: list + read] --> E[Enqueue]
        E --> C[Delete / archive]
    end

    E -->|Dapr pub/sub| D[(Topic)]

    subgraph Send Pipeline
        D --> DS[Deserialize]
        DS --> EX[Extract]
        EX --> IC[Idempotency Check]
        IC --> V[Validate]
        V --> T[Transform]
        T --> S[Serialize]
        S --> SD[Send]
        SD --> IR[Idempotency Record]
    end
```

The Transactional Integration (TI) follows a classic reliable messaging pattern: receive a message from the source, publish it to an internal queue, then process it from that queue. The queue acts as a safety net — if processing fails, the message stays on the queue and can be retried without having to re-fetch it from the source system.

This matters because source systems are often unreliable or have side effects on read (e.g., SFTP servers where you delete the file after reading, APIs with pagination cursors that expire). By immediately enqueuing the raw data and completing the source transaction (deleting the file, acknowledging the read), you decouple the reliability of receiving from the reliability of processing. The source is only touched once, and all retries happen against the internal queue.

In practice, this means the TI has two pipelines connected by Dapr pub/sub:

- The **receive side** is owned by the framework: it sweeps the source, publishes each file to a Dapr topic, then cleans up the source (e.g., deletes the file).
- The **send pipeline** subscribes to the topic and processes each message through deserialization, validation, transformation, serialization, and sending to the destination.

If the send pipeline fails for a message, the message remains on the topic for automatic retry. The source system is not involved in retries.

## Lifecycle

The lifecycle is a run-to-completion job, hosted by the same `JobRunner` as the extractor:

1. Waits for the Dapr sidecar to be ready
2. Starts the receive pipeline (publisher) and send pipeline (subscriber) concurrently
3. The publisher sweeps the source (`FileSweep`): each file is read, published through the receive pipeline, and only then deleted or archived
4. The subscriber subscribes to the Dapr topic, processes messages through the send pipeline
5. After the publisher finishes and an idle timeout elapses — or the host cancels — the subscriber drains in-flight messages and shuts down
6. The Dapr sidecar is shut down

```csharp
// Starts the host, runs the job, then stops and disposes the host (flushing telemetry).
return await app.RunToCompletionAsync(ct); // 0 success or host cancellation; 1 failure, files left in place, or messages left for redelivery; 2 sidecar unavailable or host failed to start
```

## Configuration

`TransactionalIntegrationOptions` controls the lifecycle behavior:

The swept port is registered on its own, with its file adapter configured under `Ports:<port>`:

```csharp
builder.Services.AddSourcePort("order-source", builder.Configuration); // optional third argument: FileCompletion.Archive("archive")
```

```csharp
builder.Services.AddTransactionalIntegration(opts =>
{
    opts.DaprPubSubName = "orders-bus";      // default: <system>-pubsub
    opts.DaprTopicName = "orders";           // default: internal-<component>
    opts.IdleTimeout = TimeSpan.FromSeconds(5);      // default: 5 seconds
    opts.PostIdleGracePeriod = TimeSpan.FromSeconds(45); // default: 45 seconds
    opts.MaxMessageProcessingTime = TimeSpan.FromSeconds(40); // default: 40 seconds
}, job =>
{
    job.JobName = "order-processor";         // default: the component name (the trace activity name)
    job.SidecarTimeout = TimeSpan.FromSeconds(30);   // default: 30 seconds
});
```

| Option | Description |
|--------|-------------|
| `DaprPubSubName` | Name of the Dapr pub/sub component |
| `DaprTopicName` | Topic to publish/subscribe to |
| `IdleTimeout` | Time of inactivity before the subscriber considers itself idle |
| `PostIdleGracePeriod` | Time to wait after idle before shutting down |
| `MaxMessageProcessingTime` | Maximum time a single message can take before timeout |

The optional second delegate configures the shared `JobOptions`: `JobName`, `SidecarTimeout`, and `SidecarShutdownTimeout`.

Pipelines use the base `Context` by default. To use your own context type in both pipelines, pass a
`ContextFactory<TCtx>`, the same factory shape extractors use:

```csharp
builder.Services.AddTransactionalIntegration<OrderContext>(
    (metadata, isRetry) => new OrderContext(metadata, isRetry),
    opts => { /* as above */ });
```

The factory is called once per source file (metadata holds `file_name` and `source_port`) and once per message
(metadata holds what the receive side propagated, plus the message id; `isRetry` is set on
redelivery). Register `ISendPipeline<OrderContext>`; the receive side is registered for you.

## Receive side

The receive side needs no component code. The lifecycle sweeps the source with `FileSweep` (from
`Intropy.Framework.Adapters`): it lists the keyed `IFileAdapter`, reads each file into a
`SourceItem` (`Id` plus raw `byte[]` data), runs the receive pipeline in the file's own scope, and
completes the file only after the pipeline succeeded. A file that cannot be read or published stays
for the next run and fails the run (exit 1). Each file gets a fresh `Context` with the file name in
its metadata under `SourceContextKeys.FileName` (`"file_name"`) and the port it was swept from under
`SourceContextKeys.SourcePort` (`"source_port"`). Both travel with the message, so send-side steps
can read them. The framework also records the port as a CloudEvent extension of its own
(`intropysourceport`), set from the sweep rather than from the metadata.
[Send routes](#several-source-ports-and-send-routes) match on it, so a step that changes the
metadata cannot misroute a file.

`AddTransactionalIntegration` registers the receive pipeline. Its one step is a
`DaprTopicEnqueuer<TCtx>`, which publishes each item to `DaprPubSubName` / `DaprTopicName` as a
structured CloudEvent (`application/cloudevents+json`). The envelope carries `Context.Metadata` and
the current W3C trace parent, so the send pipeline's context and trace continue from the file.

Both are registered only when absent:

```csharp
// Replace the publisher, e.g. with the testing fake:
services.AddSingleton<EnqueueStep<Context>>(new FakeEnqueueStep<Context>(frameworkOptions));

// Or replace the whole receive pipeline:
services.AddReceivePipeline<Context>("order-receive",
    (pipelineBuilder, sp) => pipelineBuilder.WithEnqueuer(sp.GetRequiredService<EnqueueStep<Context>>()));
```

## Send pipeline

The send pipeline has six required steps plus optional extractors. See [Getting Started](../getting-started.md) for a complete example.

Pipeline flow: `ReadOnlyMemory<byte>` → Deserialize → Extract(s) → Idempotency Check → Validate → Transform → Serialize → Send → Idempotency Record → Business Incident Route

### Registration

```csharp
builder.Services.AddSendPipeline<Order, Invoice, Context>("order-send",
    (pipelineBuilder, sp) => pipelineBuilder
        .WithDeserializer(new OrderDeserializer())
        .WithExtractor(new EnrichWithCustomerData())  // optional, can call multiple times
        .WithValidator(new OrderValidator())
        .WithIdempotency(
            sp.GetRequiredService<IIdempotencyServiceClient>(),
            order => order.OrderId,
            order => order.CreatedAt)
        .WithTransformer(new OrderToInvoiceTransformer())
        .WithSerializer(new InvoiceSerializer())
        .WithSender(new InvoiceApiSender())
        .WithBusinessIncidents(
            sp.GetRequiredService<IBusinessIncidentServiceClient>(),
            ctx => ctx.Metadata["message_id"]));
```

## Several source ports and send routes

One integration can sweep several source ports and send each port's files through the send
pipeline for its data: for example orders and invoices from the same partner, delivered to
different folders. Each port carries one kind of data, in one format. Declare the ports as
**sources** and the send pipelines as **routes**:

```csharp
builder.Services.AddTransactionalIntegration(
    new TransactionalIntegrationDefinition<Context>(),
    sources => sources
        .Port("orders-in")
        .Port("invoices-in", FileCompletion.Archive("processed")),
    routes => routes
        .On<OrderFile, Order>(["orders-in"], (send, sp) => send
            .WithDeserializer(new OrderDeserializer())
            /* validator, transformer, serializer, sender as for any send pipeline */)
        .On<InvoiceFile, Invoice>(["invoices-in"], (send, sp) => send
            .WithDeserializer(new InvoiceDeserializer())
            /* ... */));
```

The definition holds what belongs to the whole integration: the pub/sub and topic, the context
factory, the options and the runner settings. Leave its `SourcePort` and `Completion` unset; the
sources declare them. Each port's file adapter is registered keyed by the port name, as for a
single port (for example `services.AddFileAdapter("orders-in", configuration.GetSection("Ports:orders-in"))`).
Don't call `AddSourcePort` or register an `ISendPipeline`: the sources and routes declare both. A
configuration-bound overload, `AddTransactionalIntegration(configuration, factory, sources, routes)`,
binds the definition's values the same way as for a single port.

Each route's send pipeline is typed on its own data (`On<TInput, TOutput>`) and configured exactly
like `AddSendPipeline`'s; only the context type is shared by the whole integration.

### How a file finds its route

A route names the ports whose files it handles: `["orders-in"]`, or several ports that carry the
same data, `["orders-se", "orders-no"]`. Ports match exactly, and each port is handled by exactly
one route. When every port carries the same data, declare one route for all of them with
`OnAny`:

```csharp
sources => sources.Port("orders-se").Port("orders-no"),
routes => routes.OnAny<OrderFile, Order>((send, sp) => send /* ... */)
```

An integration has either one `OnAny` route or routes by port, not both.

### What is checked at startup

Registration fails if:

- no source or no route is declared;
- a route names a port that is not declared as a source;
- a declared port is handled by no route, or by more than one;
- an `OnAny` route is combined with routes by port;
- the definition sets `SourcePort` or `Completion`, or a source port is already registered.

Every route's send pipeline is built once at startup, so a route that cannot be composed fails the
run before any file is touched, naming the route.

Because every port has a route, every swept file reaches a send pipeline. A message that reaches
the send side from a port no route handles, because the routes changed while it was queued, is left
for redelivery and counted as failed. It is never acknowledged silently.

### Sweeping and failures

The ports are swept one after another, in the order they are declared, within one run. If listing
or sweeping one port fails, the other ports are still swept. The run then fails, and the error names
every port that failed. Each port's spans and `intropy.sweep.files` metrics carry its name in
`intropy.source.port`.

### Idempotency

Idempotency is configured per route, in its send pipeline. Records are scoped by component, so when
two routes can produce the same id for different things (order `123` and invoice `123`), include
the port or the kind of data in the id. `WithIdempotency` passes the context, which holds the port:

```csharp
.WithIdempotency(sp.GetRequiredService<IIdempotencyServiceClient>(),
    (invoice, ctx) => $"{ctx.Metadata[SourceContextKeys.SourcePort]}:{invoice.Number}",
    (invoice, ctx) => invoice.IssuedAt)
```

Leave the port out when several ports deliver copies of the same feed and a record should be
processed once, whichever port it arrived on.

### One integration or several?

One integration with several ports runs on one schedule, in one process, and reports one exit
code. That suits data that belongs to one business flow. Use separate integrations when the sources
need their own schedules, scale independently, or have different owners.

If you move a running single-port integration to sources and routes, drain its queue first.
Messages enqueued by older framework versions don't carry the port extension, so no
route matches them and they end up in the dead-letter queue.

## Context propagation

The TI automatically propagates `Context.Metadata` and W3C trace context through Dapr CloudEvent extensions. When the send pipeline receives a message, the metadata and trace parent from the receive pipeline are restored automatically. The receive side publishes under a `send {topic}` producer span, and the send side processes each message under a `process {topic}` consumer span that continues it, so one file is one trace from the source to the send pipeline. Each consumer span is also linked to the span of the run that consumed it. Any metadata you set in the receive pipeline (like file names or batch IDs) is available in the send pipeline.

## Related

- [Getting Started](../getting-started.md) — complete working example
- [Builders API Reference](../core/builders.md) — exact builder method signatures
- [File Adapters](../adapters/file-adapters.md) — SFTP, local, and Azure Blob Storage adapters
