# Getting Started

Build an order-to-invoice Transactional Integration: read JSON files, enqueue their bytes through Dapr, then post invoices to an HTTP destination.

This guide targets the framework source at this documentation revision. For NuGet consumption, select a framework release and use the docs from that tag/commit; do not assume the latest published package matches a development branch. See [Implementing pipeline steps](implementing-pipeline-steps.md) for a smaller example that runs without Dapr.

## Prerequisites

- .NET 10 SDK and a console application.
- Dapr CLI/runtime with streaming pub/sub support (Dapr 1.16 or later), and a running Redis instance for the local example below.
- A disposable input directory containing order files.
- An HTTP API accepting `POST /invoices` with the invoice JSON defined below and returning a successful status. Set `INVOICE_API_BASE_URL` to its absolute base URL. Authentication, if needed, must be added to the HTTP client configuration.

The minimal example deliberately omits TI's **optional** external idempotency and incident services. It has no deduplication or automatic incident creation. Use disposable inputs and a destination that tolerates repeats. Production considerations and opt-in configuration appear below.

## Install the packages

Create a .NET 10 console project, then install:

```bash
dotnet add package Intropy.Framework.Hosting
dotnet add package Microsoft.Extensions.Hosting --version 10.0.8
```

Pin the framework package to the release matching these docs. Hosting transitively includes Blocks, Adapters, and Core. When validating a source checkout rather than a release, use a project reference to `src/Intropy.Framework.Hosting/Intropy.Framework.Hosting.csproj` instead of the framework NuGet reference.

## Register framework services

Replace `Program.cs` with this block, then append the models and step classes from the following sections **in document order**. All C# blocks through “Register the send pipeline” form one application; there are no omitted application classes.

```csharp
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Dapr.Client;
using Dapr.Messaging.PublishSubscribe.Extensions;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.TransactionalIntegration.Job;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddIntropyFramework(opts =>
{
    opts.ComponentName = "order-processor";
    opts.ServiceNamespace = "example";
});
builder.Services.AddSingleton<DaprClient>(_ => new DaprClientBuilder().Build());
builder.Services.AddDaprPubSubClient();
builder.Services.AddTransactionalIntegration(opts =>
{
    opts.DaprPubSubName = "pubsub";
    opts.DaprTopicName = "orders";
});
builder.Services.AddSingleton<IFileAdapter>(sp => new LocalFileAdapter(
    sp.GetRequiredService<DaprClient>(),
    new FileAdapterOptions("local-storage", "incoming", new Regex(@"\.json$"))));

var invoiceApi = Environment.GetEnvironmentVariable("INVOICE_API_BASE_URL")
    ?? throw new InvalidOperationException("Set INVOICE_API_BASE_URL to your invoice API.");
builder.Services.AddSingleton<HttpClient>(_ => new HttpClient
{
    BaseAddress = new Uri(invoiceApi, UriKind.Absolute)
});
builder.Services.AddSingleton<ISourceLister, OrderSourceLister>();
builder.Services.AddSingleton<OrderReceiver>();
builder.Services.AddSingleton<OrderEnqueuer>();
builder.Services.AddSingleton<OrderCompleter>();
builder.Services.AddSingleton<InvoiceApiSender>();

builder.Services.AddReceivePipeline<Context>("order-receive", (pb, sp) => pb
    .WithReceiver(sp.GetRequiredService<OrderReceiver>())
    .WithEnqueuer(sp.GetRequiredService<OrderEnqueuer>())
    .WithCompleter(sp.GetRequiredService<OrderCompleter>()));
builder.Services.AddSendPipeline<Order, Invoice, Context>("order-send", (pb, sp) => pb
    .WithDeserializer(new OrderDeserializer())
    .WithValidator(new OrderValidator())
    .WithTransformer(new OrderToInvoiceTransformer())
    .WithSerializer(new InvoiceSerializer())
    .WithSender(sp.GetRequiredService<InvoiceApiSender>()));

using var app = builder.Build();
var runner = app.Services.GetRequiredService<TransactionalIntegrationRunner>();
return await runner.RunAsync();
```

`AddIntropyFramework` requires **both** names. Alternatively set `INTROPY_COMPONENT_NAME` and `INTROPY_SERVICE_NAMESPACE` and call it without a delegate. It does not register Dapr clients, logging, or your application dependencies. The generic host supplies logging here. The built-in TI host resolves pipelines using `Context`, so the registrations use that exact type.

## Define your data models

```csharp
public record Order(string OrderId, DateTimeOffset CreatedAt, string CustomerName, decimal Amount);
public record Invoice(string InvoiceNumber, string CustomerName, decimal Total, DateTimeOffset IssuedDate);
```

The sample uses default `System.Text.Json` settings, so the input below uses matching property casing.

## Implement the receive pipeline

### Source lister

`ISourceLister` lists items; it is not a pipeline step. `IFileAdapter` has no cancellation-token parameter, so the check cannot cancel an in-flight binding call.

```csharp
public sealed class OrderSourceLister(IFileAdapter fileAdapter) : ISourceLister
{
    public async Task<IReadOnlyList<SourceItemInfo>> ListItemsAsync(
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var files = await fileAdapter.ListAsync();
        return files.Select(f => new SourceItemInfo(f.FileName)).ToList();
    }
}
```

### Receive step

Reads the raw payload. Ordinary exceptions here become business failures when executed through the pipeline.

```csharp
public sealed class OrderReceiver(IFileAdapter fileAdapter) : ReceiveStep<Context>
{
    public override async Task<(BusinessStepResult<SourceItem> Result, Context Context)> ExecuteAsync(
        SourceItemInfo input, Context context, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var content = await fileAdapter.GetContentAsync(input.Id);
        return (new BusinessStepResult<SourceItem>.Success(new SourceItem(input.Id, content)), context);
    }
}
```

### Enqueue step

The base class creates a structured CloudEvent including metadata and trace extensions. Implement its four-parameter overload and publish the supplied **encoded CloudEvent**, not just `input.Data`. `FrameworkOptions` and hosting topic options are separate types.

```csharp
public sealed class OrderEnqueuer(
    DaprClient daprClient,
    FrameworkOptions frameworkOptions,
    TransactionalIntegrationOptions topicOptions) : EnqueueStep<Context>(frameworkOptions)
{
    public override async Task<(TechnicalStepResult<SourceItem> Result, Context Context)> ExecuteAsync(
        SourceItem input, ReadOnlyMemory<byte> cloudEvent, Context context, CancellationToken ct)
    {
        await daprClient.PublishByteEventAsync(
            topicOptions.DaprPubSubName, topicOptions.DaprTopicName,
            cloudEvent, "application/cloudevents+json", cancellationToken: ct);
        return (new TechnicalStepResult<SourceItem>.Success(input), context);
    }
}
```

### Complete step

Deletes the input after enqueue succeeds, **before destination delivery**. Use only disposable files for this walkthrough. A production implementation may archive instead; publication and deletion are not atomic.

```csharp
public sealed class OrderCompleter(IFileAdapter fileAdapter) : CompleteStep<Context>
{
    public override async Task<(BusinessStepResult<SourceItem> Result, Context Context)> ExecuteAsync(
        SourceItem input, Context context, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        await fileAdapter.DeleteAsync(input.Id);
        return (new BusinessStepResult<SourceItem>.Success(input), context);
    }
}
```

### Register the receive pipeline

The registration in `Program.cs` supplies all three required steps. Incident routing is optional. If enabling it, receive-side identity extractors must work before enqueue: the host initially supplies `sourceItemId`, not `message_id`.

## Implement the send pipeline

### Deserializer

TI deserializes raw message bytes, not the CloudEvent envelope; Hosting unwraps the subscribed message.

```csharp
public sealed class OrderDeserializer : DeserializeStep<Order, Context>
{
    public override Task<(BusinessStepResult<Order> Result, Context Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, Context context, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var order = JsonSerializer.Deserialize<Order>(input.Span)
            ?? throw new JsonException("Order payload must not be null.");
        return Task.FromResult<(BusinessStepResult<Order>, Context)>(
            (new BusinessStepResult<Order>.Success(order), context));
    }
}
```

### Validator

```csharp
public sealed class OrderValidator : ValidateStep<Order, Context>
{
    public override Task<(BusinessStepResult<Order> Result, Context Context)> ExecuteAsync(
        Order input, Context context, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        BusinessStepResult<Order> result = input.Amount > 0 && !string.IsNullOrWhiteSpace(input.OrderId)
            ? new BusinessStepResult<Order>.Success(input)
            : new BusinessStepResult<Order>.Failure(new BusinessIncidentData
            {
                Description = "Order ID is required and amount must be positive",
                Context = new Dictionary<string, string> { ["orderId"] = input.OrderId ?? "" }
            });
        return Task.FromResult((result, context));
    }
}
```

### Transformer

The tax multiplier is illustrative business logic, not a framework policy. This abstraction is technical, so uncaught exceptions become technical failures.

```csharp
public sealed class OrderToInvoiceTransformer : TransformStep<Order, Invoice, Context>
{
    public override Task<(TechnicalStepResult<Invoice> Result, Context Context)> ExecuteAsync(
        Order input, Context context, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var invoice = new Invoice($"INV-{input.OrderId}", input.CustomerName,
            input.Amount * 1.25m, input.CreatedAt);
        return Task.FromResult<(TechnicalStepResult<Invoice>, Context)>(
            (new TechnicalStepResult<Invoice>.Success(invoice), context));
    }
}
```

### Serializer

```csharp
public sealed class InvoiceSerializer : SerializeStep<Invoice, Context>
{
    public override Task<(TechnicalStepResult<string> Result, Context Context)> ExecuteAsync(
        Invoice input, Context context, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<(TechnicalStepResult<string>, Context)>(
            (new TechnicalStepResult<string>.Success(JsonSerializer.Serialize(input)), context));
    }
}
```

### Sender

TI's sender is a **business** step. An HTTP error thrown here therefore becomes a business failure, regardless of whether retry would help.

```csharp
public sealed class InvoiceApiSender(HttpClient httpClient) : SendStep<Context>
{
    public override async Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(
        string input, Context context, CancellationToken ct)
    {
        using var content = new StringContent(input, Encoding.UTF8, "application/json");
        using var response = await httpClient.PostAsync("/invoices", content, ct);
        response.EnsureSuccessStatusCode();
        return (new BusinessStepResult<string>.Success(input), context);
    }
}
```

### Register the send pipeline

The registration in `Program.cs` supplies the five required steps. Extractors, idempotency, and incident routing are optional for TI. Missing required steps cause `Build()` to fail when the pipeline is resolved. See [Builders](core/builders.md) rather than assuming the same requirements apply to Extractor or Loader.

## Run it

For local development, create a `components/` directory beside the application with these Dapr resources. These are local examples, not production manifests.

`components/local-storage.yaml` — replace the absolute root path; create its `incoming/` subdirectory. The **sidecar** must be able to read/write it. For a containerized sidecar, mount the directory and use the container path.

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: local-storage
spec:
  type: bindings.localstorage
  version: v1
  metadata:
    - name: rootPath
      value: /absolute/path/to/disposable-orders
```

`components/pubsub.yaml` — assumes a local development Redis at `localhost:6379` without authentication. Substitute the address and credentials for your environment; do not commit real credentials.

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: pubsub
spec:
  type: pubsub.redis
  version: v1
  metadata:
    - name: redisHost
      value: localhost:6379
    - name: redisPassword
      value: ""
```

Place `order-1001.json` in the binding root's `incoming/` directory:

```json
{"OrderId":"ORD-1001","CreatedAt":"2026-01-15T10:00:00Z","CustomerName":"Example Customer","Amount":100}
```

Start your invoice API, set `INVOICE_API_BASE_URL`, then run from the application directory:

```bash
dapr run --app-id order-processor --resources-path ./components -- dotnet run
```

The subscriber uses the Dapr streaming client; this console application does not expose a subscription HTTP endpoint. The example posts invoice `INV-ORD-1001` with total `125`, deletes the input after enqueue, and exits after the configured idle/grace handling. **Check the API result and logs**, not only exit code: returned per-item failures do not necessarily make the runner return `1`.

If configuration fails, first check both framework names, binding root permissions, Redis connectivity, and the invoice API URL. See [TI lifecycle and options](blocks/transactional-integration.md#lifecycle) for timeout and shutdown behavior.

## Handle results

Direct block `Execute` calls return `(Result, Context)`; low-level Core chains return `(Result, Context, CancellationToken)`. There are five pipeline outcomes, documented with a complete switch in [Results](core/results.md#handling-every-pipeline-outcome).

With this minimal host, unhandled business failures and technical failures request broker retry. `Cancelled` and, currently, `Aborted` are acknowledged. Redelivery depends on broker/Dapr policy. See [incident routing and retry](concepts/result-types.md#incident-routing-and-broker-retry) before enabling external incident handling: a successfully routed business failure becomes a handled success and is acknowledged without completing skipped delivery steps.

## Optional idempotency and incident services

These are **composition fragments**, not additional code required by the runnable example. Register real implementations of `Intropy.Contracts.IdempotencyService.IIdempotencyServiceClient` and `Intropy.Contracts.BusinessIncidentService.IBusinessIncidentServiceClient`, including their endpoints/authentication, before resolving pipelines. The framework contracts do not supply a universal client registration or service deployment.

Add these calls to the send builder to opt in. Here `sp` is its registration callback's service provider:

```csharp
.WithIdempotency(
    sp.GetRequiredService<Intropy.Contracts.IdempotencyService.IIdempotencyServiceClient>(),
    idExtractor: (order, ctx) => order.OrderId,
    dateExtractor: (order, ctx) => order.CreatedAt)
.WithBusinessIncidents(
    sp.GetRequiredService<IBusinessIncidentServiceClient>(),
    messageIdExtractor: ctx => ctx.Metadata[ContextKeys.MessageId],
    subjectExtractor: ctx => ctx.Metadata["sourceItemId"])
```

`sourceItemId` survives from the receive context, so it is available even when send deserialization fails. If consuming messages not produced by this receive pipeline, choose another reliable fallback. For receive-side incident routing use `sourceItemId` for both identity callbacks, or supply your own stable identifiers. Do not rely on a downstream step to populate metadata needed to handle an earlier failure.

Idempotency does not make enqueue/delete or destination-send/record atomic. Use stable IDs and timestamps, define a suitable hash, and design the destination to tolerate duplicates.

## Next steps

- [Implementing pipeline steps](implementing-pipeline-steps.md) — precise overrides and standalone validation example
- [Pipeline Execution](concepts/pipeline-execution.md) — failure propagation and finalizers
- [Result Types](concepts/result-types.md) — outcome classification and broker handling
- [Step Types](concepts/step-types.md) — block-qualified step contracts
- [Transactional Integration](blocks/transactional-integration.md) — hosting, options, and operational limits
