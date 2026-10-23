# Intropy.Framework.Components

Declarative components for [Intropy.Framework](https://github.com/integrio-intropy/intropy-framework).
A component class declares what only it knows — its identity, its rules and its steps — and the
framework wires everything else: the host, configuration, logging, telemetry, the Dapr client, the
platform-service clients, the framework identity, the endpoint, the publisher and the pipeline.
Every default is a member the component may override.

## Install

```bash
dotnet add package Intropy.Framework.Components
```

## Identity: the manifest

A component declares its identity in code, as its `Manifest`:

```csharp
public sealed class OrderGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "order-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:order-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };
    ...
}
```

The framework names the component from it — the telemetry service, the endpoint the system
declares, the message.

| Member | Required | Used for |
|---|---|---|
| `ComponentName` | yes | Telemetry service name, idempotency and incident records |
| `Organization` | yes | Telemetry service namespace |
| `Publishes` | yes | The CloudEvents type of every published event |
| `EventSource` | yes | The CloudEvent `source`, an absolute URI |
| `Port` | no | The port the component reaches the outside world through |
| `EndpointPath` | no | The path of the endpoint the manifest names; default `/<publishes>` |

The compiler holds the component to the identity: `Manifest` is abstract, and the required members
are `required`. A blank member or a relative event source fails as the manifest is created. The
payload is not a member: each input names it where it is declared (`.Publishes<OrderPlaced>(...)`),
and startup fails unless every endpoint publishes the same type.

The system tooling does not read the manifest. `intropy int create` renders it and writes the same
values into the component's scaffold record (`.intropy/scaffold.json`), which `intropy sys create`
assembles the system from. An identity change — rare — edits both: the manifest and the matching
`values` keys in the record (`componentName`, `organization`, `publishes`, `port`, `endpointPath`,
`eventSourceValue`).

The channel is not part of the identity. A component runs on the pub/sub and topic the topology
settles, which the system host and the deployment pass in `INTROPY_PUBSUB_NAME` and
`INTROPY_TOPIC_NAME`.

## Gateway

A gateway is a resident HTTP service that accepts items pushed to its endpoints and publishes each
as a CloudEvent through the extractor pipeline: an extractor whose sources are its callers. Its
endpoints take the place of source ports. The component's `Inputs` says, for each endpoint, what it
accepts (the request body callers post), what it publishes (the projection of the component's
message), and the pipeline in between — one sentence per endpoint: *accept this, publish that,
through these steps*.

```csharp
public sealed class OrderGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "order-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:order-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs) => inputs
        .Accepts<OrderRequest>()
        .Publishes<OrderPlaced>(pipeline => pipeline
            .Steps(steps => steps
                .Deserialize<OrderDeserializer>()
                .Validate<OrderValidator>()
                .Transform<OrderTransformer>())
            .Idempotency(key: (order, _) => order.OrderId, at: (order, _) => order.OrderDate)
            .OutputEvent(subject: order => order.OrderId, at: order => order.OrderDate));
}
```

The idempotency rule reads the accepted request (`OrderRequest`); the output event rule reads the
published payload (`OrderPlaced`).

```csharp
// Program.cs
return await ComponentHost.RunAsync<OrderGateway>(args);
```

Each endpoint's pipeline names its steps — the classes the component writes — by their role, in
the order they run: the class that deserializes its input, the one that validates it, any that
enrich it, and the one that transforms it to the payload.

```csharp
.Steps(steps => steps
    .Deserialize<OrderDeserializer>()
    .Validate<OrderValidator>()
    .Extract<CustomerLookup>()     // optional, repeatable: ExtractStep<TIn, Context>
    .Extract<PriceLookup>()
    .Transform<OrderTransformer>())
```

Each call offers only the steps that may come next, so a declaration that compiles is complete and
in order. Extract steps enrich the validated input and run in the order declared, after the
validator and before the idempotency check: the idempotency key may read what they add, and they
run for duplicates too. The pipeline's rules — `Idempotency`, `OutputEvent`, `Incidents` —
configure the steps the framework owns, which are not declared here.

The steps resolve from DI in the item's own scope, so a constructor may take services; each is
registered as a transient service of itself, unless the component already registered that class.
Two endpoints accepting the same input type may name the same classes, or each its own. A pipeline
that names no steps fails at startup, like a pipeline missing a rule. Every pipeline publishes the
component's one message, as the same payload type.

`inputs.Accepts<TIn>()` accepts on the endpoint the component's manifest names. A second endpoint
carrying other data is named with `On(port, path)`, followed by its own `Accepts<TIn>()`,
`Publishes<TPayload>(...)` and steps:

```csharp
protected override void Inputs(GatewayInputs inputs) => inputs
    .Accepts<OrderRequest>()
    .Publishes<OrderPlaced>(pipeline => pipeline.Steps(...).Idempotency(...).OutputEvent(...))
    .On("order-gateway-edi", "/orders/edi")
    .Accepts<EdiOrder>()
    .Publishes<OrderPlaced>(pipeline => pipeline.Steps(...).Idempotency(...).OutputEvent(...));
```

An endpoint accepts one kind of data; two endpoints accepting the same input type may share the
steps by naming the same classes, or each name its own. An endpoint that accepts nothing, or
accepts without publishing, fails at startup and names the endpoint.

### The identity check

The manifest names the endpoint the system declares: `Port` at `EndpointPath`. Startup fails
unless the component declares that endpoint at that path, so the identity and the declared
endpoints cannot drift apart; the message names both values. `inputs.Accepts<TIn>()` declares
exactly the manifest's endpoint (at `/<publishes>` when the manifest has no `EndpointPath`).

### The context

Each item runs on a `Context`: the metadata the gateway records (`SourceContextKeys.SourcePort`,
`RequestId`, `ReceivedAt`) and whatever the steps add. To give the steps a context of the
component's own — a record deriving from `Context`, carrying what they share — create it with
`WithContext` between `Accepts` and `Publishes`:

```csharp
public sealed record OrderContext(Dictionary<string, string> Metadata, bool IsRetry, string? CustomerSegment = null)
    : Context(Metadata, IsRetry);

protected override void Inputs(GatewayInputs inputs) => inputs
    .Accepts<OrderRequest>()
    .WithContext((metadata, isRetry) => new OrderContext(metadata, isRetry))
    .Publishes<OrderPlaced>(pipeline => pipeline
        .Steps(steps => steps
            .Deserialize<OrderDeserializer>()      // DeserializeStep<OrderRequest, OrderContext>
            .Validate<OrderValidator>()
            .Extract<CustomerLookup>()
            .Transform<OrderTransformer>())
        .Idempotency(key: (order, context) => order.OrderId, at: (order, _) => order.OrderDate)
        .OutputEvent(subject: order => order.OrderId, at: order => order.OrderDate));
```

The pipeline is then typed on the component's context: every step derives from its step class on
`OrderContext`, and the idempotency rule reads it. The gateway's metadata is on the context
whatever the factory does. Without `WithContext` the pipeline runs on `Context`. The context is
per endpoint, like the rest of the pipeline.

### Without idempotency

A pipeline declares its idempotency rule, or explicitly none: `.NoIdempotency()` publishes every
item it accepts and never calls the Idempotency Service. A pipeline that declares neither fails at
startup, so leaving the check out is a decision in the code, not an omission.

```csharp
protected override void Inputs(GatewayInputs inputs) => inputs
    .Accepts<OrderRequest>()
    .Publishes<OrderPlaced>(pipeline => pipeline
        .Steps(steps => steps
            .Deserialize<OrderDeserializer>()
            .Validate<OrderValidator>()
            .Transform<OrderTransformer>())
        .NoIdempotency()
        .OutputEvent(subject: order => order.OrderId, at: order => order.OrderDate));
```

The caller is the gateway's retry mechanism, so a resend publishes the item again, and the gateway
never answers `200` for a duplicate. Declare it only when the consumers deduplicate (a loader's
idempotency check) or duplicates are harmless, and leave `Calls(Services.Idempotency)` out of the
component's declaration in the system host. The rule is per endpoint: one endpoint may deduplicate
while another does not.

### Overriding the defaults

| Member | Required | Default |
|---|---|---|
| `Manifest` | yes | — |
| `Inputs(inputs)` | yes | — |
| `.Accepts<TIn>()` | yes, once per endpoint | — |
| `.WithContext(factory)`, between `Accepts` and `Publishes` | no | The pipeline runs on `Context` |
| `.Publishes<TPayload>(pipeline => …)` | yes, the same type on every endpoint | — |
| pipeline `.Steps(steps => steps.Deserialize<…>().Validate<…>().Transform<…>())` | yes | — |
| steps `.Extract<TStep>()`, between `Validate` and `Transform` | no, repeatable | No extract steps |
| pipeline `.Idempotency(key, at)` or `.NoIdempotency()` | yes, one of the two | — |
| pipeline `.OutputEvent(subject, at)` | yes | — |
| pipeline `.Incidents(rule)` | no | `IncidentRule.ByBusinessIdOrRequest`: the business id once a step sets `BusinessIncidentContextKeys.BusinessId` (identity and subject), else the request id (subject `request:<id>`) |
| `Respond(outcome)` | no | `GatewayResponses.Default` |
| `ConfigureEndpoints(endpoints)` | no | Nothing: the environment's ingress owns authentication. Add `RequireAuthorization()`, rate limiting, OpenAPI metadata here |
| `ConfigureApplication(app)` | no | No middleware. Add `UseAuthentication()` and the like here |
| `ConfigureOptions(options)` | no | The `Gateway` configuration section |
| `ConfigureServices(services, configuration)` | no | Registers nothing. Register the services the steps take in their constructors here |

### Configuration

| Key | Default |
|---|---|
| `Gateway:MaxRequestBodyBytes` | `1048576` |
| `Gateway:RetryAfter` | `00:00:05` |
| `Gateway:SidecarTimeout` | `00:01:00` |
| `PlatformServices:IdempotencyServiceAppId` | `idempotency-service.services` |
| `PlatformServices:BusinessIncidentServiceAppId` | `business-incident-service.services` |
| `urls` / `http_ports` (`ASPNETCORE_*`) | Port `8080` |

Configuration is the ASP.NET Core host's — `appsettings.json`, `appsettings.<environment>.json`,
environment variables and arguments — read from the binary's directory.

## Testing

`Intropy.Framework.Testing` serves the production graph in memory with the three externals faked —
the publisher and the two platform-service clients:

```csharp
await using var host = new GatewayTestHost<OrderGateway>();

using var response = await host.PostAsync(json, requestId: "req-1");
// A gateway with several endpoints: host.PostAsync(edi, "text/plain", endpoint: "order-gateway-edi")

Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
Assert.Single(host.Topic.Events);
```

The test host does not wait for a sidecar: the gateway is ready as soon as it starts.
