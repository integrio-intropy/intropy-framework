# Intropy.Framework.Components

Declarative components for [Intropy.Framework](https://github.com/integrio-intropy/intropy-framework).
A component class declares what only it knows — its rules and its steps — and the framework wires
everything else: the host, configuration, logging, telemetry, the Dapr client, the platform-service
clients, the framework identity, the endpoint, the publisher and the pipeline. Every default is a
member the component may override.

## Install

```bash
dotnet add package Intropy.Framework.Components
```

The package ships the identity generator as an analyzer; reference it from the component project
itself, so the generator checks the component's identity where it is declared.

## Identity: the `IntropyComponent` attribute and the scaffold record

A component declares its identity in code, on its class:

```csharp
[IntropyComponent("order-gateway", Organization = "Acme", Publishes = "order-placed",
    Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
    EventSource = "urn:Acme:order-gateway")]
public sealed class OrderGateway : GatewayComponent { ... }
```

The framework reads the attribute at startup and names the component from it — the telemetry
service, the endpoint the system declares, the message and its payload. The package's generator
also turns the same attribute into the scaffold record (`.intropy/scaffold.json`, schema
version 1) at the component root: the build artifact `intropy sys create` assembles the system
from. The record is looked up next to the project first, then one level up — a CLI-scaffolded
component keeps its project in `src/` under the root and its record at the root, next to the
Dockerfile, and the first build merges the identity into that record, so the two never split
into two files. When neither place carries a record — a component whose project sits at its own
root — one is created next to the project. Nothing hand-writes the component's identity — the
record is generated from the code, in the same format the system tooling already reads. The
record is committed to the component's
repository: `sys create` reads it from the checkout, and an identity change in the attribute shows
up as a working-tree diff of the record after the next build — commit the two together.

The record also carries the CLI's provenance, which `intropy int create` writes: the top-level
fields (`template`, `owner`, `repo`, `version`, `role`, `dataFlow`, `dependsOn` — and any field a
future schema adds) and the keys inside `values` that the attribute cannot know — `appId` (the
Dapr/deployment id `sys create`'s Assemble strictly requires), `channel` (the topic the system
host declares; without it the host falls back to the message name), and the render echoes the
workspace tooling reads (`componentClass`, `projectName`, `contracts`). The ownership contract
runs two levels deep: the generator owns `schemaVersion`, `blockKind` and, within `values`, the
identity keys (`componentName`, `organization`, `publishes`, `payloadType`, `port`,
`endpointPath`, `eventSourceValue`); every other field and every other `values` key — the CLI's
`appId`, `channel`, template provenance — is preserved byte-for-byte, in the order it was found.
Owning the identity keys includes owning their absence: an attribute that leaves `Port` or
`EndpointPath` unset drops the key from the merged values, even when the record still carries
it. A record that is valid JSON but carries no usable `values` object is not malformed: the
other top-level fields are preserved and the identity-only values is written as usual. The two
owners share the one file, and neither clobbers the other; when the merged content is already
what is on disk, the file is left untouched, so an unchanged identity makes no mtime noise and no
git churn.

| Manifest member | Attribute member | Used for |
|---|---|---|
| `ComponentName` | `componentName` (first argument) | Telemetry service name, idempotency and incident records |
| `Organization` | `Organization` | Telemetry service namespace |
| `Publishes` | `Publishes` | The CloudEvents type of every published event |
| `Payload` | `Payload` | Checked, as a type, against each route's output at startup |
| `Port` | `Port` | The port the component reaches the outside world through |
| `EndpointPath` | `EndpointPath` | The path of the endpoint the attribute names; default `/<publishes>` |
| `EventSource` | `EventSource` | The CloudEvent `source`, an absolute URI |

The generator checks the identity at compile time: the build fails when a concrete
`IntropyComponent` subclass carries no attribute (INTROPY1001), a required member is missing
(INTROPY1002), the event source is not an absolute URI (INTROPY1003), or a project declares
several components while recording is on (INTROPY1004). The same checks run again at startup, so
a host that assembles a manifest by hand cannot sneak an incomplete identity past launch.

The scaffold record's `values.payloadType` is the payload type's fully qualified name, and the
record's `blockKind` is inferred from the component's base class (`GatewayComponent` →
`gateway`). The CLI seeds `values.payloadType` as the payload's short type name, and the first
build rewrites it to the fully qualified name — commit that diff; nothing in the system tooling
reads the field. A component scaffolded outside the CLI gets an identity-only record — its
`values` carries none of the CLI-owned keys — and `sys create` will then name the missing keys
(`values.appId` being the first): scaffold with `int create`, or add them to the record yourself.
A project that only carries components and never assembles
a system — a test project, say — sets
`<IntropyGenerateScaffold>false</IntropyGenerateScaffold>` and writes no record; the checks
stay on.

The channel is not part of the identity. A component runs on the pub/sub and topic the topology
settles, which the system host and the deployment pass in `INTROPY_PUBSUB_NAME` and
`INTROPY_TOPIC_NAME`.

## Gateway

A gateway is a resident HTTP service that accepts items pushed to its endpoints and publishes each
as a CloudEvent through the extractor pipeline: an extractor whose sources are its callers. Its
endpoints take the place of source ports, and each endpoint says what it accepts: the kind of data
callers post to it and the rules of the pipeline that data goes through.

```csharp
[IntropyComponent("order-gateway", Organization = "Acme", Publishes = "order-placed",
    Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
    EventSource = "urn:Acme:order-gateway")]
public sealed class OrderGateway : GatewayComponent
{
    protected override void Endpoints(GatewayEndpoints endpoints) =>
        endpoints.Endpoint(endpoint => endpoint
            .Accepts<SourceOrder, OrderPlaced>(route => route
                .Steps<OrderDeserializer, OrderValidator, OrderTransformer>()
                .Idempotency(key: (order, _) => order.OrderId, at: (order, _) => order.OrderDate)
                .OutputEvent(subject: order => order.OrderId, at: order => order.OrderDate)));
}
```

```csharp
// Program.cs
return await ComponentHost.RunAsync<OrderGateway>(args);
```

Each route names its steps: the class that deserializes its input, the one that validates it, and
the one that transforms it to the payload (`route.Steps<TDeserializer, TValidator, TTransformer>()`).
They resolve from DI in the item's own scope, so a constructor may take services; each is registered
as a transient service of itself, unless the component already registered that class. A component
that registers a step slot — the step base type — in `ConfigureServices` overrides the route's
declared class. Two endpoints accepting the same input type may name the same classes, or each its
own. A route that names no steps fails at startup, like a route missing a rule. Every route
publishes the component's one message, so each route's `TOutput` is its payload type.

`Endpoint(...)` without a port and path is the endpoint the component's identity names. A second
endpoint carrying other data is a second `Endpoint(port, path, ...)` with its own `Accepts<TIn, TOut>`
and steps. An endpoint accepts one kind of data; two endpoints accepting the same input type may
share the steps by naming the same classes, or each name its own.

### The identity check

The attribute names the endpoint the system declares: `Port` at `EndpointPath`. Startup fails
unless the component declares that endpoint at that path, so the identity and the declared
endpoints cannot drift apart; the message names both values. `Endpoint(...)` without a port and
path declares exactly the attributed endpoint (at `/<publishes>` when the attribute has no
`EndpointPath`). Every route publishes the component's one message, so each route's `TOutput`
is the attribute's `Payload` — checked as a type, naming the route that publishes the wrong one.

### Overriding the defaults

| Member | Required | Default |
|---|---|---|
| `Endpoints(endpoints)` | yes | — |
| endpoint `.Accepts<TIn, TOut>(route => …)` | yes, once per endpoint | — |
| route `.Steps<TDeserializer, TValidator, TTransformer>()` | yes | — |
| route `.Idempotency(key, at)` | yes | — |
| route `.OutputEvent(subject, at)` | yes | — |
| route `.Incidents(rule)` | no | `IncidentRule.ByBusinessIdOrRequest`: the business id once a step sets `BusinessIncidentContextKeys.BusinessId` (identity and subject), else the request id (subject `request:<id>`) |
| route `.Extend((pipeline, services) => …)` | no | The framework's pipeline, unextended |
| `Respond(outcome)` | no | `GatewayResponses.Default` |
| `ConfigureEndpoints(endpoints)` | no | Nothing: the environment's ingress owns authentication. Add `RequireAuthorization()`, rate limiting, OpenAPI metadata here |
| `ConfigureApplication(app)` | no | No middleware. Add `UseAuthentication()` and the like here |
| `ConfigureOptions(options)` | no | The `Gateway` configuration section |
| `ConfigureServices(services, configuration)` | no | Registers nothing; a step slot registered here overrides the routes' declared step classes |

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
