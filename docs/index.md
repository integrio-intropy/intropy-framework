# Intropy Framework

> A type-safe, observable pipeline framework for building system integrations in .NET.

## Overview

Intropy Framework provides a structured way to build integration pipelines that move data between systems. Every pipeline is a chain of typed steps — deserialize, validate, transform, serialize, send — with built-in OpenTelemetry tracing and a result type system that enforces separation between business and technical failures.

The framework ships as five packages. **Core** provides the pipeline engine and step abstractions you can use to build any pipeline from scratch. **Blocks** provides pre-composed pipeline templates for common integration patterns like Transactional Integration and Extractor. **Adapters** provides file adapters for SFTP, local file systems, and Azure Blob Storage via Dapr bindings. **Hosting** provides runtime orchestration for the Transactional Integration lifecycle (sidecar lifecycle, message subscription, idle timeout). **EventDispatcher** provides attribute-based routing of CloudEvents to typed handler classes.

## Installation

```bash
# Core pipeline engine (step abstractions, result types, pipeline execution)
dotnet add package Intropy.Framework.Core

# Blocks (Transactional Integration, Extractor, Loader — includes Core)
dotnet add package Intropy.Framework.Blocks

# File adapters (SFTP, local, Azure Blob Storage — via Dapr bindings)
dotnet add package Intropy.Framework.Adapters

# Hosting (TransactionalIntegrationRunner, Dapr sidecar lifecycle — includes Blocks)
dotnet add package Intropy.Framework.Hosting

# Event dispatcher (attribute-based CloudEvent routing to typed handlers)
dotnet add package Intropy.Framework.EventDispatcher
```

For a full Transactional Integration worker, install **Hosting** — it transitively includes Blocks, Adapters, and Core.

## Quick example

Register the framework and a send pipeline in your DI container:

```csharp
builder.Services.AddIntropyFramework(opts => opts.ComponentName = "order-processor");

builder.Services.AddTransactionalIntegration(opts =>
{
    opts.DaprPubSubName = "pubsub";
    opts.DaprTopicName = "orders";
});

builder.Services.AddSendPipeline<OrderMessage, InvoiceMessage, Context>("order-to-invoice",
    (pipelineBuilder, sp) => pipelineBuilder
        .WithDeserializer(new OrderDeserializer())
        .WithValidator(new OrderValidator())
        .WithIdempotency(
            sp.GetRequiredService<IIdempotencyServiceClient>(),
            idExtractor: order => order.OrderId,
            dateExtractor: order => order.CreatedAt)
        .WithTransformer(new OrderToInvoiceTransformer())
        .WithSerializer(new InvoiceSerializer())
        .WithSender(new InvoiceSender())
        .WithBusinessIncidents(
            sp.GetRequiredService<IBusinessIncidentServiceClient>(),
            messageIdExtractor: ctx => ctx.Metadata["message_id"]));
```

Each step is a class you implement by overriding a single `ExecuteAsync` method. The framework handles failure propagation, tracing, and result type conversion.

## Key features

- **Type-safe pipeline chains** — step input/output types are checked at compile time. [Learn more](concepts/pipeline-execution.md)
- **Business vs. technical failure separation** — the result type system forces you to classify failures explicitly. [Learn more](concepts/result-types.md)
- **Built-in OpenTelemetry tracing** — every step and pipeline gets an Activity span with result tags. [Learn more](concepts/observability.md)
- **Block pipelines** — pre-composed Transactional Integration and Extractor patterns with DI registration. [Learn more](blocks/transactional-integration.md)
- **Idempotency and business incident routing** — built-in cross-cutting concerns via builder configuration. [Learn more](core/builders.md)
- **File adapters** — SFTP, local file, and Azure Blob Storage access through Dapr bindings. [Learn more](adapters/file-adapters.md)
- **Event dispatcher** — attribute-based routing of CloudEvents to typed handler classes. [Learn more](event-dispatcher/event-dispatcher.md)

## Documentation

| Section | Description |
|---------|-------------|
| [Getting Started](getting-started.md) | Build a complete Transactional Integration from scratch |
| [Concepts](concepts/pipeline-execution.md) | How the pipeline engine, result types, and step types work |
| [Core](core/pipeline.md) | Pipeline engine, builders, step types, and result types |
| [Blocks](blocks/transactional-integration.md) | Pre-composed pipeline blocks (Transactional Integration, Extractor, Loader) |
| [Adapters](adapters/file-adapters.md) | File adapters (SFTP, local, Azure Blob) via Dapr bindings |
| [Event Dispatcher](event-dispatcher/event-dispatcher.md) | Attribute-based routing of CloudEvents to typed handlers |

## Requirements

- .NET 10 or later
- Dapr sidecar (for Blocks and Adapters)
