# Transactional Integration

File-to-queue-to-destination pipelines for Intropy.Framework. Blocks provides the pipeline shapes; Hosting provides the Dapr-backed job lifecycle.

## Overview

Use TI to enqueue source bytes before processing them at the destination. Enqueue, source cleanup, and destination writes are not an atomic transaction; applications must account for duplicate delivery and partial failures.

The canonical reference is [Transactional Integration](../../../docs/blocks/transactional-integration.md). This page is a navigation entry point, not a second copy of the tutorial.

## Integration Flow

Receive → Enqueue → Complete, connected through Dapr pub/sub to:

Deserialize → Extract(s) → Idempotency Check → Validate → Transform → Serialize → Send → Idempotency Record.

Extractors, TI idempotency, and incident finalizers are optional. See [execution and lifecycle](../../../docs/blocks/transactional-integration.md#lifecycle).

## Quick Start

Install `Intropy.Framework.Hosting` for the job lifecycle; it transitively includes Blocks, Adapters, and Core. Follow [Getting Started](../../../docs/getting-started.md) for the complete application and local prerequisites.

### 1. Define Your Types

See [data models](../../../docs/getting-started.md#define-your-data-models).

### 2. Implement ReceivePipeline Steps

See [receive implementations](../../../docs/getting-started.md#implement-the-receive-pipeline), including the special encoded-CloudEvent enqueue override.

### 3. Implement SendPipeline Steps

See [send implementations](../../../docs/getting-started.md#implement-the-send-pipeline).

### 4. Configure in Program.cs

See [full registration](../../../docs/getting-started.md#register-framework-services). Both `ComponentName` and `ServiceNamespace` are required.

## Configuration Options

### TransactionalIntegrationOptions

Owned by `Intropy.Framework.Hosting.TransactionalIntegration.Job`. See [options and defaults](../../../docs/blocks/transactional-integration.md#configuration).

## ReceivePipeline Steps

### Step Types

Receiver, enqueuer, and completer are required; routing is optional. See the [block-qualified matrix](../../../docs/concepts/step-types.md#transactional-integration--receive-pipeline).

## SendPipeline Steps

Deserialize, validate, transform, serialize, and send are required. See [TI send contracts](../../../docs/concepts/step-types.md#transactional-integration--send-pipeline). TI's sender is a business step, unlike Extractor/Loader senders.

## Source Listing

See [ISourceLister](../../../docs/blocks/transactional-integration.md#isourcelister) for `ListItemsAsync` and its cancellation contract.

## Data Types

See [receive data](../../../docs/blocks/transactional-integration.md#receive-pipeline) for `SourceItemInfo` and `SourceItem`, and [Context](../../../docs/core/steps.md#context) for mutation/isolation rules.
