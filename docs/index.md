# Intropy Framework

> A type-safe, observable pipeline framework for building system integrations in .NET.

## Overview

Start with the [repository introduction](../README.md) for package choices and a short example. This documentation describes the source at the same revision. For released NuGet packages, use the corresponding release tag or source commit; development-branch examples may target unpublished APIs.

## Installation

Use [Getting Started](getting-started.md#install-the-packages) for a TI job, or install only the package you need from the [package overview](../README.md#packages). Hosting includes Blocks, Adapters, and Core transitively.

## Quick example

- [Implement a pipeline step](implementing-pipeline-steps.md) — complete validator and Core chain; no Dapr sidecar needed.
- [Build a Transactional Integration](getting-started.md) — full application, source files, pub/sub, and an invoice API destination.
- [Dispatch a CloudEvent](event-dispatcher/event-dispatcher.md#quick-example) — standalone console example.

## Key features

Learn the model before choosing a block:

1. [Pipeline execution](concepts/pipeline-execution.md) — chaining, skipped steps, finalizers, cancellation.
2. [Result types](concepts/result-types.md) — business versus technical failures, incident handling, and broker responses.
3. [Step types](concepts/step-types.md) — the block-qualified matrix; identically named steps can differ.
4. [Observability](concepts/observability.md) — Activity sources, statuses, and exception-path caveats.

## Documentation

| Need | Canonical reference |
|---|---|
| Implement a step or mutate context | [Steps](core/steps.md), [implementation guide](implementing-pipeline-steps.md) |
| Construct and inspect outcomes | [Results](core/results.md) |
| Compose a Core chain | [Pipeline API](core/pipeline.md) |
| Configure framework identity | [FrameworkOptions](core/pipeline.md#frameworkoptions) |
| Configure block builders | [Builders](core/builders.md) |
| Run a file-to-queue-to-destination job | [Transactional Integration](blocks/transactional-integration.md) |
| Configure Hosting or investigate shutdown | [TI lifecycle and options](blocks/transactional-integration.md#lifecycle) |
| Publish canonical CloudEvents | [Extractor](blocks/extractor.md) |
| Process a CloudEvent at a destination | [Loader](blocks/loader.md) |
| Read/write files through Dapr | [File adapters](adapters/file-adapters.md) |
| Route events to handlers | [Event Dispatcher](event-dispatcher/event-dispatcher.md) |

Tutorials show complete tasks; references own exact contracts; concept pages explain consequences. Package READMEs summarize installation and link here rather than maintaining parallel walkthroughs.

## Requirements

- .NET 10 or later; repository SDK selection is in [global.json](../global.json).
- Dapr for Hosting, built-in file adapters, and Dapr-backed steps. Core and EventDispatcher do not require a sidecar.
- Real external client implementations when opting into built-in idempotency or business incident services.
