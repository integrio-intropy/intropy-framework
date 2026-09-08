# Implementing pipeline steps

> The API contracts needed to implement a step without reconstructing them from scaffolds or NuGet XML files.

This guide describes the framework source at this documentation revision. Check your `PackageReference` entries (or central package versions) and use docs from the corresponding release tag or source commit. Different blocks and package versions can expose different signatures.

You need `Intropy.Framework.Blocks` for the block steps and `Context`, and `Intropy.Framework.Adapters` for the file sender below. The examples assume a .NET 10 project with implicit usings enabled.

## 1. Choose the block before the return type

`DeserializeStep`, `ExtractStep`, and `ValidateStep` are business steps in Transactional Integration, Extractor, and Loader. `TransformStep` is technical. Transactional Integration and Extractor also have technical `SerializeStep` abstractions.

**`SendStep` differs:** Transactional Integration's returns `BusinessStepResult<string>`; Extractor's and Loader's return `TechnicalStepResult<TOut>`. Transactional Integration's receive-side `EnqueueStep` is technical.

Use the [block-specific matrix](concepts/step-types.md#block-step-abstractions) for exact namespaces, input/output types, and the special Enqueue and Loader-deserializer overrides.

## 2. Return the result and the context

This complete console example uses Transactional Integration's `ValidateStep` in a small Core pipeline. Replace `Program.cs` with it to run without a Dapr sidecar:

```csharp
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Core;

var context = new Context(new Dictionary<string, string>());
var (result, returnedContext, _) = await Pipeline
    .Start(new Order("ORD-1001", 100m), context, CancellationToken.None)
    .AddStep(new OrderValidator());

Console.WriteLine(result is StepResult<Order>.Success); // True
Console.WriteLine(returnedContext.Metadata["order_id"]); // ORD-1001
Console.WriteLine(ReferenceEquals(context, returnedContext)); // True

public record Order(string Id, decimal Amount);

public sealed class OrderValidator : ValidateStep<Order, Context>
{
    public override Task<(BusinessStepResult<Order> Result, Context Context)> ExecuteAsync(
        Order input, Context context, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        context.Metadata["order_id"] = input.Id;

        BusinessStepResult<Order> result = input.Amount > 0
            ? new BusinessStepResult<Order>.Success(input)
            : new BusinessStepResult<Order>.Failure(new BusinessIncidentData
            {
                Description = "Order amount must be positive",
                Context = new Dictionary<string, string> { ["order_id"] = input.Id }
            });

        return Task.FromResult((result, context));
    }
}
```

The base class supplies `StepName`. The override returns a **two-element** tuple; only the low-level Core pipeline chain carries the third cancellation-token element. Block `Execute` methods return `(Result, Context)`.

`Context` is `record Context(Dictionary<string, string> Metadata, bool IsRetry = false)`. Its dictionary is mutable. Returning the same context carries metadata writes downstream; a record `with` copy also shares that dictionary unless you explicitly replace it. See [Context](core/steps.md#context) for copying and isolation rules.

## 3. Use the correct nested result case

- `BusinessStepResult<T>`: `Success(T)`, `Failure(BusinessIncidentData)`, `Cancelled()`, `Aborted()`.
- `TechnicalStepResult<T>`: `Success(T)`, `Failure(TechnicalFailure)`, `Cancelled()`, `Aborted()`.
- `StepResult<T>`: `Success(T)`, `BusinessFailure(BusinessIncidentData)`, `TechnicalFailure(TechnicalFailure)`, `Cancelled()`, `Aborted()`.

`Cancelled` is a logical stop, such as a duplicate. `Aborted` is execution abortion, normally due to the cancellation token. They are not interchangeable. The pipeline converts step-level cases automatically; return your step's family, not the pipeline-level family.

For example, a technical step can return `new TechnicalStepResult<string>.Failure(new TechnicalFailure("Serialization failed"))`. `TechnicalFailure` lives in `Intropy.Framework.Core.Pipeline.Abstractions.Failures`. See [Results](core/results.md) for payload examples and all case declarations.

## 4. Write text with an explicit encoding

This sender is specifically for **Transactional Integration**, so it returns the business result family. Add it as a separate source file in a project with the Adapters and Blocks packages, and supply its `IFileAdapter` through your composition code:

```csharp
using System.Text;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

public sealed class Utf8FileSender(IFileAdapter fileAdapter) : SendStep<Context>
{
    public override async Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(
        string input, Context context, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        // This example targets a destination that requires UTF-8 text.
        await fileAdapter.WriteAsync("result.json", input, Encoding.UTF8);
        return (new BusinessStepResult<string>.Success(input), context);
    }
}
```

Choose a message-specific filename for production if messages must create distinct files. `IFileAdapter` has no cancellation-token parameter; the check above cannot cancel an in-flight write. It has two write overloads:

- `WriteAsync(string fileName, byte[] content, string? basePathOverride = null)`
- `WriteAsync(string fileName, string content, Encoding encoding, string? basePathOverride = null)`

Use the text overload for strings and the byte overload for binary or already-encoded data. UTF-8 is a destination choice, not a framework requirement. See [File Adapters](adapters/file-adapters.md#choosing-a-write-overload).

## 5. Distinguish classification from handling

When run through a pipeline, ordinary exceptions in the validator and sender above become business failures because of their base classes. A failure skips subsequent ordinary steps. It does **not** by itself route an incident or consume a broker message.

With the built-in incident router, successful incident creation turns a business failure into `Success`. The transactional job subscriber requests retry for a final technical failure **or an unhandled business failure**. Other hosts may behave differently. Read [incident routing and broker retry](concepts/result-types.md#incident-routing-and-broker-retry) before relying on delivery semantics.

## Reference

- [Steps](core/steps.md) — override signatures, context, cancellation, and finalizers
- [Step Types](concepts/step-types.md) — block-qualified step matrix
- [Results](core/results.md) — exact result cases and payload construction
- [File Adapters](adapters/file-adapters.md) — overloads and encoding behavior
