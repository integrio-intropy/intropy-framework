using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Test.TransactionalIntegration.Receive;

public sealed record ReceiveContext() : Context(new Dictionary<string, string>())
{
    public static ReceiveContext Create() => new();
}

/// <summary>
/// Test implementation that successfully reads content.
/// </summary>
public sealed class SuccessfulReceiver : ReceiveStep<ReceiveContext>
{
    public override Task<(BusinessStepResult<SourceItem> Result, ReceiveContext Context)> ExecuteAsync(
        SourceItemInfo input, ReceiveContext context, CancellationToken ct)
    {
        var sourceItem = new SourceItem(input.Id, "test-content"u8.ToArray());
        return Task.FromResult<(BusinessStepResult<SourceItem>, ReceiveContext)>(
            (new BusinessStepResult<SourceItem>.Success(sourceItem), context));
    }
}

/// <summary>
/// Test implementation that fails to read content (simulates file not found).
/// </summary>
public sealed class FailingReceiver : ReceiveStep<ReceiveContext>
{
    public override Task<(BusinessStepResult<SourceItem> Result, ReceiveContext Context)> ExecuteAsync(
        SourceItemInfo input, ReceiveContext context, CancellationToken ct)
    {
        var incident = new BusinessIncidentData
        {
            Description = $"File not found: {input.Id}",
            Context = new Dictionary<string, string> { { "sourceItemId", input.Id } }
        };
        return Task.FromResult<(BusinessStepResult<SourceItem>, ReceiveContext)>(
            (new BusinessStepResult<SourceItem>.Failure(incident), context));
    }
}

/// <summary>
/// Test implementation that successfully publishes to queue.
/// </summary>
public sealed class SuccessfulEnqueuer(FrameworkOptions fwOptions) : EnqueueStep<ReceiveContext> (fwOptions)
{
    public override Task<(TechnicalStepResult<SourceItem> Result, ReceiveContext Context)> ExecuteAsync(
        SourceItem input, ReadOnlyMemory<byte> cloudEvent, ReceiveContext context, CancellationToken ct)
    {
        return Task.FromResult<(TechnicalStepResult<SourceItem>, ReceiveContext)>(
            (new TechnicalStepResult<SourceItem>.Success(input), context));
    }
}

/// <summary>
/// Test implementation that successfully completes (deletes source).
/// </summary>
public sealed class SuccessfulCompleter : CompleteStep<ReceiveContext>
{
    public override Task<(BusinessStepResult<SourceItem> Result, ReceiveContext Context)> ExecuteAsync(
        SourceItem input, ReceiveContext context, CancellationToken ct)
    {
        return Task.FromResult<(BusinessStepResult<SourceItem>, ReceiveContext)>(
            (new BusinessStepResult<SourceItem>.Success(input), context));
    }
}

/// <summary>
/// Test implementation that fails to complete (simulates delete failure).
/// </summary>
public sealed class FailingCompleter : CompleteStep<ReceiveContext>
{
    public override Task<(BusinessStepResult<SourceItem> Result, ReceiveContext Context)> ExecuteAsync(
        SourceItem input, ReceiveContext context, CancellationToken ct)
    {
        var incident = new BusinessIncidentData
        {
            Description = $"Failed to delete: {input.Id}",
            Context = new Dictionary<string, string> { { "sourceItemId", input.Id } }
        };
        return Task.FromResult<(BusinessStepResult<SourceItem>, ReceiveContext)>(
            (new BusinessStepResult<SourceItem>.Failure(incident), context));
    }
}
