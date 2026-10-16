using System.Text.Json;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// A send route of a Transactional Integration: restores the context propagated with the message,
/// runs the message's data through the route's send pipeline, and maps the result to the message's
/// outcome (<see cref="PipelineOutcome.From"/>).
/// </summary>
/// <remarks>
/// A Transactional Integration leaves a business failure for redelivery too: the message was not
/// delivered, and the broker dead-letters it once redelivery gives up.
/// </remarks>
/// <param name="contextFactory">Creates the context for each message, from the metadata propagated
/// with it.</param>
/// <typeparam name="TCtx">The integration's context type.</typeparam>
internal abstract class SendRoute<TCtx>(ContextFactory<TCtx> contextFactory) : IMessageRoute where TCtx : Context
{
    public abstract string? Name { get; }

    public abstract bool IsCatchAll { get; }

    public abstract bool Matches(IncomingMessage message);

    public void Verify(IServiceProvider scope, string componentName) => Resolve(scope, componentName);

    public async Task<PipelineOutcome> ExecuteAsync(IncomingMessage message, IServiceScopeFactory scopes,
        string componentName, CancellationToken interrupt, CancellationToken cancellationToken)
    {
        try
        {
            await using var scope = scopes.CreateAsyncScope();
            var pipeline = Resolve(scope.ServiceProvider, componentName);
            var context = ContextCreation.Create(contextFactory, RestoreMetadata(message), message.IsRetry,
                componentName, message.MessageId);
            var data = await ResolveDataAsync(message, scope.ServiceProvider, cancellationToken);
            var (result, _) = await pipeline.Execute(data, context, cancellationToken);
            return PipelineOutcome.From(result, interrupt);
        }
        catch (Exception e)
        {
            return PipelineOutcome.FromException(e, interrupt, cancellationToken);
        }
    }

    /// <summary>The route's send pipeline, in the message's scope.</summary>
    protected abstract ISendPipeline<TCtx> Resolve(IServiceProvider scope, string componentName);

    /// <summary>
    /// The message's payload: fetched from the claim store when the message carries a pointer instead
    /// of its payload, and the message's own data otherwise.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Resolving here, rather than in a pipeline step, keeps the claim check invisible to the send
    /// pipeline: its input is the payload either way, so no component's code knows the mechanism
    /// exists. A failed fetch throws and becomes a technical failure, so the message is redelivered
    /// and eventually dead-lettered — never reported as a business failure.
    /// </para>
    /// <para>
    /// This consults no claim-check policy, deliberately: the mode governs whether the component
    /// <em>writes</em> claims, while the registered store governs whether it can resolve them. That is
    /// what lets a rollback stop new claims and still drain the ones already on the queue.
    /// </para>
    /// </remarks>
    /// <exception cref="InvalidOperationException">The message carries a pointer but the component has
    /// no claim store registered. That is the shape of a mis-ordered rollout — a publisher claim-checking
    /// before its consumers can resolve — so it says exactly that rather than surfacing later as an
    /// empty payload.</exception>
    private static async Task<ReadOnlyMemory<byte>> ResolveDataAsync(IncomingMessage message,
        IServiceProvider scope, CancellationToken cancellationToken)
    {
        if (message.GetExtension(CloudEventExtensionKeys.DataRef) is not { Length: > 0 } dataRef)
            return message.Data;

        var store = scope.GetKeyedService<IFileAdapter>(ClaimStoreServiceCollectionExtensions.ClaimStoreKey)
            ?? throw new InvalidOperationException(
                $"Message '{message.MessageId}' carries a claim pointer ('{dataRef}') but no claim store is " +
                "registered: its payload is in a store this component cannot read. Register one with " +
                "AddClaimStore(configuration), configured against the same store the publisher writes to.");

        return await store.GetContentAsync(dataRef, cancellationToken);
    }

    /// <summary>The context metadata propagated with <paramref name="message"/>, plus its message id.</summary>
    private static Dictionary<string, string> RestoreMetadata(IncomingMessage message)
    {
        var metadata = message.GetExtension(CloudEventExtensionKeys.Metadata) is { Length: > 0 } json
            ? JsonSerializer.Deserialize<Dictionary<string, string>>(json) ?? []
            : [];
        metadata.TryAdd(ContextKeys.MessageId, message.MessageId);
        return metadata;
    }
}

/// <summary>The send route of an integration whose one send pipeline the caller registers: it
/// handles every message, and reports no route name.</summary>
internal sealed class RegisteredSendRoute<TCtx>(ContextFactory<TCtx> contextFactory)
    : SendRoute<TCtx>(contextFactory) where TCtx : Context
{
    public override string? Name => null;

    public override bool IsCatchAll => true;

    public override bool Matches(IncomingMessage message) => true;

    protected override ISendPipeline<TCtx> Resolve(IServiceProvider scope, string componentName) =>
        scope.GetRequiredService<ISendPipeline<TCtx>>();
}

/// <summary>A send route declared with <see cref="TransactionalIntegrationRoutes{TCtx}"/>: the files
/// its match selects — by the source port the framework recorded on the message — or every file (a
/// null match), through a send pipeline built in the message's own scope.</summary>
internal sealed class DeclaredSendRoute<TInput, TOutput, TCtx>(
    FileMatch? match,
    Func<SendPipelineBuilder<TInput, TOutput, TCtx>, IServiceProvider, SendPipelineBuilder<TInput, TOutput, TCtx>> configureSend,
    ContextFactory<TCtx> contextFactory) : SendRoute<TCtx>(contextFactory) where TCtx : Context
{
    public override string? Name => match?.Describe();

    public override bool IsCatchAll => match is null;

    public override bool Matches(IncomingMessage message) =>
        match is null || match.Matches(message.GetExtension(CloudEventExtensionKeys.SourcePort));

    protected override ISendPipeline<TCtx> Resolve(IServiceProvider scope, string componentName) =>
        RouteComposition.Build("Transactional Integration", Name, componentName, () =>
            configureSend(SendPipelineBuilder<TInput, TOutput, TCtx>.Create(
                    Name is null ? $"{componentName}.Send" : $"{componentName}.Send.{Name}",
                    scope.GetRequiredService<FrameworkOptions>(), scope.GetRequiredService<ILoggerFactory>()),
                scope).Build());
}
