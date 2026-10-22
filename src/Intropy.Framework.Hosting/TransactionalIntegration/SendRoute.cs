using System.Text.Json;
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
/// <para>
/// A technical failure is always left for redelivery and never routed as an incident. What a
/// business failure does to the message depends on how the pipeline classified it and on whether
/// an incident router is configured:
/// </para>
/// <list type="bullet">
///   <item>
///     Without <c>WithBusinessIncidents(...)</c>, every business failure reaches this method
///     unhandled and the message is left for redelivery, whichever kind it is: nothing has reported
///     the problem, so acknowledging it would lose it silently. The broker dead-letters the message
///     once redelivery gives up.
///   </item>
///   <item>
///     With an incident router, a <em>retriable</em> business failure — the default: the
///     destination was unreachable, a maintenance window — is routed as an incident and still left
///     for redelivery, because the payload is intact and a later attempt may well deliver it.
///   </item>
///   <item>
///     A <em>terminal</em> business failure (<c>Retriable: false</c>) — a rejected record, data the
///     destination will never accept — has been routed as an incident, and the router reports
///     success: the message is acknowledged, because redelivering it would only re-raise the same
///     incident.
///   </item>
/// </list>
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
            var data = await ClaimResolution.ResolveAsync(message, scope.ServiceProvider, cancellationToken);
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
