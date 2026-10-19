using System.Globalization;
using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Common;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>One route of a loader: the messages of one CloudEvent type, or of every type (a null
/// event type: the single route of a loader without routes), through a loader pipeline built in the
/// message's own scope.</summary>
internal sealed class LoaderRoute<TInput, TOutput, TCtx>(
    string? eventType,
    Func<LoaderBuilder<TInput, TOutput, TCtx>, IServiceProvider, LoaderBuilder<TInput, TOutput, TCtx>> configurePipeline,
    ContextFactory<TCtx> contextFactory) : IMessageRoute where TCtx : Context
{
    public string Name => eventType ?? "*";

    public bool IsCatchAll => eventType is null;

    public bool Matches(IncomingMessage message) =>
        eventType is null || string.Equals(message.Type, eventType, StringComparison.Ordinal);

    public void Verify(IServiceProvider scope, string componentName) => Build(scope, componentName);

    public async Task<PipelineOutcome> ExecuteAsync(IncomingMessage message, IServiceScopeFactory scopes,
        string componentName, CancellationToken interrupt, CancellationToken cancellationToken)
    {
        try
        {
            await using var scope = scopes.CreateAsyncScope();
            var pipeline = Build(scope.ServiceProvider, componentName);
            // The message's attributes go on the context before its payload is chosen, and the two
            // are read from different places on purpose: a message does not always carry the payload
            // it names. The pipeline's input is whatever ResolvePayload returns; nothing downstream
            // knows where it came from.
            var context = ContextCreation.Create(contextFactory, ProjectAttributes(message), message.IsRetry,
                componentName, message.MessageId);
            var payload = ResolvePayload(message);
            var (result, _) = await pipeline.Execute(payload, context, detachTrace: false, cancellationToken);
            return PipelineOutcome.From(result, interrupt);
        }
        catch (Exception e)
        {
            return PipelineOutcome.FromException(e, interrupt, cancellationToken);
        }
    }

    /// <summary>The message's payload: the bytes of its <c>data</c> member, as the publisher encoded
    /// them and with nothing unwrapped, so what a deserializer reads is what was published.</summary>
    private static ReadOnlyMemory<byte> ResolvePayload(IncomingMessage message) => message.Data;

    /// <summary>
    /// The message's CloudEvent attributes, as the context metadata the pipeline reads them from
    /// (see <see cref="CloudEventContextKeys"/>).
    /// </summary>
    /// <remarks>
    /// An attribute the message does not carry is left off rather than written empty, so a missing
    /// one fails where it is required — idempotency naming the subject or time it could not find —
    /// instead of succeeding against an empty string. Which keys a pipeline wants is the route's
    /// business, not the message's, which is why this lives here and not on
    /// <see cref="IncomingMessage"/>: a Transactional Integration's send route projects a different
    /// set from the same shape.
    /// </remarks>
    private static Dictionary<string, string> ProjectAttributes(IncomingMessage message)
    {
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal);
        Set(CloudEventContextKeys.Id, message.MessageId);
        Set(CloudEventContextKeys.Subject, message.Subject);
        Set(CloudEventContextKeys.Time, Time(message));
        Set(CloudEventContextKeys.Source, message.Source);
        Set(CloudEventContextKeys.Type, message.Type);
        Set(CloudEventContextKeys.DataContentType, message.DataContentType);
        return metadata;

        void Set(string key, string? value)
        {
            if (!string.IsNullOrEmpty(value))
                metadata[key] = value;
        }
    }

    /// <summary>The message's time as a UTC ISO 8601 round-trip timestamp, or null when it carries
    /// none the framework can read. Normalised rather than passed through, because idempotency reads
    /// it back with <see cref="DateTimeStyles.RoundtripKind"/>.</summary>
    private static string? Time(IncomingMessage message) =>
        DateTimeOffset.TryParse(message.GetExtension("time"), CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind, out var time)
            ? time.ToString("O", CultureInfo.InvariantCulture)
            : null;

    private Loader<TInput, TOutput, TCtx> Build(IServiceProvider scope, string componentName) =>
        RouteComposition.Build("Loader", eventType, componentName, () =>
        {
            var builder = eventType is null
                ? LoaderBuilder<TInput, TOutput, TCtx>.Create($"{componentName}.Process", scope)
                : LoaderBuilder<TInput, TOutput, TCtx>.Create($"{componentName}.Process.{eventType}", scope, eventType);
            return configurePipeline(builder, scope).Build();
        });
}
