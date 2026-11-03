using System.Globalization;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
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
            // are read from different places on purpose: a claim-checked message does not carry the
            // payload it names. Only the attributes are read from the message itself; the payload is
            // whatever ClaimResolution hands back, and nothing downstream knows where it came from.
            // Resolution happens here rather than earlier so an unrouted message never pays a fetch:
            // the route table matched on the event type, which a claim-checked message still carries.
            var context = ContextCreation.Create(contextFactory, ProjectAttributes(message), message.IsRetry,
                componentName, message.MessageId);
            var payload = await ClaimResolution.ResolveAsync(message, scope.ServiceProvider, cancellationToken);
            var (result, _) = await pipeline.Execute(payload, context, detachTrace: false, cancellationToken);
            return PipelineOutcome.From(result, interrupt);
        }
        catch (Exception e)
        {
            return PipelineOutcome.FromException(e, interrupt, cancellationToken);
        }
    }

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
    /// <para>
    /// Every extension the sidecar delivered goes on under
    /// <see cref="CloudEventContextKeys.Extension"/> first, and the attributes above after it, so
    /// subject and time — which the sidecar delivers as extensions — end up in the form those keys
    /// promise (time normalised) rather than as they arrived.
    /// </para>
    /// </remarks>
    internal static Dictionary<string, string> ProjectAttributes(IncomingMessage message)
    {
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (name, value) in message.Extensions)
            Set(CloudEventContextKeys.Extension(name), ExtensionValue(value));

        Set(CloudEventContextKeys.Id, message.MessageId);
        Set(CloudEventContextKeys.Subject, message.Subject);
        Set(CloudEventContextKeys.Time, Time(message));
        Set(CloudEventContextKeys.Source, message.Source);
        Set(CloudEventContextKeys.Type, message.Type);
        Set(CloudEventContextKeys.DataContentType, message.DataContentType);
        return metadata;

        // Removes as well as sets, so a time the framework could not read does not survive as the
        // raw extension under the key that promises a normalised one.
        void Set(string key, string? value)
        {
            if (!string.IsNullOrEmpty(value))
                metadata[key] = value;
            else
                metadata.Remove(key);
        }
    }

    /// <summary>An extension's value as the string the context holds: a string as is, a number or
    /// boolean in its invariant JSON form (how a publisher's integer or boolean extension arrives),
    /// anything structured as its JSON, and null for a null value.</summary>
    private static string? ExtensionValue(Value value) => value.KindCase switch
    {
        Value.KindOneofCase.StringValue => value.StringValue,
        Value.KindOneofCase.NumberValue => value.NumberValue.ToString(CultureInfo.InvariantCulture),
        Value.KindOneofCase.BoolValue => value.BoolValue ? "true" : "false",
        Value.KindOneofCase.StructValue or Value.KindOneofCase.ListValue => JsonFormatter.Default.Format(value),
        _ => null
    };

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
            // The archive goes on after the route's own configuration: it is the deployment's decision.
            return configurePipeline(builder, scope)
                .WithArchive(ArchiveStoreServiceCollectionExtensions.PolicyFor(scope))
                .Build();
        });
}
