using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// One route of a component that consumes messages in bulk: which messages it handles, and the
/// bulk pipeline it runs a whole batch through, with its generic types closed over. The
/// single-message <see cref="IMessageRoute"/>'s counterpart, separate because a bulk route's unit
/// of work is a batch — many messages in, one outcome each out — not one message.
/// </summary>
internal interface IBulkMessageRoute
{
    /// <summary>The route's name in logs, spans and metrics; null for a route that reports none.</summary>
    string? Name { get; }

    /// <summary>Whether the route handles every message: the single route of a component that does
    /// not route.</summary>
    bool IsCatchAll { get; }

    /// <summary>Whether the route handles <paramref name="message"/>.</summary>
    bool Matches(IncomingMessage message);

    /// <summary>Builds the route's pipeline once, so a missing registration fails at startup.</summary>
    void Verify(IServiceProvider scope, string componentName);

    /// <summary>Runs a whole batch through the route, in one DI scope.</summary>
    /// <param name="messages">The batch's messages, in the order their outcomes are reported in.</param>
    /// <param name="spans">Each message's consumer span, one per message, null where it is not
    /// recorded: the message's work, its pipeline included, is traced under it.</param>
    /// <param name="scopes">Creates the DI scope the pipeline is built in.</param>
    /// <param name="componentName">The component, for the pipeline name and errors.</param>
    /// <param name="interrupt">Cancelled when the host interrupts in-flight work; distinguishes an
    /// interruption from a failed or timed-out pipeline.</param>
    /// <param name="cancellationToken">Cancels the pipeline.</param>
    /// <returns>Exactly one outcome per entry of <paramref name="messages"/>, same order.</returns>
    Task<IReadOnlyList<PipelineOutcome>> ExecuteBatchAsync(IReadOnlyList<IncomingMessage> messages,
        IReadOnlyList<Activity?> spans, IServiceScopeFactory scopes, string componentName, CancellationToken interrupt,
        CancellationToken cancellationToken);
}
