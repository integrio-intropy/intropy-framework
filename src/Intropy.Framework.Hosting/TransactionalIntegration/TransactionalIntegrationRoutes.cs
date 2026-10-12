using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Messaging;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// The send routes of a Transactional Integration: which send pipeline a file's message goes
/// through, by the source port the framework recorded when it swept the file. Each port carries one
/// kind of data, and each route's send pipeline is typed on its own data; the context type is the
/// integration's. The counterpart of a loader's routes, which match on the CloudEvent type.
/// </summary>
/// <remarks>
/// <code>
/// routes =&gt; routes
///     .On&lt;OrderFile, Order&gt;(m =&gt; m.Port("orders-se", "orders-no"), (send, sp) =&gt; send...)
///     .On&lt;InvoiceFile, Invoice&gt;(m =&gt; m.Port("invoices-in"), (send, sp) =&gt; send...)
/// </code>
/// <para>
/// Each source port is handled by exactly one route. An integration either has one route for every
/// port (<see cref="OnAny{TInput,TOutput}"/>) or routes by port, not both; the registration rejects
/// the mix, a port routed twice, and a port no route handles.
/// </para>
/// <para>
/// A message that reaches the send side from a port no route handles (the routes changed while it
/// was queued) is left for redelivery.
/// </para>
/// </remarks>
/// <typeparam name="TCtx">The integration's context type, shared by the receive and send sides.</typeparam>
public sealed class TransactionalIntegrationRoutes<TCtx> where TCtx : Context
{
    private readonly List<Declaration> _routes = [];

    /// <summary>Sends the files <paramref name="match"/> selects through a send pipeline.</summary>
    /// <param name="match">Selects the files this route handles, by their source port.</param>
    /// <param name="configureSend">Configures the route's send pipeline builder, given the message's
    /// scope — the same shape as <c>AddSendPipeline</c>.</param>
    /// <typeparam name="TInput">The route's deserialized input.</typeparam>
    /// <typeparam name="TOutput">What the route sends to the target.</typeparam>
    /// <returns>The routes, for chaining.</returns>
    /// <exception cref="ArgumentException"><paramref name="match"/> names no port.</exception>
    public TransactionalIntegrationRoutes<TCtx> On<TInput, TOutput>(
        Func<FileMatch, FileMatch> match,
        Func<SendPipelineBuilder<TInput, TOutput, TCtx>, IServiceProvider, SendPipelineBuilder<TInput, TOutput, TCtx>> configureSend)
    {
        ArgumentNullException.ThrowIfNull(match);
        ArgumentNullException.ThrowIfNull(configureSend);
        var criteria = match(new FileMatch()) ??
            throw new ArgumentException("The match returned null.", nameof(match));
        if (criteria.IsEmpty)
            throw new ArgumentException("A route names at least one source port; use OnAny for every port.",
                nameof(match));

        _routes.Add(new Declaration(criteria, contextFactory =>
            new DeclaredSendRoute<TInput, TOutput, TCtx>(criteria, configureSend, contextFactory)));
        return this;
    }

    /// <summary>Sends every file, from any source port, through one send pipeline: the same data,
    /// received in several places.</summary>
    /// <param name="configureSend">Configures the send pipeline builder, given the message's scope —
    /// the same shape as <c>AddSendPipeline</c>.</param>
    /// <typeparam name="TInput">The deserialized input.</typeparam>
    /// <typeparam name="TOutput">What the pipeline sends to the target.</typeparam>
    /// <returns>The routes, for chaining.</returns>
    public TransactionalIntegrationRoutes<TCtx> OnAny<TInput, TOutput>(
        Func<SendPipelineBuilder<TInput, TOutput, TCtx>, IServiceProvider, SendPipelineBuilder<TInput, TOutput, TCtx>> configureSend)
    {
        ArgumentNullException.ThrowIfNull(configureSend);
        _routes.Add(new Declaration(null, contextFactory =>
            new DeclaredSendRoute<TInput, TOutput, TCtx>(null, configureSend, contextFactory)));
        return this;
    }

    /// <summary>The declared routes' matches, in declaration order; null for a route for every file.</summary>
    internal IEnumerable<FileMatch?> Matches => _routes.Select(r => r.Match);

    /// <summary>The declared routes, with the integration's context factory.</summary>
    internal IReadOnlyList<IMessageRoute> Build(ContextFactory<TCtx> contextFactory) =>
        [.. _routes.Select(r => r.Create(contextFactory))];

    private sealed record Declaration(FileMatch? Match,
        Func<ContextFactory<TCtx>, IMessageRoute> Create);
}
