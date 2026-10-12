using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Hosting.Messaging;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// The send routes of a Transactional Integration: which send pipeline a file's message goes
/// through, by what the framework recorded when it swept the file — its source port and its name.
/// Each route's send pipeline is typed on its own data; the context type is the integration's. The
/// counterpart of a loader's routes, which match on the CloudEvent type.
/// </summary>
/// <remarks>
/// <code>
/// routes =&gt; routes
///     .On&lt;OrderCsv, Order&gt;(m =&gt; m.Port("orders-in").FileName("*.csv"), (send, sp) =&gt; send...)
///     .On&lt;OrderXml, Order&gt;(m =&gt; m.Port("orders-in").FileName("*.xml"), (send, sp) =&gt; send...)
///     .On&lt;InvoiceFile, Invoice&gt;(m =&gt; m.Port("invoices-in"), (send, sp) =&gt; send...)
/// </code>
/// <para>
/// Routes are tried in declaration order and the first that matches wins. An integration either has
/// one route for every file (<see cref="OnAny{TInput,TOutput}"/>) or matching routes, not both; the
/// registration rejects the mix.
/// </para>
/// <para>
/// A file no route matches is not put on the queue: it stays in the source and fails the run, like
/// any file that could not be handled. A message that reaches the send side matching no route (the
/// routes changed while it was queued) is left for redelivery.
/// </para>
/// </remarks>
/// <typeparam name="TCtx">The integration's context type, shared by the receive and send sides.</typeparam>
public sealed class TransactionalIntegrationRoutes<TCtx> where TCtx : Context
{
    private readonly List<Declaration> _routes = [];

    /// <summary>Sends the files <paramref name="match"/> selects through a send pipeline.</summary>
    /// <param name="match">Selects the files this route handles: their source port, their name, or
    /// both.</param>
    /// <param name="configureSend">Configures the route's send pipeline builder, given the message's
    /// scope — the same shape as <c>AddSendPipeline</c>.</param>
    /// <typeparam name="TInput">The route's deserialized input.</typeparam>
    /// <typeparam name="TOutput">What the route sends to the target.</typeparam>
    /// <returns>The routes, for chaining.</returns>
    /// <exception cref="ArgumentException"><paramref name="match"/> sets no criterion.</exception>
    public TransactionalIntegrationRoutes<TCtx> On<TInput, TOutput>(
        Func<TransactionalIntegrationMatch, TransactionalIntegrationMatch> match,
        Func<SendPipelineBuilder<TInput, TOutput, TCtx>, IServiceProvider, SendPipelineBuilder<TInput, TOutput, TCtx>> configureSend)
    {
        ArgumentNullException.ThrowIfNull(match);
        ArgumentNullException.ThrowIfNull(configureSend);
        var criteria = match(new TransactionalIntegrationMatch()) ??
            throw new ArgumentException("The match returned null.", nameof(match));
        if (criteria.IsEmpty)
            throw new ArgumentException("A route matches on at least one criterion; use OnAny for every file.",
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
    internal IEnumerable<TransactionalIntegrationMatch?> Matches => _routes.Select(r => r.Match);

    /// <summary>Whether a route handles a file from <paramref name="port"/> named
    /// <paramref name="fileName"/>.</summary>
    internal bool Handles(string port, string fileName) =>
        _routes.Any(r => r.Match is null || r.Match.Matches(port, fileName));

    /// <summary>The declared routes, with the integration's context factory.</summary>
    internal IReadOnlyList<IMessageRoute> Build(ContextFactory<TCtx> contextFactory) =>
        [.. _routes.Select(r => r.Create(contextFactory))];

    private sealed record Declaration(TransactionalIntegrationMatch? Match,
        Func<ContextFactory<TCtx>, IMessageRoute> Create);
}
