using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.FileSweeps;

namespace Intropy.Framework.Hosting.Extractor;

/// <summary>
/// The routes of an extractor: which pipeline a file goes through, by the source port the framework
/// swept it from. Each port carries one kind of data, and each route's pipeline is typed on its own
/// data; the context type is the extractor's. The counterpart of a Transactional Integration's send
/// routes.
/// </summary>
/// <remarks>
/// <code>
/// routes =&gt; routes
///     .On&lt;OrderFile, OrderPublished&gt;(m =&gt; m.Port("orders-se", "orders-no"), (builder, sp) =&gt; builder...)
///     .On&lt;InvoiceFile, InvoicePublished&gt;(m =&gt; m.Port("invoices-in"), (builder, sp) =&gt; builder...)
/// </code>
/// <para>
/// Each source port is handled by exactly one route. An extractor either has one route for every
/// port (<see cref="OnAny{TInput,TOutput}"/>) or routes by port, not both; the registration rejects
/// the mix, a port routed twice, and a port no route handles.
/// </para>
/// <para>
/// Each route's pipeline is built for one port at a time: it stamps the port on every event it
/// publishes (the <see cref="CloudEventExtensionKeys.SourcePort"/> extension) and keeps its
/// idempotency records per port unless it shares them
/// (<see cref="ExtractorBuilder{TInput,TOutput,TCtx}.WithIdempotency"/>).
/// </para>
/// </remarks>
/// <typeparam name="TCtx">The extractor's context type.</typeparam>
public sealed class ExtractorRoutes<TCtx> where TCtx : Context
{
    private readonly List<ExtractorRoute<TCtx>> _routes = [];

    /// <summary>Runs the files <paramref name="match"/> selects through a pipeline.</summary>
    /// <param name="match">Selects the files this route handles, by their source port.</param>
    /// <param name="configurePipeline">Configures the route's pipeline builder, given the file's
    /// scope — the same shape as <c>AddExtractor</c>'s.</param>
    /// <typeparam name="TInput">The route's deserialized input.</typeparam>
    /// <typeparam name="TOutput">The route's published payload.</typeparam>
    /// <returns>The routes, for chaining.</returns>
    /// <exception cref="ArgumentException"><paramref name="match"/> names no port.</exception>
    public ExtractorRoutes<TCtx> On<TInput, TOutput>(
        Func<FileMatch, FileMatch> match,
        Func<ExtractorBuilder<TInput, TOutput, TCtx>, IServiceProvider, ExtractorBuilder<TInput, TOutput, TCtx>> configurePipeline)
    {
        ArgumentNullException.ThrowIfNull(match);
        ArgumentNullException.ThrowIfNull(configurePipeline);
        var criteria = match(new FileMatch()) ??
            throw new ArgumentException("The match returned null.", nameof(match));
        if (criteria.IsEmpty)
            throw new ArgumentException("A route names at least one source port; use OnAny for every port.",
                nameof(match));

        _routes.Add(new DeclaredExtractorRoute<TInput, TOutput, TCtx>(criteria, configurePipeline));
        return this;
    }

    /// <summary>Runs every file, from any source port, through one pipeline: the same data,
    /// received in several places.</summary>
    /// <param name="configurePipeline">Configures the pipeline builder, given the file's scope — the
    /// same shape as <c>AddExtractor</c>'s.</param>
    /// <typeparam name="TInput">The deserialized input.</typeparam>
    /// <typeparam name="TOutput">The published payload.</typeparam>
    /// <returns>The routes, for chaining.</returns>
    public ExtractorRoutes<TCtx> OnAny<TInput, TOutput>(
        Func<ExtractorBuilder<TInput, TOutput, TCtx>, IServiceProvider, ExtractorBuilder<TInput, TOutput, TCtx>> configurePipeline)
    {
        ArgumentNullException.ThrowIfNull(configurePipeline);
        _routes.Add(new DeclaredExtractorRoute<TInput, TOutput, TCtx>(null, configurePipeline));
        return this;
    }

    /// <summary>The declared routes, in declaration order.</summary>
    internal IReadOnlyList<ExtractorRoute<TCtx>> Routes => _routes;
}
