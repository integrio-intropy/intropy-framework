using Intropy.Framework.Blocks.Extractor;
using Microsoft.Extensions.DependencyInjection;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Jobs;

namespace Intropy.Framework.Hosting.Extractor;

/// <summary>
/// Describes an extractor component for
/// <see cref="ExtractorServiceCollectionExtensions.AddExtractor{TInput,TOutput,TCtx}(IServiceCollection, ExtractorDefinition{TInput,TOutput,TCtx})"/>:
/// the pipeline, built per file with <see cref="ExtractorBuilder{TInput,TOutput,TCtx}"/>, together
/// with the settings the registration needs around it — the source port, the context factory, and
/// the runner options. The definition is validated when it is registered, so a misconfigured
/// component fails at startup with the member that is wrong and how to fix it, not on the first
/// file.
/// </summary>
/// <remarks>
/// Build one with an object initializer:
/// <code>
/// services.AddExtractor(new ExtractorDefinition&lt;Order, OrderPublished, OrderContext&gt;
/// {
///     SourcePort = "orders-inbox",
///     ContextFactory = (metadata, isRetry) =&gt; new OrderContext(metadata, isRetry),
///     Pipeline = (builder, services) =&gt; builder
///         .WithDeserializer&lt;OrderDeserializer&gt;()
///         .WithValidator&lt;OrderValidator&gt;(),
/// });
/// </code>
/// </remarks>
/// <typeparam name="TInput">The deserialized input.</typeparam>
/// <typeparam name="TOutput">The published payload.</typeparam>
/// <typeparam name="TCtx">The pipeline context.</typeparam>
public sealed class ExtractorDefinition<TInput, TOutput, TCtx> where TCtx : Context
{
    /// <summary>The port the component sweeps, as declared in the system's topology; the
    /// registration calls <c>AddSourcePort</c> with it
    /// (and <see cref="Completion"/>), so the component identity and the source it sweeps are
    /// described in one place. Optional: leave null when the component registers the source port
    /// itself — a test fake, or an adapter whose registration needs configuration.</summary>
    public string? SourcePort { get; init; }

    /// <summary>What happens to a handled source file, for <see cref="SourcePort"/>. Default:
    /// deleted.</summary>
    public FileCompletion? Completion { get; init; }

    /// <summary>Creates the context for each source file. Required when <typeparamref name="TCtx"/>
    /// derives from <see cref="Context"/>; when <typeparamref name="TCtx"/> is the base
    /// <see cref="Context"/>, leave null and the plain record is created.</summary>
    public ContextFactory<TCtx>? ContextFactory { get; init; }

    /// <summary>Configures the pipeline builder, given the file's scope — the deserializer,
    /// validator, idempotency, transformer, serializer, sender and business-incident routing. This
    /// is where the generic step overloads (<c>WithDeserializer&lt;TStep&gt;()</c> and friends)
    /// save the cast-and-resolve boilerplate.</summary>
    public required Func<ExtractorBuilder<TInput, TOutput, TCtx>, IServiceProvider,
        ExtractorBuilder<TInput, TOutput, TCtx>> Pipeline { get; init; }

    /// <summary>Optional runner settings: the job name (default: the component name) and the
    /// sidecar timeouts.</summary>
    public Action<JobOptions>? ConfigureJob { get; init; }

    /// <summary>Checks the definition and returns the registration-ready pipeline configurator, so
    /// callers fail fast with the offending member instead of a null dereference when the first
    /// file is composed.</summary>
    /// <returns>The validated pipeline configurator.</returns>
    /// <exception cref="InvalidOperationException"><see cref="Pipeline"/> is missing, or
    /// <typeparamref name="TCtx"/> needs a <see cref="ContextFactory"/> that is not set.</exception>
    internal Func<ExtractorBuilder<TInput, TOutput, TCtx>, IServiceProvider,
        ExtractorBuilder<TInput, TOutput, TCtx>> Validate()
    {
        if (Pipeline is null)
            throw new InvalidOperationException(
                "Extractor composition failed: Pipeline — the definition has no pipeline configurator; set Pipeline.");

        return Pipeline;
    }

    /// <summary>The registration-ready context factory: the definition's own, or the default for
    /// the base <see cref="Context"/>.</summary>
    internal ContextFactory<TCtx> ResolveContextFactory() =>
        ContextFactory ?? ComponentRegistration.DefaultContextFactory<TCtx>("Extractor", nameof(ContextFactory));
}
