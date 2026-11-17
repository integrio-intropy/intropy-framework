using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Sweeps;
using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Extractor;

/// <summary>Runs one source item's bytes through a route's extractor pipeline: the pipeline with its
/// input and output types bound, so a port sweep needs only the context type.</summary>
internal delegate Task<StepResult<CloudEvent>> ExtractorRun<in TCtx>(ReadOnlyMemory<byte> content, TCtx context,
    CancellationToken ct) where TCtx : Context;

/// <summary>
/// A route of an extractor: the pipeline the items of the source ports it handles go through. An
/// extractor has no queue hop, so a port's route is chosen once, when the job starts, and each port
/// sweep resolves its route's pipeline in every item's scope.
/// </summary>
/// <typeparam name="TCtx">The extractor's context type.</typeparam>
internal abstract class ExtractorRoute<TCtx> where TCtx : Context
{
    /// <summary>The items the route handles, by their source port; null for every item.</summary>
    public abstract PortMatch? Match { get; }

    /// <summary>The route's pipeline for the items of <paramref name="port"/>, in
    /// <paramref name="scope"/>.</summary>
    /// <exception cref="InvalidOperationException">The pipeline cannot be composed.</exception>
    public abstract ExtractorRun<TCtx> Resolve(IServiceProvider scope, string componentName, string port);

    /// <summary>Runs <paramref name="pipeline"/> as a host does: continuing the item's trace.</summary>
    protected static ExtractorRun<TCtx> Run<TInput, TOutput>(Extractor<TInput, TOutput, TCtx> pipeline) =>
        async (content, context, ct) => (await pipeline.Execute(content, context, detachTrace: false, ct)).Result;
}

/// <summary>The route of an extractor whose one pipeline is registered in the service provider (the
/// overloads without routes): every item, through the scoped
/// <see cref="Extractor{TInput,TOutput,TCtx}"/>.</summary>
internal sealed class RegisteredExtractorRoute<TInput, TOutput, TCtx> : ExtractorRoute<TCtx> where TCtx : Context
{
    public override PortMatch? Match => null;

    public override ExtractorRun<TCtx> Resolve(IServiceProvider scope, string componentName, string port) =>
        Run(scope.GetRequiredService<Extractor<TInput, TOutput, TCtx>>());
}

/// <summary>A route declared with <see cref="ExtractorRoutes{TCtx}"/>: the items its match selects,
/// or every item (a null match), through a pipeline built for the item's port in the item's own
/// scope — so the pipeline stamps the port on its events and keeps its idempotency records per port
/// (<see cref="ExtractorBuilder{TInput,TOutput,TCtx}.Create(string, IServiceProvider, string)"/>).</summary>
internal sealed class DeclaredExtractorRoute<TInput, TOutput, TCtx>(
    PortMatch? match,
    Func<ExtractorBuilder<TInput, TOutput, TCtx>, IServiceProvider, ExtractorBuilder<TInput, TOutput, TCtx>> configurePipeline)
    : ExtractorRoute<TCtx> where TCtx : Context
{
    public override PortMatch? Match => match;

    public override ExtractorRun<TCtx> Resolve(IServiceProvider scope, string componentName, string port)
    {
        var name = match?.Describe();
        return Run(RouteComposition.Build("Extractor", name, componentName, () =>
            // The claim check and the archive go on after the route's own configuration, the same as
            // for an extractor without routes: every route of a component shares their settings.
            ExtractorClaimCheck.Apply(
                configurePipeline(ExtractorBuilder<TInput, TOutput, TCtx>.Create(
                        name is null ? $"{componentName}.Process" : $"{componentName}.Process.{name}", scope, port),
                    scope), scope)
                .WithArchive(ArchiveStoreServiceCollectionExtensions.PolicyFor(scope))
                .Build()));
    }
}
