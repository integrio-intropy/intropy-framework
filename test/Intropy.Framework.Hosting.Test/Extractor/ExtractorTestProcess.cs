using Microsoft.Extensions.DependencyInjection;
using Intropy.Framework.Core.Configuration;
using System.Text.Json;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Extractor;

namespace Intropy.Framework.Hosting.Test.Extractor;

internal static class ExtractorTestProcess
{
    internal const string ComponentName = "orders-extractor";

    /// <summary>A context factory written against the source file name the framework seeds.</summary>
    internal static ContextFactory<TCtx> ByFileName<TCtx>(Func<string, TCtx> create) where TCtx : Context =>
        (metadata, _) => create(metadata[SourceContextKeys.FileName]);

    /// <summary>Registers the component identity the extractor reads, as a host would.</summary>
    internal static IServiceCollection AddTestIdentity(this IServiceCollection services,
        string componentName = ComponentName) =>
        services.AddIntropyFramework(options =>
        {
            options.ComponentName = componentName;
            options.ServiceNamespace = "example";
        });

    internal const string ValidInput = """{"Id":"input-42","Version":"2026-01-15T09:30:00+00:00","Valid":true}""";
    internal const string InvalidInput = """{"Id":"input-42","Version":"2026-01-15T09:30:00+00:00","Valid":false}""";
    internal static readonly DateTimeOffset InputTime = new(2026, 1, 15, 9, 30, 0, TimeSpan.Zero);

    internal static TestExtractor<Input, Output, TestContext> Definition() => new()
    {
        SourcePort = "source",
        Deserializer = typeof(Deserializer),
        Validator = typeof(Validator),
        Transformer = typeof(Transformer),
        Enrichments = [typeof(FirstEnrichment), typeof(SecondEnrichment)],
        InputIdentity = (input, _) => input.Id,
        InputDate = (input, _) => input.Version,
        InputHash = input => "hash-" + input.Id,
        OutputSubject = output => output.Subject,
        OutputTime = output => output.OccurredAt,
        IncidentIdentity = context => context.Metadata["file_name"],
        IncidentSubject = context => "file:" + context.Metadata["file_name"],
        ContextFactory = (_, _) => new TestContext()
    };

    internal sealed record Input(string Id, DateTimeOffset Version, bool Valid);
    internal sealed record Output(string Subject, DateTimeOffset OccurredAt);
    internal sealed record TestContext() : Context(new Dictionary<string, string> { ["file_name"] = "order.json" })
    {
        public List<string> Steps { get; } = [];
        public Guid DependencyId { get; set; }
    }

    internal sealed class ScopedDependency : IDisposable
    {
        public Guid Id { get; } = Guid.NewGuid();
        public bool Disposed { get; private set; }
        public void Dispose() => Disposed = true;
    }

    internal sealed class Deserializer(ScopedDependency dependency) : DeserializeStep<Input, TestContext>
    {
        public ScopedDependency Dependency { get; } = dependency;
        public override Task<(BusinessStepResult<Input> Result, TestContext Context)> ExecuteAsync(string input, TestContext context, CancellationToken ct)
        {
            context.Steps.Add("deserialize");
            context.DependencyId = Dependency.Id;
            return Task.FromResult<(BusinessStepResult<Input>, TestContext)>((new BusinessStepResult<Input>.Success(JsonSerializer.Deserialize<Input>(input)!), context));
        }
    }

    internal sealed class Validator : ValidateStep<Input, TestContext>
    {
        public override Task<(BusinessStepResult<Input> Result, TestContext Context)> ExecuteAsync(Input input, TestContext context, CancellationToken ct)
        {
            context.Steps.Add("validate");
            BusinessStepResult<Input> result = input.Valid
                ? new BusinessStepResult<Input>.Success(input)
                : new BusinessStepResult<Input>.Failure(new BusinessIncidentData { Description = "Input rejected" });
            return Task.FromResult((result, context));
        }
    }

    internal sealed class FirstEnrichment : ExtractStep<Input, TestContext>
    {
        public override Task<(BusinessStepResult<Input> Result, TestContext Context)> ExecuteAsync(Input input, TestContext context, CancellationToken ct)
        {
            context.Steps.Add("first");
            return Task.FromResult<(BusinessStepResult<Input>, TestContext)>((new BusinessStepResult<Input>.Success(input), context));
        }
    }

    internal sealed class SecondEnrichment : ExtractStep<Input, TestContext>
    {
        public override Task<(BusinessStepResult<Input> Result, TestContext Context)> ExecuteAsync(Input input, TestContext context, CancellationToken ct)
        {
            context.Steps.Add("second");
            return Task.FromResult<(BusinessStepResult<Input>, TestContext)>((new BusinessStepResult<Input>.Success(input), context));
        }
    }

    internal sealed class Transformer : TransformStep<Input, Output, TestContext>
    {
        public override Task<(TechnicalStepResult<Output> Result, TestContext Context)> ExecuteAsync(Input input, TestContext context, CancellationToken ct)
        {
            context.Steps.Add("transform");
            return Task.FromResult<(TechnicalStepResult<Output>, TestContext)>((new TechnicalStepResult<Output>.Success(new Output("event-" + input.Id, input.Version.AddHours(1))), context));
        }
    }
}
