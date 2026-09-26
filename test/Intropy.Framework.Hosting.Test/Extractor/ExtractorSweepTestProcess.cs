using System.Collections.Concurrent;
using System.Text;
using Dapr.Client;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Hosting.Sweep;
using Intropy.Framework.Testing.Adapters;
using Intropy.Framework.Testing.Services;
using Intropy.Framework.Testing.Topics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NSubstitute;
using Intropy.Framework.Adapters.Common;

namespace Intropy.Framework.Hosting.Test.Extractor;

/// <summary>
/// Shared fixtures for the file-sweep tests: the composed definition (with an explicit
/// context factory), a probe for observing per-file scopes, derived-context processing, and
/// adapter wrappers that simulate behaviors <see cref="InMemoryFileAdapter"/> cannot express.
/// </summary>
internal static class ExtractorSweepTestProcess
{
    internal const string SourceKey = "source";

    /// <summary>The base test definition with a context factory that returns a fresh <see cref="ExtractorTestProcess.TestContext"/> with a caller-owned extra
    /// metadata entry, to prove caller metadata is preserved alongside the framework's
    /// <c>file_name</c> key and that metadata never leaks between files.</summary>
    internal static TestExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext> Definition() =>
        ExtractorTestProcess.Definition() with
        {
            ContextFactory = ExtractorTestProcess.ByFileName(NewCallerContext)
        };

    /// <summary>A variant whose validator aborts processing — used to pin Aborted handling
    /// both with and without a requested host cancellation. The test registers the
    /// <see cref="CancelAbortingValidator"/> instance to pass its cancellation trigger.</summary>
    internal static TestExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext> AbortingDefinition() =>
        Definition() with { Validator = typeof(CancelAbortingValidator) };

    /// <summary>A definition whose pipeline records each file's scope and context metadata via
    /// <see cref="ProbeEnrichment"/>.</summary>
    internal static TestExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext> ProbedDefinition() =>
        ExtractorTestProcess.Definition() with
        {
            ContextFactory = ExtractorTestProcess.ByFileName(NewCallerContext),
            Enrichments = [typeof(ProbeEnrichment)]
        };

    /// <summary>A definition on a context type with a required-argument constructor — proves the
    /// sweep supports derived contexts without reflection or a parameterless constructor.</summary>
    internal static TestExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output, DerivedContext> DerivedDefinition() => new()
    {
        SourcePort = SourceKey,
        Deserializer = typeof(DerivedDeserializer),
        Validator = typeof(DerivedValidator),
        Transformer = typeof(DerivedTransformer),
        InputIdentity = (input, _) => input.Id,
        InputDate = (input, _) => input.Version,
        OutputSubject = output => output.Subject,
        OutputTime = output => output.OccurredAt,
        IncidentIdentity = context => context.Metadata["file_name"],
        IncidentSubject = context => "file:" + context.Metadata["file_name"],
        ContextFactory = ExtractorTestProcess.ByFileName(fileName => new DerivedContext(
            new Dictionary<string, string> { ["caller"] = $"caller-{fileName}" },
            owner: $"owner-{fileName}"))
    };

    /// <summary>A fresh context per call, with caller-owned metadata on top of the context's
    /// own entries — the sweep must preserve both alongside its <c>file_name</c> key.</summary>
    private static ExtractorTestProcess.TestContext NewCallerContext(string fileName)
    {
        var context = new ExtractorTestProcess.TestContext();
        context.Metadata["caller"] = $"caller-{fileName}";
        return context;
    }

    /// <summary>A derived context with a required-argument constructor — no parameterless
    /// constructor exists, so the sweep cannot construct it without the explicit factory.
    /// Contexts are records, so derived types are records too, and this one carries state
    /// beyond the metadata dictionary.</summary>
    internal sealed record DerivedContext(string Owner) : Context(new Dictionary<string, string>())
    {
        public DerivedContext(Dictionary<string, string> metadata, string owner) : this(owner)
        {
            foreach (var (key, value) in metadata)
                Metadata[key] = value;
        }
    }

    /// <summary>Records one entry per processed file: file name, caller metadata, and the
    /// scope-unique probe id. Singleton per test provider.</summary>
    internal sealed class ProbeCollector
    {
        private readonly ConcurrentQueue<(string FileName, string Caller, Guid ScopeId)> _entries = [];
        private readonly ConcurrentQueue<Guid> _disposedScopeIds = [];

        public void Record(string fileName, string caller, Guid scopeId) => _entries.Enqueue((fileName, caller, scopeId));

        public void RecordDisposed(Guid scopeId) => _disposedScopeIds.Enqueue(scopeId);

        public IReadOnlyList<(string FileName, string Caller, Guid ScopeId)> Entries => [.. _entries];

        public IReadOnlyList<Guid> DisposedScopeIds => [.. _disposedScopeIds];
    }

    /// <summary>A scoped service whose lifetime and identity make per-file scope isolation
    /// observable.</summary>
    internal sealed class ScopeProbe(ProbeCollector collector) : IDisposable
    {
        public Guid Id { get; } = Guid.NewGuid();

        public bool Disposed { get; private set; }

        public void Dispose()
        {
            Disposed = true;
            collector.RecordDisposed(Id);
        }
    }

    internal sealed class ProbeEnrichment(ScopeProbe probe, ProbeCollector collector)
        : ExtractStep<ExtractorTestProcess.Input, ExtractorTestProcess.TestContext>
    {
        public override Task<(BusinessStepResult<ExtractorTestProcess.Input> Result, ExtractorTestProcess.TestContext Context)>
            ExecuteAsync(ExtractorTestProcess.Input input, ExtractorTestProcess.TestContext context, CancellationToken ct)
        {
            collector.Record(
                context.Metadata[SourceContextKeys.FileName],
                context.Metadata.GetValueOrDefault("caller", "<missing>"),
                probe.Id);
            return Task.FromResult<(BusinessStepResult<ExtractorTestProcess.Input>, ExtractorTestProcess.TestContext)>(
                (new BusinessStepResult<ExtractorTestProcess.Input>.Success(input), context));
        }
    }

    internal sealed class DerivedDeserializer : DeserializeStep<ExtractorTestProcess.Input, DerivedContext>
    {
        public override Task<(BusinessStepResult<ExtractorTestProcess.Input> Result, DerivedContext Context)>
            ExecuteAsync(string input, DerivedContext context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<ExtractorTestProcess.Input>, DerivedContext)>(
                (new BusinessStepResult<ExtractorTestProcess.Input>.Success(
                    System.Text.Json.JsonSerializer.Deserialize<ExtractorTestProcess.Input>(input)!), context));
    }

    internal sealed class DerivedValidator : ValidateStep<ExtractorTestProcess.Input, DerivedContext>
    {
        public override Task<(BusinessStepResult<ExtractorTestProcess.Input> Result, DerivedContext Context)>
            ExecuteAsync(ExtractorTestProcess.Input input, DerivedContext context, CancellationToken ct)
        {
            BusinessStepResult<ExtractorTestProcess.Input> result = input.Valid
                ? new BusinessStepResult<ExtractorTestProcess.Input>.Success(input)
                : new BusinessStepResult<ExtractorTestProcess.Input>.Failure(
                    new Intropy.Contracts.BusinessIncidentService.BusinessIncidentData { Description = "Input rejected" });
            return Task.FromResult((result, context));
        }
    }

    /// <summary>Transforms using derived context state set by the factory: a published output
    /// subject containing the owner proves the derived values survive the full pipeline.</summary>
    internal sealed class DerivedTransformer : TransformStep<ExtractorTestProcess.Input, ExtractorTestProcess.Output, DerivedContext>
    {
        public override Task<(TechnicalStepResult<ExtractorTestProcess.Output> Result, DerivedContext Context)>
            ExecuteAsync(ExtractorTestProcess.Input input, DerivedContext context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<ExtractorTestProcess.Output>, DerivedContext)>(
                (new TechnicalStepResult<ExtractorTestProcess.Output>.Success(
                    new ExtractorTestProcess.Output("owned-by-" + context.Owner, input.Version.AddHours(1))), context));
    }

    /// <summary>Validator used with <see cref="AbortingDefinition"/>: returns
    /// <see cref="BusinessStepResult{T}.Aborted"/>, optionally after requesting host
    /// cancellation — the difference between an interruption and an unexpected abort.</summary>
    internal sealed class CancelAbortingValidator(CancellationTokenSource cts, bool requestCancel)
        : ValidateStep<ExtractorTestProcess.Input, ExtractorTestProcess.TestContext>
    {
        public override async Task<(BusinessStepResult<ExtractorTestProcess.Input> Result, ExtractorTestProcess.TestContext Context)>
            ExecuteAsync(ExtractorTestProcess.Input input, ExtractorTestProcess.TestContext context, CancellationToken ct)
        {
            if (requestCancel)
                await cts.CancelAsync();
            return (new BusinessStepResult<ExtractorTestProcess.Input>.Aborted(), context);
        }
    }

    /// <summary>Returns empty content for one file (listed, but with no content) while
    /// delegating everything else.</summary>
    internal sealed class EmptyContentFileAdapter(IFileAdapter inner, string emptyContentFile) : IFileAdapter
    {
        public Task<List<FileEntry>> ListAsync(CancellationToken ct = default) => inner.ListAsync(ct);

        public Task<byte[]> GetContentAsync(string fileName, CancellationToken ct = default) => inner.GetContentAsync(fileName, ct);

        public async Task<string> GetContentAsync(string fileName, Encoding encoding, CancellationToken ct = default) =>
            fileName == emptyContentFile ? string.Empty : await inner.GetContentAsync(fileName, encoding, ct);

        public Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null, CancellationToken ct = default) =>
            inner.WriteAsync(fileName, content, basePathOverride, ct);

        public Task WriteAsync(string fileName, string content, Encoding encoding, string? basePathOverride = null, CancellationToken ct = default) =>
            inner.WriteAsync(fileName, content, encoding, basePathOverride, ct);

        public Task DeleteAsync(string fileName, CancellationToken ct = default) => inner.DeleteAsync(fileName, ct);
    }

    /// <summary>Cancels the host token after deleting a file, simulating a scheduler-level
    /// cancellation arriving between the completion of one file and the start of the next.</summary>
    internal sealed class CancelAfterDeleteFileAdapter(IFileAdapter inner, CancellationTokenSource cts) : IFileAdapter
    {
        public Task<List<FileEntry>> ListAsync(CancellationToken ct = default) => inner.ListAsync(ct);

        public Task<byte[]> GetContentAsync(string fileName, CancellationToken ct = default) => inner.GetContentAsync(fileName, ct);

        public Task<string> GetContentAsync(string fileName, Encoding encoding, CancellationToken ct = default) => inner.GetContentAsync(fileName, encoding, ct);

        public Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null, CancellationToken ct = default) =>
            inner.WriteAsync(fileName, content, basePathOverride, ct);

        public Task WriteAsync(string fileName, string content, Encoding encoding, string? basePathOverride = null, CancellationToken ct = default) =>
            inner.WriteAsync(fileName, content, encoding, basePathOverride, ct);

        public async Task DeleteAsync(string fileName, CancellationToken ct = default)
        {
            await inner.DeleteAsync(fileName, ct);
            await cts.CancelAsync();
        }
    }

    /// <summary>Cancels the host token in the middle of a read that does not forward the token it
    /// was given, and then completes the read normally.</summary>
    internal sealed class CancelDuringReadFileAdapter(IFileAdapter inner, CancellationTokenSource cts) : IFileAdapter
    {
        public Task<List<FileEntry>> ListAsync(CancellationToken ct = default) => inner.ListAsync(ct);

        public Task<byte[]> GetContentAsync(string fileName, CancellationToken ct = default) => inner.GetContentAsync(fileName, ct);

        public async Task<string> GetContentAsync(string fileName, Encoding encoding, CancellationToken ct = default)
        {
            // The read is not hooked to the token (doesn't forward it): the host cancellation
            // arrives mid-read and the read still completes normally.
            await Task.Delay(20, CancellationToken.None);
            await cts.CancelAsync();
            return await inner.GetContentAsync(fileName, encoding, ct);
        }

        public Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null, CancellationToken ct = default) =>
            inner.WriteAsync(fileName, content, basePathOverride, ct);

        public Task WriteAsync(string fileName, string content, Encoding encoding, string? basePathOverride = null, CancellationToken ct = default) =>
            inner.WriteAsync(fileName, content, encoding, basePathOverride, ct);

        public Task DeleteAsync(string fileName, CancellationToken ct = default) => inner.DeleteAsync(fileName, ct);
    }

    /// <summary>Cancels the host token after reading a specific file (the read itself is
    /// awaited to completion), simulating a cancellation signal arriving mid-processing.</summary>
    internal sealed class CancelAfterReadFileAdapter(IFileAdapter inner, CancellationTokenSource cts, string cancelAfterFile) : IFileAdapter
    {
        public Task<List<FileEntry>> ListAsync(CancellationToken ct = default) => inner.ListAsync(ct);

        public Task<byte[]> GetContentAsync(string fileName, CancellationToken ct = default) => inner.GetContentAsync(fileName, ct);

        public async Task<string> GetContentAsync(string fileName, Encoding encoding, CancellationToken ct = default)
        {
            var content = await inner.GetContentAsync(fileName, encoding, ct);
            if (fileName == cancelAfterFile)
                await cts.CancelAsync();
            return content;
        }

        public Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null, CancellationToken ct = default) =>
            inner.WriteAsync(fileName, content, basePathOverride, ct);

        public Task WriteAsync(string fileName, string content, Encoding encoding, string? basePathOverride = null, CancellationToken ct = default) =>
            inner.WriteAsync(fileName, content, encoding, basePathOverride, ct);

        public Task DeleteAsync(string fileName, CancellationToken ct = default) => inner.DeleteAsync(fileName, ct);
    }

    /// <summary>Throws <see cref="OperationCanceledException"/> during the read of one file:
    /// after requesting host cancellation (<paramref name="cancelFirst"/>) or without any
    /// host cancellation request at all.</summary>
    internal sealed class OceReadFileAdapter(IFileAdapter inner, CancellationTokenSource cts, bool cancelFirst, string oceFile) : IFileAdapter
    {
        public Task<List<FileEntry>> ListAsync(CancellationToken ct = default) => inner.ListAsync(ct);

        public Task<byte[]> GetContentAsync(string fileName, CancellationToken ct = default) => inner.GetContentAsync(fileName, ct);

        public async Task<string> GetContentAsync(string fileName, Encoding encoding, CancellationToken ct = default)
        {
            if (fileName == oceFile)
            {
                if (cancelFirst)
                    await cts.CancelAsync();
                throw new OperationCanceledException("Simulated cancellation surfacing during read");
            }
            return await inner.GetContentAsync(fileName, encoding, ct);
        }

        public Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null, CancellationToken ct = default) =>
            inner.WriteAsync(fileName, content, basePathOverride, ct);

        public Task WriteAsync(string fileName, string content, Encoding encoding, string? basePathOverride = null, CancellationToken ct = default) =>
            inner.WriteAsync(fileName, content, encoding, basePathOverride, ct);

        public Task DeleteAsync(string fileName, CancellationToken ct = default) => inner.DeleteAsync(fileName, ct);
    }

    /// <summary>Throws <see cref="OperationCanceledException"/> from <c>ListAsync</c>:
    /// after requesting host cancellation (<paramref name="cancelFirst"/>) or without any
    /// host cancellation request at all.</summary>
    internal sealed class OceListFileAdapter(IFileAdapter inner, CancellationTokenSource cts, bool cancelFirst) : IFileAdapter
    {
        public async Task<List<FileEntry>> ListAsync(CancellationToken ct = default)
        {
            if (cancelFirst)
                await cts.CancelAsync();
            throw new OperationCanceledException("Simulated cancellation surfacing during listing");
        }

        public Task<byte[]> GetContentAsync(string fileName, CancellationToken ct = default) => inner.GetContentAsync(fileName, ct);

        public Task<string> GetContentAsync(string fileName, Encoding encoding, CancellationToken ct = default) => inner.GetContentAsync(fileName, encoding, ct);

        public Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null, CancellationToken ct = default) =>
            inner.WriteAsync(fileName, content, basePathOverride, ct);

        public Task WriteAsync(string fileName, string content, Encoding encoding, string? basePathOverride = null, CancellationToken ct = default) =>
            inner.WriteAsync(fileName, content, encoding, basePathOverride, ct);

        public Task DeleteAsync(string fileName, CancellationToken ct = default) => inner.DeleteAsync(fileName, ct);
    }

    /// <summary>Counts <c>ListAsync</c> calls while delegating everything else, so tests can
    /// prove listing was (or was not) started.</summary>
    internal sealed class CountingListFileAdapter(IFileAdapter inner) : IFileAdapter
    {
        public int ListCalls { get; private set; }

        public async Task<List<FileEntry>> ListAsync(CancellationToken ct = default)
        {
            ListCalls++;
            return await inner.ListAsync(ct);
        }

        public Task<byte[]> GetContentAsync(string fileName, CancellationToken ct = default) => inner.GetContentAsync(fileName, ct);

        public Task<string> GetContentAsync(string fileName, Encoding encoding, CancellationToken ct = default) => inner.GetContentAsync(fileName, encoding, ct);

        public Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null, CancellationToken ct = default) =>
            inner.WriteAsync(fileName, content, basePathOverride, ct);

        public Task WriteAsync(string fileName, string content, Encoding encoding, string? basePathOverride = null, CancellationToken ct = default) =>
            inner.WriteAsync(fileName, content, encoding, basePathOverride, ct);

        public Task DeleteAsync(string fileName, CancellationToken ct = default) => inner.DeleteAsync(fileName, ct);
    }

    /// <summary>Scoped dependency whose <see cref="IAsyncDisposable.DisposeAsync"/> throws
    /// <see cref="OperationCanceledException"/> when <paramref name="throwOnDispose"/> is
    /// set — simulating a cleanup cancellation the host never requested.</summary>
    internal sealed class ScopeCleanupOceDependency(bool throwOnDispose) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            if (throwOnDispose)
                throw new OperationCanceledException("Simulated cancellation during scope cleanup");
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>A deserializer, like the base test one, that drags
    /// <see cref="ScopeCleanupOceDependency"/> into every processing scope.</summary>
    internal sealed class CleanupOceDeserializer(ScopeCleanupOceDependency dependency)
        : DeserializeStep<ExtractorTestProcess.Input, ExtractorTestProcess.TestContext>
    {
        // Kept alive so its dispose failure participates in every processing scope's cleanup.
        public ScopeCleanupOceDependency Dependency { get; } = dependency;

        public override Task<(BusinessStepResult<ExtractorTestProcess.Input> Result, ExtractorTestProcess.TestContext Context)>
            ExecuteAsync(string input, ExtractorTestProcess.TestContext context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<ExtractorTestProcess.Input>, ExtractorTestProcess.TestContext)>(
                (new BusinessStepResult<ExtractorTestProcess.Input>.Success(
                    System.Text.Json.JsonSerializer.Deserialize<ExtractorTestProcess.Input>(input)!), context));
    }

    /// <summary>The four externals every sweep test fakes. Adapter overrides (for behaviors
    /// the in-memory adapter cannot express) are passed via <paramref name="adapterOverride"/>.</summary>
    internal sealed class SweepFakes
    {
        public InMemoryFileAdapter Files { get; } = new();
        public FakeTopic<ExtractorTestProcess.TestContext> Topic { get; } = new();
        public FakeIdempotencyServiceClient Idempotency { get; } = new();
        public FakeBusinessIncidentServiceClient Incidents { get; } = new();

        public ServiceCollection CreateServices(
            TestExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext>? definition = null,
            IFileAdapter? adapterOverride = null,
            Action<IServiceCollection>? configure = null)
        {
            var services = CreateEdgeServices(adapterOverride);
            services.AddExtractor(definition ?? Definition());
            configure?.Invoke(services);
            return services;
        }

        /// <summary>The caller-owned edges only — no pipeline and no run-to-completion host;
        /// callers compose those via <c>AddExtractor</c>.</summary>
        public ServiceCollection CreateEdgeServices(IFileAdapter? adapterOverride = null)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddTestIdentity();
            // The base test deserializer resolves this scoped dependency.
            services.AddScoped<ExtractorTestProcess.ScopedDependency>();
            services.AddSingleton(Substitute.For<DaprClient>());
            services.AddSingleton(Topic);
            services.AddSingleton<SendStep<ExtractorTestProcess.TestContext>>(Topic);
            services.AddSingleton(Idempotency);
            services.AddSingleton<IIdempotencyServiceClient>(p => p.GetRequiredService<FakeIdempotencyServiceClient>());
            services.AddSingleton(Incidents);
            services.AddSingleton<IBusinessIncidentServiceClient>(p => p.GetRequiredService<FakeBusinessIncidentServiceClient>());
            services.AddKeyedSingleton<IFileAdapter>(SourceKey, adapterOverride ?? Files);
            return services;
        }
    }
}
