using System.Diagnostics;
using System.Text;
using Acme.Adapters.Inbox;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.Sweeps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Intropy.Framework.Hosting.Extensibility.Test;

/// <summary>
/// A kind of source written outside Hosting on <see cref="SourceSweep{TEntry, TItem}"/> follows
/// the same rules as a file source without implementing any of them: failed, duplicate and
/// throwing items, the host's cancellation before, between and during items, a cancellation the
/// host did not request, a listing that fails partway, and each item's own trace.
/// </summary>
public class SourceSweepSemanticsTests
{
    private readonly Inbox _inbox = new();
    private readonly ServiceProvider _services = new ServiceCollection().BuildServiceProvider();

    [Fact]
    public async Task AFailedItem_StaysInItsSource_AndTheOthersAreHandled()
    {
        _inbox.Add("a", "ok").Add("b", "fail").Add("c", "ok");

        var summary = await SweepAsync(item => item.Id == "b" ? SweepOutcome.Failed : SweepOutcome.Consumed);

        Assert.Equal(new RunSummary(2, 1, 0), summary);
        Assert.Equal(["b"], _inbox.Pending.Select(m => m.Id));
    }

    [Fact]
    public async Task ADuplicate_IsCompletedAndCountedAsSkipped()
    {
        _inbox.Add("a", "seen before");

        var summary = await SweepAsync(_ => SweepOutcome.Duplicate);

        Assert.Equal(new RunSummary(0, 0, 1), summary);
        Assert.Empty(_inbox.Pending);
    }

    [Fact]
    public async Task AThrowingHandler_FailsOnlyItsItem()
    {
        _inbox.Add("a", "boom").Add("b", "ok");

        var summary = await SweepAsync(item => item.Id == "a"
            ? throw new InvalidOperationException("The handler failed")
            : SweepOutcome.Consumed);

        Assert.Equal(new RunSummary(1, 1, 0), summary);
        Assert.Equal(["a"], _inbox.Pending.Select(m => m.Id));
    }

    [Fact]
    public async Task AHostCancellationBeforeTheRun_ListsNothing()
    {
        _inbox.Add("a", "ok");
        using var host = new CancellationTokenSource();
        await host.CancelAsync();

        var summary = await SweepAsync(_ => SweepOutcome.Consumed, host.Token);

        Assert.Equal(RunSummary.Empty, summary);
        Assert.Equal(0, _inbox.Listings);
        Assert.Single(_inbox.Pending);
    }

    [Fact]
    public async Task AHostCancellationBetweenItems_LeavesTheRestUnstarted()
    {
        _inbox.Add("a", "ok").Add("b", "ok").Add("c", "ok");
        using var host = new CancellationTokenSource();
        var handled = new List<string>();

        var summary = await SweepAsync(item =>
        {
            handled.Add(item.Id);
            host.Cancel(); // the host stops after this item is handled
            return SweepOutcome.Consumed;
        }, host.Token);

        Assert.Equal(["a"], handled);
        Assert.Equal(new RunSummary(1, 0, 0), summary);
        Assert.Equal(["b", "c"], _inbox.Pending.Select(m => m.Id));
    }

    [Fact]
    public async Task AHostCancellationDuringAHandler_AbortsTheItemWithoutCompletingIt()
    {
        _inbox.Add("a", "ok").Add("b", "ok");
        using var host = new CancellationTokenSource();
        var handled = new List<string>();

        var summary = await SweepAsync(item =>
        {
            handled.Add(item.Id);
            host.Cancel();
            host.Token.ThrowIfCancellationRequested();
            return SweepOutcome.Consumed;
        }, host.Token);

        // Interrupted: not completed, not counted, and the sweep stops.
        Assert.Equal(["a"], handled);
        Assert.Equal(RunSummary.Empty, summary);
        Assert.Equal(["a", "b"], _inbox.Pending.Select(m => m.Id));
    }

    [Fact]
    public async Task ACancellationTheHostDidNotRequest_FailsTheItem()
    {
        _inbox.Add("a", "ok").Add("b", "ok");

        var summary = await SweepAsync(item => item.Id == "a"
            ? throw new OperationCanceledException("A timeout inside the handler")
            : SweepOutcome.Consumed);

        Assert.Equal(new RunSummary(1, 1, 0), summary);
        Assert.Equal(["a"], _inbox.Pending.Select(m => m.Id));
    }

    [Fact]
    public async Task AListingThatFailsPartway_PropagatesAfterTheItemsAlreadyHandled()
    {
        _inbox.Add("a", "ok").Add("b", "ok");
        _inbox.FailBeforeMessage = index => index == 1 ? new IOException("The inbox went away") : null;

        await Assert.ThrowsAsync<IOException>(() => SweepAsync(_ => SweepOutcome.Consumed));

        // Never reported as a clean sweep, and what was handled before the failure stays handled.
        Assert.Equal(["b"], _inbox.Pending.Select(m => m.Id));
    }

    [Fact]
    public async Task AListingCancelledWithoutAHostCancellation_FailsTheSweep()
    {
        _inbox.Add("a", "ok");
        _inbox.FailBeforeMessage = _ => new OperationCanceledException("The listing timed out");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => SweepAsync(_ => SweepOutcome.Consumed));

        Assert.Contains("listing source port 'inbox' failed with an unexpected cancellation", error.Message);
    }

    [Fact]
    public async Task AnEmptySource_IsACleanEmptySweep()
    {
        var summary = await SweepAsync(_ => SweepOutcome.Consumed);

        Assert.Equal(RunSummary.Empty, summary);
        Assert.Equal(1, _inbox.Listings);
    }

    [Fact]
    public async Task EachItem_IsItsOwnTrace_LinkedToTheRunsSpan_AndTaggedWithItsOutcome()
    {
        _inbox.Add("a", "ok", batch: "b-7").Add("b", "fail", batch: "b-7");
        using var tracing = new TraceCapture();
        using var jobSource = new ActivitySource("Acme.Tests.Job");
        using var listener = TraceCapture.ListenTo(jobSource.Name);

        using (var job = jobSource.StartActivity("run job"))
        {
            Assert.NotNull(job);
            await SweepAsync(item => item.Id == "b" ? SweepOutcome.Failed : SweepOutcome.Consumed);
            tracing.LinkedTo = job.TraceId;
        }

        var spans = tracing.Items();
        Assert.Equal(["a", "b"], spans.Select(s => s.GetTagItem("intropy.source.item")).Order());
        Assert.All(spans, span =>
        {
            Assert.Equal("process inbox", span.OperationName);
            Assert.Equal("inbox", span.GetTagItem("intropy.source.port"));
            Assert.Equal("b-7", span.GetTagItem("inbox.batch"));
        });
        Assert.NotEqual(spans[0].TraceId, spans[1].TraceId);
        var consumed = spans.Single(s => (string?)s.GetTagItem("intropy.source.item") == "a");
        var failed = spans.Single(s => (string?)s.GetTagItem("intropy.source.item") == "b");
        Assert.Equal("consumed", consumed.GetTagItem("intropy.sweep.outcome"));
        Assert.Equal(ActivityStatusCode.Unset, consumed.Status);
        Assert.Equal("failed", failed.GetTagItem("intropy.sweep.outcome"));
        Assert.Equal(ActivityStatusCode.Error, failed.Status);
    }

    [Fact]
    public async Task AThrowingIdOf_FailsOnlyItsEntry_UnderADiagnosticIdentity()
    {
        _inbox.Add("a", "ok").Add("b", "ok");
        _inbox.IdOf = m => m.Id == "a" ? throw new FormatException("No id in the message") : m.Id;
        using var tracing = new TraceCapture();
        var handled = new List<string>();

        var summary = await tracing.WithinJobAsync(() => SweepAsync(item =>
        {
            handled.Add(item.Id);
            return SweepOutcome.Consumed;
        }));

        Assert.Equal(new RunSummary(1, 1, 0), summary);
        Assert.Equal(["b"], handled);
        Assert.Equal(["a"], _inbox.Pending.Select(m => m.Id));
        var failed = tracing.Items().Single(s => (string?)s.GetTagItem("intropy.sweep.outcome") == "failed");
        Assert.Equal("entry#1", failed.GetTagItem("intropy.source.item"));
        Assert.Equal(ActivityStatusCode.Error, failed.Status);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public async Task AnIdOfWithoutAnIdentity_FailsOnlyItsEntry_UnderADiagnosticIdentity(string? missing)
    {
        _inbox.Add("a", "ok").Add("b", "ok");
        _inbox.IdOf = m => m.Id == "b" ? missing : m.Id;
        using var tracing = new TraceCapture();
        var handled = new List<string>();

        var summary = await tracing.WithinJobAsync(() => SweepAsync(item =>
        {
            handled.Add(item.Id);
            return SweepOutcome.Consumed;
        }));

        Assert.Equal(new RunSummary(1, 1, 0), summary);
        Assert.Equal(["a"], handled);
        Assert.Equal(["b"], _inbox.Pending.Select(m => m.Id));
        var failed = tracing.Items().Single(s => (string?)s.GetTagItem("intropy.sweep.outcome") == "failed");
        Assert.Equal("entry#2", failed.GetTagItem("intropy.source.item"));
    }

    [Fact]
    public async Task AThrowingTelemetryOf_FailsOnlyItsEntry_UnderItsRealIdentity()
    {
        _inbox.Add("a", "ok").Add("b", "ok");
        _inbox.TelemetryOf = m => m.Id == "a"
            ? throw new InvalidOperationException("Telemetry mapping failed")
            : new SweepItemTelemetry();
        using var tracing = new TraceCapture();
        var handled = new List<string>();

        var summary = await tracing.WithinJobAsync(() => SweepAsync(item =>
        {
            handled.Add(item.Id);
            return SweepOutcome.Consumed;
        }));

        Assert.Equal(new RunSummary(1, 1, 0), summary);
        Assert.Equal(["b"], handled);
        Assert.Equal(["a"], _inbox.Pending.Select(m => m.Id));
        var failed = tracing.Items().Single(s => (string?)s.GetTagItem("intropy.sweep.outcome") == "failed");
        Assert.Equal("a", failed.GetTagItem("intropy.source.item"));
    }

    [Theory]
    [InlineData(Hook.IdOf)]
    [InlineData(Hook.TelemetryOf)]
    public async Task AMappingCancelledWithoutAHostCancellation_FailsOnlyItsEntry(Hook hook)
    {
        _inbox.Add("a", "ok").Add("b", "ok");
        FailIn(hook, m => m.Id == "a" ? new OperationCanceledException("A timeout in the mapping") : null);

        var summary = await SweepAsync(_ => SweepOutcome.Consumed);

        Assert.Equal(new RunSummary(1, 1, 0), summary);
        Assert.Equal(["a"], _inbox.Pending.Select(m => m.Id));
    }

    [Theory]
    [InlineData(Hook.IdOf)]
    [InlineData(Hook.TelemetryOf)]
    public async Task AMappingInterruptedByTheHost_AbortsTheSweep(Hook hook)
    {
        _inbox.Add("a", "ok").Add("b", "ok");
        using var host = new CancellationTokenSource();
        FailIn(hook, _ =>
        {
            host.Cancel();
            return new OperationCanceledException(host.Token);
        });
        var handled = new List<string>();

        var summary = await SweepAsync(item =>
        {
            handled.Add(item.Id);
            return SweepOutcome.Consumed;
        }, host.Token);

        // Interrupted: nothing completed or counted, and no later entry started.
        Assert.Empty(handled);
        Assert.Equal(RunSummary.Empty, summary);
        Assert.Equal(["a", "b"], _inbox.Pending.Select(m => m.Id));
    }

    [Fact]
    public async Task ASourcesTelemetry_CannotReplaceTheFrameworksIdentityTags()
    {
        _inbox.Add("a", "ok").Add("b", "ok");
        _inbox.TelemetryOf = _ => new SweepItemTelemetry
        {
            SpanTags = new Dictionary<string, object?>
            {
                ["intropy.source.port"] = "spoofed-port",
                ["intropy.source.item"] = "spoofed-item",
                ["inbox.batch"] = "b-9",
            },
        };
        using var tracing = new TraceCapture();

        await tracing.WithinJobAsync(() => SweepAsync(_ => SweepOutcome.Consumed));

        var spans = tracing.Items();
        Assert.Equal(["a", "b"], spans.Select(s => s.GetTagItem("intropy.source.item")).Order());
        Assert.All(spans, span =>
        {
            Assert.Equal("inbox", span.GetTagItem("intropy.source.port"));
            Assert.Equal("b-9", span.GetTagItem("inbox.batch"));
        });
    }

    public enum Hook
    {
        IdOf,
        TelemetryOf,
    }

    private void FailIn(Hook hook, Func<InboxMessage, Exception?> failure)
    {
        if (hook is Hook.IdOf)
            _inbox.IdOf = m => failure(m) is { } e ? throw e : m.Id;
        else
            _inbox.TelemetryOf = m => failure(m) is { } e ? throw e : new SweepItemTelemetry();
    }

    private Task<RunSummary> SweepAsync(Func<SweptItem, SweepOutcome> handle, CancellationToken ct = default)
    {
        var sweep = new InboxSourcePort("inbox", _inbox)
            .CreateSweep(new SourceSweepContext(_services, "acme-component", NullLoggerFactory.Instance));
        return sweep.SweepAsync(async (item, _) =>
        {
            await item.ReadTextAsync(Encoding.UTF8, CancellationToken.None);
            return handle(item);
        }, ct);
    }

    /// <summary>Captures the framework's item spans that link to one test's job span, so spans
    /// from tests running in parallel are ignored.</summary>
    private sealed class TraceCapture : IDisposable
    {
        private readonly List<Activity> _stopped = [];
        private readonly ActivityListener _listener;

        public TraceCapture()
        {
            _listener = ListenTo("Intropy.Framework.Hosting", _stopped);
        }

        public ActivityTraceId? LinkedTo { get; set; }

        /// <summary>Runs <paramref name="sweep"/> under a job span of its own and captures the
        /// item spans linked to it.</summary>
        public async Task<T> WithinJobAsync<T>(Func<Task<T>> sweep)
        {
            using var jobSource = new ActivitySource("Acme.Tests.Job");
            using var listener = ListenTo(jobSource.Name);
            using var job = jobSource.StartActivity("run job");
            Assert.NotNull(job);
            LinkedTo = job.TraceId;
            return await sweep();
        }

        public List<Activity> Items()
        {
            lock (_stopped)
                return [.. _stopped.Where(a => a.Links.Any(l => l.Context.TraceId == LinkedTo))];
        }

        public static ActivityListener ListenTo(string source, List<Activity>? stopped = null)
        {
            var listener = new ActivityListener
            {
                ShouldListenTo = s => s.Name == source,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    if (stopped is null)
                        return;
                    lock (stopped)
                        stopped.Add(activity);
                },
            };
            ActivitySource.AddActivityListener(listener);
            return listener;
        }

        public void Dispose() => _listener.Dispose();
    }
}
