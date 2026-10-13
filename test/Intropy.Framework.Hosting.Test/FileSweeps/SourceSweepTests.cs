using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Intropy.Framework.Hosting.Test.FileSweeps;

/// <summary>
/// The run's sweep over a component's source ports: no port fails the run before anything is
/// swept, every port is validated before any port is swept, the ports' summaries are added
/// together in registration order, and a failing port does not stop the others but still fails
/// the run, whose error names every failed port.
/// </summary>
public class SourceSweepTests
{
    private static readonly FrameworkOptions Identity = new() { ComponentName = "test-component", ServiceNamespace = "test" };

    [Fact]
    public async Task SweepAsync_WithoutASourcePort_FailsWithoutCreatingASweep()
    {
        var created = new List<string>();
        var sweep = Sweep([], port => new RecordingPortSweep(port.Name, created));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => sweep.SweepAsync(CancellationToken.None));

        Assert.Equal("Component 'test-component': no source port is registered. Call AddSourcePort.", error.Message);
        Assert.Empty(created);
    }

    [Fact]
    public async Task SweepAsync_ValidatesEveryPortBeforeSweepingAny()
    {
        var calls = new List<string>();
        var sweep = Sweep(["orders-in", "invoices-in"], port =>
            new RecordingPortSweep(port.Name, calls, failValidation: port.Name == "invoices-in"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => sweep.SweepAsync(CancellationToken.None));

        // The second port's misconfiguration is found before the first port lists a file.
        Assert.Equal(["validate orders-in", "validate invoices-in"], calls);
    }

    [Fact]
    public async Task SweepAsync_SweepsEachPortInRegistrationOrder_AndAddsTheirSummaries()
    {
        var calls = new List<string>();
        var summaries = new Dictionary<string, RunSummary>
        {
            ["orders-in"] = new(Processed: 2, Failed: 1, Skipped: 0),
            ["invoices-in"] = new(Processed: 3, Failed: 0, Skipped: 4),
        };
        var sweep = Sweep(["orders-in", "invoices-in"], port =>
            new RecordingPortSweep(port.Name, calls, summary: summaries[port.Name]));

        var summary = await sweep.SweepAsync(CancellationToken.None);

        Assert.Equal(new RunSummary(Processed: 5, Failed: 1, Skipped: 4), summary);
        Assert.Equal(["validate orders-in", "validate invoices-in", "sweep orders-in", "sweep invoices-in"], calls);
    }

    [Fact]
    public async Task SweepAsync_WithOnePort_IsolatesItsFailure_ThenFailsTheRunNamingIt()
    {
        // A listing failure is never reported as an empty successful sweep, whatever the port count.
        var calls = new List<string>();
        var listingFailure = new IOException("SFTP unreachable");
        var sweep = Sweep(["orders-in"], port => new RecordingPortSweep(port.Name, calls,
            sweepFailure: listingFailure));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => sweep.SweepAsync(CancellationToken.None));

        Assert.Equal("Component 'test-component': sweeping source port(s) 'orders-in' failed", error.Message);
        var inner = Assert.IsType<AggregateException>(error.InnerException);
        Assert.Same(listingFailure, Assert.Single(inner.InnerExceptions));
    }

    [Fact]
    public async Task SweepAsync_WithSeveralPorts_SweepsTheOthersWhenOneFails_ThenFailsTheRunNamingIt()
    {
        var calls = new List<string>();
        var listingFailure = new IOException("SFTP unreachable");
        var sweep = Sweep(["orders-in", "invoices-in", "stock-in"], port => port.Name == "orders-in"
            ? new RecordingPortSweep(port.Name, calls, sweepFailure: listingFailure)
            : new RecordingPortSweep(port.Name, calls, summary: new RunSummary(Processed: 1, Failed: 0, Skipped: 0)));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => sweep.SweepAsync(CancellationToken.None));

        Assert.Equal(["validate orders-in", "validate invoices-in", "validate stock-in",
            "sweep orders-in", "sweep invoices-in", "sweep stock-in"], calls);
        Assert.Equal("Component 'test-component': sweeping source port(s) 'orders-in' failed", error.Message);
        var inner = Assert.IsType<AggregateException>(error.InnerException);
        Assert.Same(listingFailure, Assert.Single(inner.InnerExceptions));
    }

    [Fact]
    public async Task SweepAsync_WithSeveralFailingPorts_NamesEveryFailedPort()
    {
        var sweep = Sweep(["orders-in", "invoices-in"], port =>
            new RecordingPortSweep(port.Name, [], sweepFailure: new IOException($"{port.Name} unreachable")));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => sweep.SweepAsync(CancellationToken.None));

        Assert.Equal("Component 'test-component': sweeping source port(s) 'orders-in', 'invoices-in' failed",
            error.Message);
        Assert.Equal(2, Assert.IsType<AggregateException>(error.InnerException).InnerExceptions.Count);
    }

    [Fact]
    public async Task SweepAsync_WithSeveralPorts_PropagatesAFailureWhileTheHostCancels()
    {
        // Host cancellation ends the run; it is not a port failure to isolate.
        using var cts = new CancellationTokenSource();
        var calls = new List<string>();
        var sweep = Sweep(["orders-in", "invoices-in"], port => port.Name == "orders-in"
            ? new RecordingPortSweep(port.Name, calls, sweepFailure: new OperationCanceledException(),
                onSweep: _ => cts.Cancel())
            : new RecordingPortSweep(port.Name, calls));

        await Assert.ThrowsAsync<OperationCanceledException>(() => sweep.SweepAsync(cts.Token));

        Assert.DoesNotContain("sweep invoices-in", calls);
    }

    [Fact]
    public async Task SweepAsync_PassesTheHostTokenToEachPort()
    {
        using var cts = new CancellationTokenSource();
        var seen = new List<CancellationToken>();
        var sweep = Sweep(["orders-in", "invoices-in"], port => new RecordingPortSweep(port.Name, [], onSweep: seen.Add));

        await sweep.SweepAsync(cts.Token);

        Assert.Equal([cts.Token, cts.Token], seen);
    }

    private static SourceSweep Sweep(string[] ports, Func<SourcePort, IPortSweep> createSweep)
    {
        var services = new ServiceCollection();
        foreach (var port in ports)
            services.AddSingleton(new SourcePort(port, FileCompletion.Delete));
        return new SourceSweep(services.BuildServiceProvider(), Identity, createSweep, NullLoggerFactory.Instance);
    }

    private sealed class RecordingPortSweep(
        string port,
        List<string> calls,
        bool failValidation = false,
        RunSummary? summary = null,
        Exception? sweepFailure = null,
        Action<CancellationToken>? onSweep = null) : IPortSweep
    {
        public string Port => port;

        public Task ValidateAsync()
        {
            calls.Add($"validate {port}");
            return failValidation
                ? Task.FromException(new InvalidOperationException($"'{port}' cannot be swept"))
                : Task.CompletedTask;
        }

        public Task<RunSummary> SweepAsync(CancellationToken ct)
        {
            calls.Add($"sweep {port}");
            onSweep?.Invoke(ct);
            return sweepFailure is null
                ? Task.FromResult(summary ?? RunSummary.Empty)
                : Task.FromException<RunSummary>(sweepFailure);
        }
    }
}
