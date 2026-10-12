using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Test.FileSweeps;

/// <summary>
/// The run's sweep over a component's source ports: no port fails the run before anything is
/// swept, every port is validated before any port is swept, and the ports' summaries are added
/// together in registration order.
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
    public async Task SweepAsync_WhenAPortsSweepThrows_PropagatesTheFailure()
    {
        // A listing failure is never reported as an empty successful sweep.
        var calls = new List<string>();
        var sweep = Sweep(["orders-in"], port => new RecordingPortSweep(port.Name, calls,
            sweepFailure: new IOException("SFTP unreachable")));

        var error = await Assert.ThrowsAsync<IOException>(() => sweep.SweepAsync(CancellationToken.None));

        Assert.Equal("SFTP unreachable", error.Message);
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
        return new SourceSweep(services.BuildServiceProvider(), Identity, createSweep);
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
