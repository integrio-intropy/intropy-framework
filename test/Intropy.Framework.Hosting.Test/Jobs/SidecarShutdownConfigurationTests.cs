using Dapr.Client;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.Test.Loader;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Test.Jobs;

/// <summary>INTROPY_JOB_SIDECAR_SHUTDOWN is process-wide, so these run one at a time.</summary>
[Collection(ProcessStateCollection.Name)]
public class SidecarShutdownConfigurationTests
{
    private const string Variable = "INTROPY_JOB_SIDECAR_SHUTDOWN";

    [Theory]
    [InlineData(null, null)]
    [InlineData("", null)]
    [InlineData("  ", null)]
    [InlineData("OnSuccess", SidecarShutdown.OnSuccess)]
    [InlineData(" onsuccess ", SidecarShutdown.OnSuccess)]
    [InlineData("always", SidecarShutdown.Always)]
    public void Parse_AcceptsTheMemberNames(string? value, SidecarShutdown? expected)
    {
        Assert.Equal(expected, JobOptions.ParseSidecarShutdown(value));
    }

    [Theory]
    [InlineData("1")]
    [InlineData("OnFailure")]
    [InlineData("Always,OnSuccess")]
    public void Parse_RejectsAnythingElse_NamingTheVariable(string value)
    {
        var e = Assert.Throws<InvalidOperationException>(() => JobOptions.ParseSidecarShutdown(value));
        Assert.Contains(Variable, e.Message);
    }

    [Fact]
    public void AddJob_TakesTheDeploymentsSetting()
    {
        var options = Resolve("OnSuccess");

        Assert.Equal(SidecarShutdown.OnSuccess, options.SidecarShutdown);
    }

    [Fact]
    public void AddJob_DefaultsToAlways_WithoutTheVariable()
    {
        var options = Resolve(null);

        Assert.Equal(SidecarShutdown.Always, options.SidecarShutdown);
    }

    [Fact]
    public void AddJob_CodeWinsOverTheVariable()
    {
        var options = Resolve("OnSuccess", o => o.SidecarShutdown = SidecarShutdown.Always);

        Assert.Equal(SidecarShutdown.Always, options.SidecarShutdown);
    }

    [Fact]
    public void AddJob_FailsOnAnInvalidValue()
    {
        Assert.Throws<InvalidOperationException>(() => Resolve("sometimes"));
    }

    private static JobOptions Resolve(string? value, Action<JobOptions>? configure = null)
    {
        Environment.SetEnvironmentVariable(Variable, value);
        try
        {
            var services = new ServiceCollection();
            services.AddSingleton(_ => new DaprClientBuilder().Build());
            services.AddJob<ServiceCollectionExtensionsTests.TestJob>(o =>
            {
                o.JobName = "test-job";
                configure?.Invoke(o);
            });
            return services.BuildServiceProvider().GetRequiredService<JobOptions>();
        }
        finally
        {
            Environment.SetEnvironmentVariable(Variable, null);
        }
    }
}
