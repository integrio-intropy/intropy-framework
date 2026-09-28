using Dapr.Client;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.FileSweeps;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.FileSweeps;

/// <summary>
/// A component's source port: its adapter keyed by the port name and configured from
/// <c>Ports:&lt;port&gt;</c>, the port declared as the component's one
/// <see cref="SourcePort"/>, and a second source port rejected without changing services.
/// </summary>
public class SourcePortRegistrationTests
{
    private static readonly IConfiguration Configuration = new ConfigurationBuilder().AddInMemoryCollection(
        new Dictionary<string, string?>
        {
            ["Ports:orders-source:Kind"] = "Sftp",
            ["Ports:orders-destination:Kind"] = "Local",
            ["Ports:audit-destination:Kind"] = "Local"
        }).Build();

    [Fact]
    public void AddSourcePort_RegistersTheAdapterFromItsPortSectionAndDeclaresTheSource()
    {
        var archive = FileCompletion.Archive("archive");
        var services = new ServiceCollection().AddSingleton(Substitute.For<DaprClient>());

        services.AddSourcePort("orders-source", Configuration, archive);

        using var provider = services.BuildServiceProvider();
        Assert.IsType<SftpAdapter>(provider.GetRequiredKeyedService<IFileAdapter>("orders-source"));
        Assert.Equal(new SourcePort("orders-source", archive), provider.GetRequiredService<SourcePort>());
    }

    [Fact]
    public void AddSourcePort_DefaultsToDeletingHandledFiles()
    {
        using var provider = new ServiceCollection().AddSourcePort("orders-source").BuildServiceProvider();

        Assert.Same(FileCompletion.Delete, provider.GetRequiredService<SourcePort>().Completion);
    }

    [Fact]
    public void ASecondSourcePort_IsRejectedWithoutChangingServices()
    {
        var services = new ServiceCollection().AddSourcePort("orders-source", Configuration);
        var count = services.Count;

        var error = Assert.Throws<InvalidOperationException>(() => services.AddSourcePort("audit-destination", Configuration));

        Assert.Contains("already registered", error.Message);
        Assert.Equal(count, services.Count);
    }
}
