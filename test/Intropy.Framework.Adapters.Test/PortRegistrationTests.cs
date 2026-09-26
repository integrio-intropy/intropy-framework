using Dapr.Client;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Testing.Adapters;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace Intropy.Framework.Adapters.Test;

/// <summary>
/// A component's destination ports: each one's adapter keyed by the port name and configured from
/// <c>Ports:&lt;port&gt;</c>. The source port (<c>AddSourcePort</c>) is tested in
/// <c>Intropy.Framework.Hosting.Test</c>.
/// </summary>
public class PortRegistrationTests
{
    private static readonly IConfiguration Configuration = new ConfigurationBuilder().AddInMemoryCollection(
        new Dictionary<string, string?>
        {
            ["Ports:orders-source:Kind"] = "Sftp",
            ["Ports:orders-destination:Kind"] = "Local",
            ["Ports:audit-destination:Kind"] = "Local"
        }).Build();

    [Fact]
    public void AddDestinationPort_RegistersEachPortsAdapter()
    {
        var services = new ServiceCollection().AddSingleton(Substitute.For<DaprClient>());

        services.AddDestinationPort("orders-destination", Configuration);
        services.AddDestinationPort("audit-destination", Configuration);

        using var provider = services.BuildServiceProvider();
        Assert.IsType<LocalFileAdapter>(provider.GetRequiredKeyedService<IFileAdapter>("orders-destination"));
        Assert.IsType<LocalFileAdapter>(provider.GetRequiredKeyedService<IFileAdapter>("audit-destination"));
    }

    [Fact]
    public void APortWithoutConfiguration_FailsNamingThePort()
    {
        var error = Assert.Throws<InvalidOperationException>(() =>
            new ServiceCollection().AddDestinationPort("unknown-destination", Configuration));

        Assert.Contains("Ports:unknown-destination", error.Message);
    }
}
