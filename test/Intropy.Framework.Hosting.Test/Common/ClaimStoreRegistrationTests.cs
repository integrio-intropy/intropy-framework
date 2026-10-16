using Dapr.Client;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.Common;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.Common;

/// <summary>
/// The component's claim store: its adapter configured from <c>ClaimStore</c> and registered keyed, so
/// the receive side's claim check and the send side's resolution both find it. Pinned here rather than
/// left to the one component that calls it — the section name is a literal a component writes in its
/// configuration, and renaming it would break every deployment silently.
/// </summary>
public class ClaimStoreRegistrationTests
{
    private static readonly IConfiguration Configuration = new ConfigurationBuilder().AddInMemoryCollection(
        new Dictionary<string, string?>
        {
            ["ClaimStore:Kind"] = "Local",
            ["ClaimStore:DaprBindingName"] = "claim-store"
        }).Build();

    [Fact]
    public void AddClaimStore_ReadsTheClaimStoreSection_AndRegistersItsAdapterKeyed()
    {
        var services = new ServiceCollection().AddSingleton(Substitute.For<DaprClient>());

        services.AddClaimStore(Configuration);

        // The same keyed lookup SendRoute and AddTransactionalIntegrationCore perform. They share this
        // one constant, so the key itself cannot drift; what this pins is the section being read and the
        // adapter arriving as a keyed registration of the configured kind.
        using var provider = services.BuildServiceProvider();
        Assert.IsType<LocalFileAdapter>(
            provider.GetKeyedService<IFileAdapter>(ClaimStoreServiceCollectionExtensions.ClaimStoreKey));
    }

    [Fact]
    public void AClaimStoreWithoutConfiguration_FailsNamingTheSection()
    {
        var error = Assert.Throws<InvalidOperationException>(() =>
            new ServiceCollection().AddClaimStore(new ConfigurationBuilder().Build()));

        Assert.Contains(ClaimStoreServiceCollectionExtensions.ClaimStoreSectionName, error.Message,
            StringComparison.Ordinal);
    }
}
