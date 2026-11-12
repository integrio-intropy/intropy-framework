using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Sweeps;
using Microsoft.Extensions.Configuration;
using Intropy.Framework.Hosting.Test.Sweeps;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Test.Extractor;

/// <summary>
/// The configuration-based extractor registration: the section's <c>SourcePort</c> key fills what
/// the definition's delegate leaves unset, and the delegate wins over configuration where both
/// speak. The <see cref="Sweeps.SourcePort"/> the registration declares is the observable.
/// </summary>
public class ExtractorConfigurationTests
{
    [Fact]
    public void Configuration_FillsTheSourcePortTheDelegateLeavesUnset()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Extractor:SourcePort"] = "orders-inbox",
        }).Build();

        services.AddExtractor<int, int, Context>(configuration.GetSection("Extractor"),
            () => new ExtractorDefinition<int, int, Context> { Pipeline = (_, _) => null! });

        var provider = services.BuildServiceProvider();

        Assert.Equal(new FileSourcePort("orders-inbox", FileCompletion.Delete),
            provider.GetRequiredService<SourcePort>());
    }

    [Fact]
    public void Code_WinsOverConfiguration()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Extractor:SourcePort"] = "orders-inbox",
        }).Build();

        services.AddExtractor<int, int, Context>(configuration.GetSection("Extractor"),
            () => new ExtractorDefinition<int, int, Context>
            {
                SourcePort = "code-inbox",
                Pipeline = (_, _) => null!,
            });

        var provider = services.BuildServiceProvider();

        Assert.Equal(new FileSourcePort("code-inbox", FileCompletion.Delete),
            provider.GetRequiredService<SourcePort>());
    }


    [Fact]
    public void ASourceOfAnyKindInCode_WinsOverTheConfiguredPortName()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Extractor:SourcePort"] = "orders-inbox",
        }).Build();
        var feed = new ListedSourcePort("feed", []);

        services.AddExtractor<int, int, Context>(configuration.GetSection("Extractor"),
            () => new ExtractorDefinition<int, int, Context> { Source = feed, Pipeline = (_, _) => null! });

        var provider = services.BuildServiceProvider();

        Assert.Same(feed, Assert.Single(provider.GetServices<SourcePort>()));
    }

    [Fact]
    public void ADefinitionWithoutASourcePort_RegistersNone_EvenWithASection()
    {
        // The source port is optional: a component registering its own adapter leaves the key unset
        // and the section empty of it, and no SourcePort is declared.
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Extractor:Unrelated"] = "ignored",
        }).Build();

        services.AddExtractor<int, int, Context>(configuration.GetSection("Extractor"),
            () => new ExtractorDefinition<int, int, Context> { Pipeline = (_, _) => null! });

        var provider = services.BuildServiceProvider();

        Assert.Null(provider.GetService<SourcePort>());
    }
}
