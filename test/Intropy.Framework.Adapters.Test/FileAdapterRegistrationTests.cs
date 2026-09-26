using System.Text;
using System.Text.RegularExpressions;
using Dapr.Client;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Testing.Adapters;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NSubstitute;

namespace Intropy.Framework.Adapters.Test;

public class FileAdapterRegistrationTests
{
    [Theory]
    [InlineData(FileAdapterKind.Local, typeof(LocalFileAdapter))]
    [InlineData(FileAdapterKind.Sftp, typeof(SftpAdapter))]
    [InlineData(FileAdapterKind.AzureBlob, typeof(AzureBlobStorageAdapter))]
    public async Task KindSelectsAdapter_WithoutChangingBinding_AndResolutionIsLazy(FileAdapterKind kind, Type expected)
    {
        var services = new ServiceCollection();
        var created = 0;
        using var client = Client();
        services.AddSingleton<DaprClient>(_ => { created++; return client; });
        services.AddFileAdapter("source", Options(kind));
        Assert.Equal(0, created);
        using var provider = services.BuildServiceProvider();
        var adapter = provider.GetRequiredKeyedService<IFileAdapter>("source");
        Assert.IsType(expected, adapter);
        Assert.Same(adapter, provider.GetRequiredKeyedService<IFileAdapter>("source"));
        Assert.Equal(1, created);
        Assert.Empty(client.ReceivedCalls());
        Assert.Null(provider.GetKeyedService<IFileAdapter>("unknown"));
        Assert.Null(provider.GetService<IFileAdapter>());
        await adapter.GetContentAsync("order.json");
        await client.Received(1).InvokeBindingAsync(Arg.Is<BindingRequest>(r => r.BindingName == "orders-source"), Arg.Any<CancellationToken>());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task KeysHaveIndependentSnapshots_RegardlessOfOrder(bool reverse)
    {
        var configuration = Configuration();
        using var client = Client();
        var services = new ServiceCollection().AddSingleton(client);
        var keys = reverse ? new[] { "destination", "source" } : new[] { "source", "destination" };
        foreach (var key in keys)
            services.AddFileAdapter(key, configuration.GetSection($"FileTransports:{key}"));
        configuration["FileTransports:source:Kind"] = "Local";
        configuration["FileTransports:source:DaprBindingName"] = "changed";
        using var provider = services.BuildServiceProvider();
        var source = provider.GetRequiredKeyedService<IFileAdapter>("source");
        var destination = provider.GetRequiredKeyedService<IFileAdapter>("destination");
        Assert.IsType<SftpAdapter>(source);
        Assert.IsType<LocalFileAdapter>(destination);
        await source.GetContentAsync("order.json");
        await destination.GetContentAsync("order.json");
        await client.Received().InvokeBindingAsync(Arg.Is<BindingRequest>(r => r.BindingName == "orders-source" && r.Metadata["fileName"] == "/inbound/order.json"), Arg.Any<CancellationToken>());
        await client.Received().InvokeBindingAsync(Arg.Is<BindingRequest>(r => r.BindingName == "orders-destination" && r.Metadata["fileName"] == "outbound/order.json"), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task OmittedBindingName_IsThePortNameUsedAsKey()
    {
        using var client = Client();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["FileTransports:orders-source:Kind"] = "Sftp"
        }).Build();
        var services = new ServiceCollection().AddSingleton(client);
        services.AddFileAdapter("orders-source", configuration.GetSection("FileTransports:orders-source"));
        services.AddFileAdapter("orders-destination", new FileTransportOptions { Kind = FileAdapterKind.Local });
        using var provider = services.BuildServiceProvider();

        await provider.GetRequiredKeyedService<IFileAdapter>("orders-source").GetContentAsync("order.json");
        await provider.GetRequiredKeyedService<IFileAdapter>("orders-destination").GetContentAsync("order.json");

        await client.Received(1).InvokeBindingAsync(Arg.Is<BindingRequest>(r => r.BindingName == "orders-source"), Arg.Any<CancellationToken>());
        await client.Received(1).InvokeBindingAsync(Arg.Is<BindingRequest>(r => r.BindingName == "orders-destination"), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HostRootPath_IsAcceptedAndDoesNotChangeTheAdapter()
    {
        // Under the system host a port's section also carries the folder the host resolved for it.
        using var client = Client();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Ports:orders-source:Kind"] = "Local",
            ["Ports:orders-source:RootPath"] = "/workspace/host/test/orders-source"
        }).Build();
        var services = new ServiceCollection().AddSingleton(client);
        services.AddFileAdapter("orders-source", configuration.GetSection("Ports:orders-source"));
        using var provider = services.BuildServiceProvider();

        await provider.GetRequiredKeyedService<IFileAdapter>("orders-source").GetContentAsync("order.json");

        await client.Received(1).InvokeBindingAsync(Arg.Is<BindingRequest>(r =>
            r.BindingName == "orders-source" && !r.Metadata["fileName"].Contains("workspace")), Arg.Any<CancellationToken>());
    }

    [Theory]
    [InlineData("Kind", null)]
    [InlineData("Kind", "")]
    [InlineData("Kind", "unknown-secret")]
    [InlineData("Kind", "99")]
    [InlineData("Kind", "File,Sftp")]
    [InlineData("Kind", "File")]
    [InlineData("Kind", "Blob")]
    [InlineData("DaprBindingName", " ")]
    [InlineData("DaprBindngName", "secret")]
    [InlineData("FileNameRegex", "[secret")]
    [InlineData("BasePath:child", "secret")]
    public void BadConfiguration_FailsAtRegistrationWithoutLeakingValues(string setting, string? value)
    {
        var configuration = Configuration();
        configuration[$"FileTransports:source:{setting}"] = value;
        var services = new ServiceCollection();
        var error = Assert.Throws<InvalidOperationException>(() => services.AddFileAdapter("source", configuration.GetSection("FileTransports:source")));
        Assert.Contains("FileTransports:source", error.Message);
        Assert.DoesNotContain("secret", error.ToString());
        Assert.Empty(services);
    }

    [Fact]
    public void MissingSectionsKindsAndDuplicateKeys_FailImmediately()
    {
        var services = new ServiceCollection();
        Assert.Throws<InvalidOperationException>(() => services.AddFileAdapter("missing", Configuration().GetSection("missing")));
        Assert.Throws<InvalidOperationException>(() => services.AddFileAdapter("source", new FileTransportOptions { DaprBindingName = "source" }));
        Assert.Throws<InvalidOperationException>(() => services.AddFileAdapter("source", Options((FileAdapterKind)42)));
        services.AddFileAdapter("source", Options(FileAdapterKind.Local));
        Assert.Contains("source", Assert.Throws<InvalidOperationException>(() => services.AddFileAdapter("source", Options(FileAdapterKind.Sftp))).Message);
    }

    [Fact]
    public void MissingDapr_HasKeyAndClientDiagnostic()
    {
        using var provider = new ServiceCollection().AddFileAdapter("source", Options(FileAdapterKind.Local)).BuildServiceProvider();
        var error = Assert.Throws<InvalidOperationException>(() => provider.GetRequiredKeyedService<IFileAdapter>("source"));
        Assert.Contains("source", error.Message);
        Assert.Contains("DaprClient", error.Message);
    }

    [Fact]
    public void NormalKeyedReplacement_LeavesDestinationUntouched()
    {
        using var client = Client();
        var services = new ServiceCollection().AddSingleton(client);
        services.AddFileAdapter("source", Options(FileAdapterKind.Sftp));
        services.AddFileAdapter("destination", Options(FileAdapterKind.Local));
        services.RemoveAllKeyed<IFileAdapter>("source");
        var fake = new InMemoryFileAdapter();
        services.AddKeyedSingleton<IFileAdapter>("source", fake);
        using var provider = services.BuildServiceProvider();
        Assert.Same(fake, provider.GetRequiredKeyedService<IFileAdapter>("source"));
        Assert.IsType<LocalFileAdapter>(provider.GetRequiredKeyedService<IFileAdapter>("destination"));
        Assert.Empty(client.ReceivedCalls());
    }

    [Theory]
    [InlineData(FileAdapterKind.Local)]
    [InlineData(FileAdapterKind.Sftp)]
    public async Task ConfiguredAndManualAdapters_EmitIdenticalRequests(FileAdapterKind kind)
    {
        using var client = Client();
        var options = Options(kind);
        options.FileNameRegex = @"\.json$";
        var services = new ServiceCollection().AddSingleton(client).AddFileAdapter("source", options);
        options.BasePath = "changed";
        options.DaprBindingName = "changed";
        options.Kind = FileAdapterKind.Local;
        using var provider = services.BuildServiceProvider();
        var configured = provider.GetRequiredKeyedService<IFileAdapter>("source");
        var manual = FileAdapterRequestTests.Create(kind == FileAdapterKind.Local ? "Local" : "Sftp", client, new FileAdapterOptions("orders-source", "/inbound",
            new Regex(@"\.json$", RegexOptions.None, TimeSpan.FromSeconds(1))));
        foreach (var adapter in new[] { manual, configured })
        {
            await adapter.ListAsync();
            await adapter.GetContentAsync("order.json");
            await adapter.GetContentAsync("order.json", Encoding.UTF8);
            await adapter.WriteAsync("order.json", "åö", Encoding.Unicode);
            await adapter.WriteAsync("order.json", new byte[] { 0, 1, 255 }, "/override");
            await adapter.DeleteAsync("order.json");
        }
        var requests = client.ReceivedCalls().Select(c => (BindingRequest)c.GetArguments()[0]!).ToArray();
        for (var i = 0; i < 6; i++)
        {
            Assert.Equal(requests[i].BindingName, requests[i + 6].BindingName);
            Assert.Equal(requests[i].Operation, requests[i + 6].Operation);
            Assert.Equal(requests[i].Metadata, requests[i + 6].Metadata);
            Assert.Equal(requests[i].Data.ToArray(), requests[i + 6].Data.ToArray());
        }
    }

    [Fact]
    public async Task ConfiguredRegex_HasFiniteTimeout()
    {
        using var client = Client();
        client.InvokeBindingAsync(Arg.Any<BindingRequest>(), Arg.Any<CancellationToken>())
            .Returns(c => new BindingResponse(c.Arg<BindingRequest>(), Encoding.UTF8.GetBytes("[\"" + new string('a', 10000) + "!\"]"), new Dictionary<string, string>()));
        var options = Options(FileAdapterKind.Local);
        options.FileNameRegex = "^(a+)+$";
        using var provider = new ServiceCollection().AddSingleton(client).AddFileAdapter("source", options).BuildServiceProvider();
        var exception = await Assert.ThrowsAsync<RegexMatchTimeoutException>(() => provider.GetRequiredKeyedService<IFileAdapter>("source").ListAsync());
        Assert.Equal(TimeSpan.FromSeconds(1), exception.MatchTimeout);
    }

    [Fact]
    public async Task ConfiguredAzureBlobKind_EndToEnd_ResolvesTheBlobAdapter()
    {
        using var client = Client();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Ports:orders-source:Kind"] = "AzureBlob",
            ["Ports:orders-source:BasePath"] = "/inbound"
        }).Build();
        var services = new ServiceCollection().AddSingleton(client);
        services.AddFileAdapter("orders-source", configuration.GetSection("Ports:orders-source"));
        using var provider = services.BuildServiceProvider();

        var adapter = provider.GetRequiredKeyedService<IFileAdapter>("orders-source");
        Assert.IsType<AzureBlobStorageAdapter>(adapter);
        await adapter.GetContentAsync("order.json");
        await client.Received(1).InvokeBindingAsync(Arg.Is<BindingRequest>(r =>
            r.BindingName == "orders-source" && r.Metadata["blobName"] == "inbound/order.json"), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FactoryOverload_RegistersLazily_ResolvesOnceInsideTheProvider()
    {
        using var client = Client();
        var created = 0;
        var services = new ServiceCollection().AddSingleton(client)
            .AddFileAdapter("source", p =>
            {
                created++;
                // The factory runs inside the provider: its dependency resolution works.
                return p.GetRequiredService<DaprClient>() == client
                    ? new InMemoryFileAdapter().AddFile("order.json", "{}")
                    : throw new InvalidOperationException("not-called");
            });
        Assert.Equal(0, created);
        using var provider = services.BuildServiceProvider();

        var adapter = provider.GetRequiredKeyedService<IFileAdapter>("source");
        Assert.Same(adapter, provider.GetRequiredKeyedService<IFileAdapter>("source"));
        Assert.Equal(1, created);
        Assert.Equal("{}", await adapter.GetContentAsync("order.json", Encoding.UTF8));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void FactoryOverload_AndOptionsOverload_RejectDuplicateKeys_RegardlessOfOrder(bool factoryFirst)
    {
        var services = new ServiceCollection();
        if (factoryFirst)
        {
            services.AddFileAdapter("source", _ => new InMemoryFileAdapter());
            Assert.Contains("source", Assert.Throws<InvalidOperationException>(
                () => services.AddFileAdapter("source", Options(FileAdapterKind.Local))).Message);
        }
        else
        {
            services.AddFileAdapter("source", Options(FileAdapterKind.Local));
            Assert.Contains("source", Assert.Throws<InvalidOperationException>(
                () => services.AddFileAdapter("source", _ => new InMemoryFileAdapter())).Message);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void FactoryOverload_AndSectionOverload_RejectDuplicateKeys_RegardlessOfOrder(bool factoryFirst)
    {
        var services = new ServiceCollection();
        if (factoryFirst)
        {
            services.AddFileAdapter("source", _ => new InMemoryFileAdapter());
            Assert.Contains("source", Assert.Throws<InvalidOperationException>(
                () => services.AddFileAdapter("source", Configuration().GetSection("FileTransports:source"))).Message);
        }
        else
        {
            services.AddFileAdapter("source", Configuration().GetSection("FileTransports:source"));
            Assert.Contains("source", Assert.Throws<InvalidOperationException>(
                () => services.AddFileAdapter("source", _ => new InMemoryFileAdapter())).Message);
        }
    }

    private static FileTransportOptions Options(FileAdapterKind kind) => new() { Kind = kind, DaprBindingName = "orders-source", BasePath = "/inbound" };

    private static DaprClient Client()
    {
        var client = Substitute.For<DaprClient>();
        client.InvokeBindingAsync(Arg.Any<BindingRequest>(), Arg.Any<CancellationToken>())
            .Returns(c => new BindingResponse(c.Arg<BindingRequest>(), "[]"u8.ToArray(), new Dictionary<string, string>()));
        return client;
    }

    private static IConfigurationRoot Configuration() => new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["FileTransports:source:Kind"] = "Sftp",
        ["FileTransports:source:DaprBindingName"] = "orders-source",
        ["FileTransports:source:BasePath"] = "/inbound",
        ["FileTransports:destination:Kind"] = "Local",
        ["FileTransports:destination:DaprBindingName"] = "orders-destination",
        ["FileTransports:destination:BasePath"] = "outbound"
    }).Build();
}
