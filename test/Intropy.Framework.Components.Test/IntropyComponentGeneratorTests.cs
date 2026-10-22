using System.Collections.Immutable;
using Intropy.Framework.Components.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Intropy.Framework.Components.Test;

/// <summary>
/// The generator's checks and the record it writes, exercised with a GeneratorDriver on source
/// compiled against the components themselves — so the compilation sees the same
/// <see cref="Intropy.Framework.Components.IntropyComponent"/> base class the framework ships.
/// </summary>
public sealed class IntropyComponentGeneratorTests : IDisposable
{
    private readonly string _rootDirectory;
    private bool _layoutDeclared;
    private bool _cliLayout;
    private readonly Dictionary<string, string> _properties = [];

    public IntropyComponentGeneratorTests() => _rootDirectory = Path.Combine(
        Path.GetTempPath(), "intropy-generator-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_rootDirectory))
            Directory.Delete(_rootDirectory, recursive: true);
    }

    /// <summary>The flat layout: the component root is the project directory — the csproj and
    /// the scaffold record share it.</summary>
    private void UseFlatLayout() => _layoutDeclared = true;

    /// <summary>The CLI layout: <c>intropy int create</c> scaffolds the project into <c>src/</c>
    /// under the component root and keeps the record at the root, next to the Dockerfile — the
    /// project compiles one level down from the record.</summary>
    private void UseCliLayout()
    {
        _layoutDeclared = true;
        _cliLayout = true;
    }

    /// <summary>The directory MSBuild compiles the project from.</summary>
    private string ProjectDirectory =>
        _cliLayout ? Path.Combine(_rootDirectory, "src") : _rootDirectory;

    private string ProjectRecordPath => Path.Combine(ProjectDirectory, ".intropy", "scaffold.json");

    private string RootRecordPath => Path.Combine(_rootDirectory, ".intropy", "scaffold.json");

    [Fact]
    public void Generator_WritesTheRecordFromTheAttribute()
    {
        _properties["build_property.IntropyGenerateScaffold"] = "true";
        UseFlatLayout();

        Run([CompletedGateway]);

        Assert.Equal(GeneratedRecord, ReadScaffold().TrimEnd('\n'));
    }

    [Fact]
    public void Generator_WithOptionalsUnset_OmitsTheRecordValuesTheyName()
    {
        _properties["build_property.IntropyGenerateScaffold"] = "true";
        UseFlatLayout();

        Run([WithoutEndpoint]);

        var record = ReadScaffold();
        Assert.DoesNotContain("\"port\"", record, StringComparison.Ordinal);
        Assert.DoesNotContain("\"endpointPath\"", record, StringComparison.Ordinal);
        Assert.Equal("gateway", BlockKind(record));
    }

    [Fact]
    public void Generator_WithRecordingTurnedOff_WritesNothing()
    {
        _properties["build_property.IntropyGenerateScaffold"] = "false";
        UseFlatLayout();

        Run([CompletedGateway]);

        Assert.False(File.Exists(ProjectRecordPath));
    }

    [Fact]
    public void Generator_MergesTheIdentity_IntoARecordCarryingTheCliSProvenance()
    {
        _properties["build_property.IntropyGenerateScaffold"] = "true";
        UseFlatLayout();
        WriteScaffold(CliScaffoldedRecord);

        Run([CompletedGateway]);

        // The identity's owners replaced; the CLI's appId, channel and render echoes, its
        // provenance and an unknown future field preserved verbatim, in the order the CLI
        // wrote them — within values as much as at the top level.
        Assert.Equal(MergedWithProvenance, ReadScaffold().TrimEnd('\n'));
    }

    [Fact]
    public void Generator_WithTheAttributeMissingOptionals_DropsTheOwnedKeysTheExistingValuesCarries()
    {
        // Owning the identity keys includes owning their absence: an attribute without
        // Port/EndpointPath removes those values keys even when the record still has them —
        // and still preserves the CLI's own appId.
        _properties["build_property.IntropyGenerateScaffold"] = "true";
        UseFlatLayout();
        WriteScaffold(CliScaffoldedRecordWithStaleOptionals);

        Run([WithoutEndpoint]);

        Assert.Equal(MergedWithoutOptionals, ReadScaffold().TrimEnd('\n'));
    }

    [Fact]
    public void Generator_Merges_KeepingThePreservedValuesKeysTheirOriginalOrder()
    {
        // However the CLI scrambled its own values keys, they come back in that same relative
        // order, behind the canonical identity keys.
        _properties["build_property.IntropyGenerateScaffold"] = "true";
        UseFlatLayout();
        WriteScaffold(CliScaffoldedRecordWithScrambledValues);

        Run([CompletedGateway]);

        var record = ReadScaffold();
        var identity = record.IndexOf("\"componentName\": \"order-gateway\"", StringComparison.Ordinal);
        Assert.True(identity > -1);
        var contracts = record.IndexOf("\"contracts\": \"src/contracts\"", StringComparison.Ordinal);
        var appId = record.IndexOf("\"appId\": \"order-gateway.app\"", StringComparison.Ordinal);
        var channel = record.IndexOf("\"channel\": \"orders-topic\"", StringComparison.Ordinal);
        Assert.True(contracts < appId);
        Assert.True(appId < channel);
        Assert.True(identity < contracts);
        Assert.Equal(MergedWithScrambledValues, record.TrimEnd('\n'));
    }

    [Fact]
    public void Generator_WithARecordWhoseValuesIsAbsent_WritesTheIdentityValues_AndKeepsTheOtherFields()
    {
        _properties["build_property.IntropyGenerateScaffold"] = "true";
        UseFlatLayout();
        WriteScaffold(RecordWithoutValues);

        var diagnostics = Run([CompletedGateway]);

        Assert.DoesNotContain(diagnostics, d => d.Id == "INTROPY1006");
        Assert.Equal(GeneratedRecordWithProvenance, ReadScaffold().TrimEnd('\n'));
    }

    [Fact]
    public void Generator_WithARecordWhoseValuesIsNotAnObject_WritesTheIdentityValues_AndKeepsTheOtherFields()
    {
        _properties["build_property.IntropyGenerateScaffold"] = "true";
        UseFlatLayout();
        WriteScaffold(RecordWithNonObjectValues);

        var diagnostics = Run([CompletedGateway]);

        Assert.DoesNotContain(diagnostics, d => d.Id == "INTROPY1006");
        Assert.Equal(GeneratedRecordWithProvenance, ReadScaffold().TrimEnd('\n'));
    }

    [Fact]
    public void Generator_Merges_AroundAnUnknownExtraField()
    {
        _properties["build_property.IntropyGenerateScaffold"] = "true";
        UseFlatLayout();
        WriteScaffold(
            """
            {
              "schemaVersion": 1,
              "future": { "tag": "keep-me" },
              "count": 42,
              "unset": null,
              "values": { "componentName": "stale-name" },
              "blockKind": "component"
            }
            """);

        Run([CompletedGateway]);

        var record = ReadScaffold();
        Assert.Contains("\"future\": { \"tag\": \"keep-me\" }", record, StringComparison.Ordinal);
        Assert.Contains("\"count\": 42", record, StringComparison.Ordinal);
        Assert.Contains("\"unset\": null", record, StringComparison.Ordinal);
        Assert.DoesNotContain("stale-name", record, StringComparison.Ordinal);
        Assert.Contains("\"blockKind\": \"gateway\"", record, StringComparison.Ordinal);
    }

    [Fact]
    public void Generator_WithAMalformedRecord_RewritesIdentityOnly_AndNamesTheFile()
    {
        _properties["build_property.IntropyGenerateScaffold"] = "true";
        UseFlatLayout();
        WriteScaffold("{ not json at all ");

        var diagnostics = Run([CompletedGateway]);

        var warning = Assert.Single(diagnostics, d => d.Id == "INTROPY1006");
        Assert.Contains("scaffold.json", warning.GetMessage(), StringComparison.Ordinal);
        Assert.Contains("was rewritten", warning.GetMessage(), StringComparison.Ordinal);
        Assert.Equal(GeneratedRecord, ReadScaffold().TrimEnd('\n'));
    }

    [Fact]
    public void Generator_WithAnUnchangedIdentity_DoesNotTouchTheFile()
    {
        _properties["build_property.IntropyGenerateScaffold"] = "true";
        UseFlatLayout();
        Run([CompletedGateway]);
        var written = ReadScaffold();
        var touched = File.GetLastWriteTimeUtc(ProjectRecordPath);

        Run([CompletedGateway]);

        Assert.Equal(written, ReadScaffold());
        Assert.Equal(touched, File.GetLastWriteTimeUtc(ProjectRecordPath));
    }

    [Fact]
    public void Generator_WithARecordAtTheComponentRoot_MergesThere_AndWritesNothingUnderTheProject()
    {
        // The CLI layout: the project is scaffolded in src/ under the root, the CLI's record —
        // provenance included — lives at the root. The identity merges into that record; a
        // second, identity-only one must not appear under the project directory.
        _properties["build_property.IntropyGenerateScaffold"] = "true";
        UseCliLayout();
        WriteScaffold(CliScaffoldedRecord, directory: _rootDirectory);

        Run([CompletedGateway]);

        Assert.Equal(MergedWithProvenance, ReadScaffold(_rootDirectory).TrimEnd('\n'));
        Assert.False(File.Exists(ProjectRecordPath));
        Assert.False(Directory.Exists(Path.Combine(ProjectDirectory, ".intropy")));
    }

    [Fact]
    public void Generator_WithARecordAtTheProjectDirectory_MergesThere()
    {
        // A flat-layout component whose record sits next to the csproj: merged in place, and
        // the root above it stays untouched.
        _properties["build_property.IntropyGenerateScaffold"] = "true";
        UseCliLayout();
        WriteScaffold(CliScaffoldedRecord);

        Run([CompletedGateway]);

        Assert.Equal(MergedWithProvenance, ReadScaffold().TrimEnd('\n'));
        Assert.False(File.Exists(RootRecordPath));
    }

    [Fact]
    public void Generator_WithoutARecordAnywhere_CreatesItAtTheProjectDirectory()
    {
        _properties["build_property.IntropyGenerateScaffold"] = "true";
        UseCliLayout();

        Run([CompletedGateway]);

        Assert.Equal(GeneratedRecord, ReadScaffold().TrimEnd('\n'));
        Assert.False(File.Exists(RootRecordPath));
    }

    [Fact]
    public void Generator_WithRecordsInBothPlaces_TheProjectDirectoryRecordWins_AndTheParentIsUntouched()
    {
        // Pathological: a record next to the csproj and one at the root. The project-directory
        // record is the one merged; the parent's is left byte-for-byte as the CLI wrote it.
        _properties["build_property.IntropyGenerateScaffold"] = "true";
        UseCliLayout();
        const string parentRecord = "{\n  \"marker\": \"the-parent-record\"\n}";
        WriteScaffold(parentRecord, directory: _rootDirectory);
        WriteScaffold(CliScaffoldedRecord);

        Run([CompletedGateway]);

        Assert.Equal(MergedWithProvenance, ReadScaffold().TrimEnd('\n'));
        Assert.Equal(parentRecord, ReadScaffold(_rootDirectory));
    }

    [Fact]
    public void Generator_WithoutTheAttribute_NamesTheClass()
    {
        var diagnostics = Run([UnhandledGateway]);

        var error = Assert.Single(diagnostics, d => d.Id == "INTROPY1001");
        Assert.Contains("UnhandledGateway derives from IntropyComponent but carries no [IntropyComponent]",
            error.GetMessage(), StringComparison.Ordinal);
    }

    [Fact]
    public void Generator_WithAMissingMember_NamesTheMember()
    {
        var diagnostics = Run([MissingOrganization]);

        var error = Assert.Single(diagnostics, d => d.Id == "INTROPY1002");
        Assert.Contains("MissingOrganizationGateway: the IntropyComponent attribute sets no Organization",
            error.GetMessage(), StringComparison.Ordinal);
    }

    [Fact]
    public void Generator_WithARelativeEventSource_Fails()
    {
        var diagnostics = Run([RelativeEventSource]);

        var error = Assert.Single(diagnostics, d => d.Id == "INTROPY1003");
        Assert.Contains("EventSource 'orders' is not an absolute URI", error.GetMessage(), StringComparison.Ordinal);
    }

    [Fact]
    public void Generator_WithSeveralComponents_NamesThemAll()
    {
        _properties["build_property.IntropyGenerateScaffold"] = "true";
        UseFlatLayout();

        var diagnostics = Run([CompletedGateway, WithoutEndpoint]);

        var error = Assert.Single(diagnostics, d => d.Id == "INTROPY1004");
        Assert.Contains("several IntropyComponents (CompletedGateway, WithoutEndpointGateway)",
            error.GetMessage(), StringComparison.Ordinal);
    }

    private ImmutableArray<Diagnostic> Run(string[] sources)
    {
        if (_layoutDeclared)
            _properties["build_property.MSBuildProjectDirectory"] = ProjectDirectory;
        var references = AppDomain.CurrentDomain.GetAssemblies()
            .Where(assembly => !assembly.IsDynamic && !string.IsNullOrEmpty(assembly.Location))
            .Select(assembly => MetadataReference.CreateFromFile(assembly.Location))
            .Cast<MetadataReference>()
            .ToList();
        var compilation = CSharpCompilation.Create("components.generator.tests",
            sources.Select(source => CSharpSyntaxTree.ParseText(source)).ToArray(),
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        var driver = CSharpGeneratorDriver.Create(
            new ISourceGenerator[] { new IntropyComponentGenerator() },
            optionsProvider: new TestOptionsProvider(_properties.ToImmutableDictionary()));
        driver.RunGeneratorsAndUpdateCompilation(compilation, out _, out var diagnostics);
        return diagnostics;
    }

    private void WriteScaffold(string content, string? directory = null)
    {
        var path = Path.Combine(directory ?? ProjectDirectory, ".intropy", "scaffold.json");
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, content);
    }

    private string ReadScaffold(string? directory = null) =>
        File.ReadAllText(Path.Combine(directory ?? ProjectDirectory, ".intropy", "scaffold.json"));

    private static string BlockKind(string record)
    {
        using var document = System.Text.Json.JsonDocument.Parse(record);
        return document.RootElement.GetProperty("blockKind").GetString()!;
    }

    private sealed class TestOptionsProvider(ImmutableDictionary<string, string> global) : AnalyzerConfigOptionsProvider
    {
        public override AnalyzerConfigOptions GlobalOptions { get; } = new TestOptions(global);

        public override AnalyzerConfigOptions GetOptions(SyntaxTree tree) => GlobalOptions;

        public override AnalyzerConfigOptions GetOptions(AdditionalText textFile) => GlobalOptions;
    }

    private sealed class TestOptions(ImmutableDictionary<string, string> values) : AnalyzerConfigOptions
    {
        public override bool TryGetValue(string key, out string value)
        {
            if (values.TryGetValue(key, out var found))
            {
                value = found;
                return true;
            }
            value = string.Empty;
            return false;
        }
    }

    private const string Namespace = """
        using Intropy.Framework.Components;
        using Intropy.Framework.Components.Gateway;

        namespace Intropy.Framework.Components.Test.GeneratorExamples;

        public sealed record OrderPlaced(string OrderId);

        """;

    private const string CompletedGateway = Namespace + """
        [IntropyComponent("order-gateway", Organization = "Acme", Publishes = "order-placed",
            Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
            EventSource = "urn:Acme:order-gateway")]
        public sealed class CompletedGateway : GatewayComponent
        {
            protected override void Endpoints(GatewayEndpoints endpoints) { }
        }

        """;

    private const string WithoutEndpoint = Namespace + """
        [IntropyComponent("without-endpoint-gateway", Organization = "Acme", Publishes = "order-placed",
            Payload = typeof(OrderPlaced), EventSource = "urn:Acme:without-endpoint-gateway")]
        public sealed class WithoutEndpointGateway : GatewayComponent
        {
            protected override void Endpoints(GatewayEndpoints endpoints) { }
        }

        """;

    private const string UnhandledGateway = Namespace + """
        public sealed class UnhandledGateway : GatewayComponent
        {
            protected override void Endpoints(GatewayEndpoints endpoints) { }
        }

        """;

    private const string MissingOrganization = Namespace + """
        [IntropyComponent("missing-organization-gateway", Publishes = "order-placed",
            Payload = typeof(OrderPlaced), EventSource = "urn:Acme:missing-organization-gateway")]
        public sealed class MissingOrganizationGateway : GatewayComponent
        {
            protected override void Endpoints(GatewayEndpoints endpoints) { }
        }

        """;

    private const string RelativeEventSource = Namespace + """
        [IntropyComponent("relative-event-source-gateway", Organization = "Acme", Publishes = "order-placed",
            Payload = typeof(OrderPlaced), EventSource = "orders")]
        public sealed class RelativeEventSourceGateway : GatewayComponent
        {
            protected override void Endpoints(GatewayEndpoints endpoints) { }
        }

        """;

    /// <summary>The record the generator renders when no record exists yet, and what a malformed
    /// record is rewritten to: the attribute's identity alone.</summary>
    private const string GeneratedRecord = """
        {
          "schemaVersion": 1,
          "values": {
            "componentName": "order-gateway",
            "organization": "Acme",
            "publishes": "order-placed",
            "payloadType": "Intropy.Framework.Components.Test.GeneratorExamples.OrderPlaced",
            "port": "order-gateway-endpoint",
            "endpointPath": "/orders",
            "eventSourceValue": "urn:Acme:order-gateway"
          },
          "blockKind": "gateway"
        }
        """;

    /// <summary>The record <see cref="CliScaffoldedRecord"/> merges into: the identity's owners
    /// replaced, the CLI's appId, channel and render echoes kept behind them — its own keys kept
    /// in their original order, byte-for-byte — and its top-level provenance and the unknown
    /// future field preserved likewise.</summary>
    private const string MergedWithProvenance = """
        {
          "schemaVersion": 1,
          "values": {
            "componentName": "order-gateway",
            "organization": "Acme",
            "publishes": "order-placed",
            "payloadType": "Intropy.Framework.Components.Test.GeneratorExamples.OrderPlaced",
            "port": "order-gateway-endpoint",
            "endpointPath": "/orders",
            "eventSourceValue": "urn:Acme:order-gateway",
            "appId": "order-gateway.app",
            "channel": "orders-topic",
            "componentClass": "StaleGateway",
            "projectName": "StaleGateway",
            "contracts": "src/contracts"
          },
          "blockKind": "gateway",
          "template": "http-gateway",
          "owner": "Acme",
          "repo": "acme/integrations",
          "version": "1.2.3",
          "role": "gateway",
          "dataFlow": "inbound",
          "dependsOn": [
            {
              "name": "publisher",
              "type": "pubsub"
            }
          ],
          "future": null
        }
        """;

    /// <summary>The record as <see cref="CliScaffoldedRecord"/> scaffolded it, with the CLI's
    /// stale port and endpointPath still in its values.</summary>
    private const string CliScaffoldedRecordWithStaleOptionals = """
        {
          "schemaVersion": 1,
          "template": "http-gateway",
          "values": {
            "appId": "without-endpoint-gateway.app",
            "channel": "orders-topic",
            "componentName": "stale-name",
            "port": "order-gateway-endpoint",
            "endpointPath": "/orders",
            "organization": "Old"
          },
          "blockKind": "component"
        }
        """;

    /// <summary>What <see cref="CliScaffoldedRecordWithStaleOptionals"/> merges into when the
    /// attribute names no Port or EndpointPath: the owned keys drop out of values, the CLI's
    /// appId and channel stay.</summary>
    private const string MergedWithoutOptionals = """
        {
          "schemaVersion": 1,
          "values": {
            "componentName": "without-endpoint-gateway",
            "organization": "Acme",
            "publishes": "order-placed",
            "payloadType": "Intropy.Framework.Components.Test.GeneratorExamples.OrderPlaced",
            "eventSourceValue": "urn:Acme:without-endpoint-gateway",
            "appId": "without-endpoint-gateway.app",
            "channel": "orders-topic"
          },
          "blockKind": "gateway",
          "template": "http-gateway"
        }
        """;

    /// <summary>A CLI record whose own values keys are in no canonical order at all — the
    /// preserved ones must come back in exactly this relative order, behind the identity's.
    /// </summary>
    private const string CliScaffoldedRecordWithScrambledValues = """
        {
          "schemaVersion": 1,
          "values": {
            "contracts": "src/contracts",
            "appId": "order-gateway.app",
            "channel": "orders-topic",
            "componentName": "stale-name"
          },
          "blockKind": "component"
        }
        """;

    /// <summary>What <see cref="CliScaffoldedRecordWithScrambledValues"/> merges into.</summary>
    private const string MergedWithScrambledValues = """
        {
          "schemaVersion": 1,
          "values": {
            "componentName": "order-gateway",
            "organization": "Acme",
            "publishes": "order-placed",
            "payloadType": "Intropy.Framework.Components.Test.GeneratorExamples.OrderPlaced",
            "port": "order-gateway-endpoint",
            "endpointPath": "/orders",
            "eventSourceValue": "urn:Acme:order-gateway",
            "contracts": "src/contracts",
            "appId": "order-gateway.app",
            "channel": "orders-topic"
          },
          "blockKind": "gateway"
        }
        """;

    /// <summary>Valid JSON, no values member: not malformed, so no INTROPY1006 — the other
    /// fields are preserved and the identity-only values is written.</summary>
    private const string RecordWithoutValues = """
        {
          "schemaVersion": 1,
          "template": "http-gateway",
          "owner": "Acme",
          "blockKind": "component"
        }
        """;

    /// <summary>A values that is not an object: treated like an absent one.</summary>
    private const string RecordWithNonObjectValues = """
        {
          "schemaVersion": 1,
          "values": "broken-by-a-partial-write",
          "template": "http-gateway",
          "owner": "Acme",
          "blockKind": "component"
        }
        """;

    /// <summary>A record without usable values merges into: identity-only values, the record's
    /// other top-level fields preserved behind the block kind.</summary>
    private const string GeneratedRecordWithProvenance = """
        {
          "schemaVersion": 1,
          "values": {
            "componentName": "order-gateway",
            "organization": "Acme",
            "publishes": "order-placed",
            "payloadType": "Intropy.Framework.Components.Test.GeneratorExamples.OrderPlaced",
            "port": "order-gateway-endpoint",
            "endpointPath": "/orders",
            "eventSourceValue": "urn:Acme:order-gateway"
          },
          "blockKind": "gateway",
          "template": "http-gateway",
          "owner": "Acme"
        }
        """;

    /// <summary>The record as <c>intropy int create</c> scaffolds it: the CLI's provenance
    /// first, its values beside it — the CLI-owned keys the attribute cannot know (<c>appId</c>,
    /// the app id <c>sys create</c> requires; <c>channel</c>, the topic the system host declares;
    /// the render echoes: <c>componentClass</c>, <c>projectName</c>, <c>contracts</c>) — and an
    /// unknown field, perhaps a future schema version's, at the end.</summary>
    private const string CliScaffoldedRecord = """
        {
          "schemaVersion": 1,
          "template": "http-gateway",
          "owner": "Acme",
          "repo": "acme/integrations",
          "version": "1.2.3",
          "role": "gateway",
          "dataFlow": "inbound",
          "dependsOn": [
            {
              "name": "publisher",
              "type": "pubsub"
            }
          ],
          "values": {
            "componentName": "stale-name",
            "organization": "Old",
            "appId": "order-gateway.app",
            "channel": "orders-topic",
            "componentClass": "StaleGateway",
            "projectName": "StaleGateway",
            "contracts": "src/contracts"
          },
          "blockKind": "component",
          "future": null
        }
        """;
}
