using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Intropy.Framework.Components.Generators;

/// <summary>
/// Turns the <c>IntropyComponentAttribute</c> into the component's scaffold record
/// (<c>.intropy/scaffold.json</c>, schema version 1), the artifact <c>intropy sys create</c>
/// assembles the system from. The record's shape is the one <c>intropy int create</c> writes, so
/// the system tooling reads a generated record exactly as it read a scaffolded one:
/// <c>values.componentName</c>, <c>values.organization</c>, <c>values.publishes</c>,
/// <c>values.payloadType</c> (the payload type's fully qualified name), <c>values.port</c> and
/// <c>values.endpointPath</c> when the attribute sets them, <c>values.eventSourceValue</c>, and
/// the root <c>blockKind</c>, inferred from the component's base class (GatewayComponent →
/// <c>gateway</c>). The record has two owners, and the contract between them is two levels deep.
/// At the top level the generator replaces the fields it owns (<c>schemaVersion</c> and
/// <c>blockKind</c>, plus <c>values</c> as a whole) and preserves all others — the CLI's
/// provenance travels in the same file. Within <c>values</c> the ownership goes down one more
/// level: the generator owns the attribute-backed identity keys (<c>componentName</c>,
/// <c>organization</c>, <c>publishes</c>, <c>payloadType</c>, <c>port</c>, <c>endpointPath</c>,
/// <c>eventSourceValue</c>) and preserves every other values key verbatim, in its original
/// order — the CLI's <c>appId</c> (the app id <c>sys create</c>'s Assemble requires) and
/// <c>channel</c> (the topic the system host declares), the render echoes it writes
/// (<c>componentClass</c>, <c>projectName</c>, <c>contracts</c>), and the keys a future schema
/// adds. Owning the identity keys includes owning their absence: an optional the attribute
/// leaves unset removes its key from the merged values, even when the record already carries
/// it.
/// </summary>
/// <remarks>
/// <para>The record is one per component, and it lives at the component root: the directory
/// holding the project — or, when the CLI scaffolded the project into <c>src/</c> under the
/// root, one level up, next to the root's Dockerfile. The generator looks for
/// <c>.intropy/scaffold.json</c> next to the project first, then one level up — never
/// further — merges the identity into the record it finds there, and creates the record at
/// the project directory when it finds neither.</para>
/// </remarks>
/// <remarks>
/// The generator also checks the attribute at compile time, so an identity problem fails the
/// build, not the deployment: a concrete <c>IntropyComponent</c> subclass without the attribute
/// (INTROPY1001), a missing required attribute member (INTROPY1002), an <c>EventSource</c> that is
/// not an absolute URI (INTROPY1003), a project that declares several components while
/// recording is on (INTROPY1004), and a malformed existing record, which is rewritten with the
/// attribute's identity alone under INTROPY1006 — the code is the source of truth. A record that
/// is valid JSON but carries no usable <c>values</c> object is not malformed: the other top-level
/// fields are preserved and the identity-only values is written as usual. Writing the
/// record can be turned off with
/// <c>&lt;IntropyGenerateScaffold&gt;false&lt;/IntropyGenerateScaffold&gt;</c> — a project that
/// only carries components (a test project, say) and never assembles a system needs no record.
/// The diagnostics stay on.
/// </remarks>
[Generator]
public sealed class IntropyComponentGenerator : ISourceGenerator
{
    private const string AttributeName = "Intropy.Framework.Components.IntropyComponentAttribute";
    private const string ComponentBaseName = "Intropy.Framework.Components.IntropyComponent";
    private const string ComponentsNamespace = "Intropy.Framework.Components";

    private static readonly DiagnosticDescriptor MissingAttribute = new(
        id: "INTROPY1001",
        title: "Component identity missing",
        messageFormat: "{0} derives from IntropyComponent but carries no [IntropyComponent]: a component declares " +
                       "its identity in code — put [IntropyComponent(...)] on the class, with its name, organization, " +
                       "message, payload and event source.",
        category: "Intropy.Components",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor MissingMember = new(
        id: "INTROPY1002",
        title: "Component identity incomplete",
        messageFormat: "{0}: the IntropyComponent attribute sets no {1}. The member is required, and the framework " +
                       "cannot name the component without it.",
        category: "Intropy.Components",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor EventSourceNotAbsolute = new(
        id: "INTROPY1003",
        title: "Component event source is not an absolute URI",
        messageFormat: "{0}: the IntropyComponent attribute's EventSource '{1}' is not an absolute URI (e.g. " +
                       "urn:Acme:order-gateway). The source is the CloudEvent source of every event the component sends.",
        category: "Intropy.Components",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor SeveralComponents = new(
        id: "INTROPY1004",
        title: "Several components want the one scaffold record",
        messageFormat: "This project declares several IntropyComponents ({0}), but .intropy/scaffold.json names one " +
                       "component: give each component its own project, or turn the record off with " +
                       "<IntropyGenerateScaffold>false</IntropyGenerateScaffold>.",
        category: "Intropy.Components",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor RecordNotWritten = new(
        id: "INTROPY1005",
        title: "Scaffold record not written",
        messageFormat: "The scaffold record could not be written to {0}: {1}",
        category: "Intropy.Components",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor MalformedRecord = new(
        id: "INTROPY1006",
        title: "Scaffold record is not valid JSON",
        messageFormat: "The scaffold record {0} carries no valid JSON and was rewritten: the component's identity " +
                       "in code is the source of truth, so the file now holds the record the attribute describes " +
                       "and nothing else. Recover any other fields — the CLI's provenance, say — from version " +
                       "control before committing.",
        category: "Intropy.Components",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    /// <inheritdoc cref="ISourceGenerator.Initialize" />
    public void Initialize(GeneratorInitializationContext context) =>
        context.RegisterForSyntaxNotifications(() => new ComponentClassReceiver());

    /// <inheritdoc cref="ISourceGenerator.Execute" />
    public void Execute(GeneratorExecutionContext context)
    {
        // Not a project that references the components — nothing to check, nothing to generate.
        if (context.Compilation.GetTypeByMetadataName(ComponentBaseName) is not { } componentBase ||
            context.Compilation.GetTypeByMetadataName(AttributeName) is null ||
            context.SyntaxReceiver is not ComponentClassReceiver receiver)
            return;

        var components = new List<Component>();
        foreach (var candidate in receiver.Candidates)
        {
            var model = context.Compilation.GetSemanticModel(candidate.SyntaxTree);
            if (model.GetDeclaredSymbol(candidate) is not INamedTypeSymbol type || !DerivesFrom(type, componentBase))
                continue;

            // A concrete block subclass that declares no identity fails the build, naming the class.
            if (type.IsAbstract)
                continue;
            var attribute = type.GetAttributes()
                .FirstOrDefault(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass?.OriginalDefinition,
                    context.Compilation.GetTypeByMetadataName(AttributeName)));
            if (attribute is null)
            {
                context.ReportDiagnostic(Diagnostic.Create(MissingAttribute, candidate.Identifier.GetLocation(),
                    type.Name));
                continue;
            }

            components.Add(Read(type, attribute, context));
        }

        if (components.Count == 0 || !GenerationEnabled(context, out var projectDirectory))
            return;
        if (components.Count > 1)
        {
            context.ReportDiagnostic(Diagnostic.Create(SeveralComponents, Location.None,
                string.Join(", ", components.Select(c => c.ClassName))));
            return;
        }

        WriteRecord(context, projectDirectory, components[0]);
    }

    /// <summary>Reads one component's identity from its attribute, checked: a missing member or a
    /// relative event source fails the build, naming the component.</summary>
    private static Component Read(INamedTypeSymbol type, AttributeData attribute, GeneratorExecutionContext context)
    {
        var named = attribute.NamedArguments.ToDictionary(argument => argument.Key, argument => argument.Value);
        string? Member(string member) => named.TryGetValue(member, out var value) &&
            value.Value is string s && s.Trim().Length > 0 ? s : null;
        var payload = named.TryGetValue("Payload", out var payloadValue) &&
            payloadValue.Value is ITypeSymbol payloadType ? payloadType : null;
        var componentName = attribute.ConstructorArguments is { Length: 1 } arguments &&
            arguments[0].Value is string name && name.Trim().Length > 0 ? name : null;
        Location AttributeLocation() =>
            attribute.ApplicationSyntaxReference?.GetSyntax().GetLocation() ?? type.Locations.First();

        if (componentName is null)
            context.ReportDiagnostic(Diagnostic.Create(MissingMember, AttributeLocation(), type.Name, "componentName"));
        if (Member("Organization") is null)
            context.ReportDiagnostic(Diagnostic.Create(MissingMember, AttributeLocation(), type.Name, "Organization"));
        if (Member("Publishes") is null)
            context.ReportDiagnostic(Diagnostic.Create(MissingMember, AttributeLocation(), type.Name, "Publishes"));
        if (payload is null)
            context.ReportDiagnostic(Diagnostic.Create(MissingMember, AttributeLocation(), type.Name, "Payload"));
        var eventSource = Member("EventSource");
        if (eventSource is null)
            context.ReportDiagnostic(Diagnostic.Create(MissingMember, AttributeLocation(), type.Name, "EventSource"));
        else if (!Uri.TryCreate(eventSource, UriKind.Absolute, out _))
            context.ReportDiagnostic(Diagnostic.Create(EventSourceNotAbsolute, AttributeLocation(), type.Name,
                eventSource));

        return new Component(type.Name, componentName, Member("Organization"), Member("Publishes"),
            payload?.ToString(), eventSource, Member("Port"), Member("EndpointPath"), BlockKind(type));
    }

    /// <summary>The record's <c>blockKind</c>: the component's nearest block base class's kind —
    /// the base class's simple name with its <c>Component</c> suffix stripped and lowercased
    /// (GatewayComponent → <c>gateway</c>); a component deriving <c>IntropyComponent</c> directly,
    /// or through an unknown block, is <c>component</c>.</summary>
    private static string BlockKind(INamedTypeSymbol type)
    {
        for (var block = type.BaseType; block is not null; block = block.BaseType)
        {
            if (!block.ContainingNamespace.ToDisplayString().StartsWith(ComponentsNamespace, StringComparison.Ordinal))
                continue;
            return block.Name == "IntropyComponent" ? "component"
                : block.Name.EndsWith("Component", StringComparison.Ordinal)
                    ? block.Name.Substring(0, block.Name.Length - "Component".Length).ToLowerInvariant()
                    : block.Name.ToLowerInvariant();
        }
        return "component";
    }

    private static bool DerivesFrom(ITypeSymbol type, ITypeSymbol baseType)
    {
        for (var current = type.BaseType; current is not null; current = current.BaseType)
            if (SymbolEqualityComparer.Default.Equals(current, baseType))
                return true;
        return false;
    }

    private static bool GenerationEnabled(GeneratorExecutionContext context, out string projectDirectory)
    {
        projectDirectory = string.Empty;
        if (!context.AnalyzerConfigOptions.GlobalOptions.TryGetValue("build_property.MSBuildProjectDirectory",
                out var directory) || string.IsNullOrWhiteSpace(directory))
            return false;
        if (context.AnalyzerConfigOptions.GlobalOptions.TryGetValue("build_property.IntropyGenerateScaffold",
                out var enabled) && enabled.Equals("false", StringComparison.OrdinalIgnoreCase))
            return false;
        projectDirectory = directory;
        return true;
    }

    /// <summary>Writes <c>.intropy/scaffold.json</c> at the component root, in the schema
    /// version 1 shape <c>intropy int create</c> writes and <c>intropy sys create</c> reads. The
    /// record's location follows the same rule the CLI scaffolded by: the record is looked up
    /// next to the project, then in the project directory's parent — exactly those two, never
    /// further — because a CLI-scaffolded component keeps its project in <c>src/</c> under the
    /// root and its record at the root, next to the Dockerfile. The record found first is the
    /// one the identity is merged into; when neither carries a record, one is created at the
    /// project directory (a component whose project sits at its root writes to its root).
    /// The record has two owners, and this is where the contract is kept — two levels of it: the
    /// generator replaces the top-level fields it owns (<c>schemaVersion</c>, <c>values</c>,
    /// <c>blockKind</c>) and preserves every other top-level field verbatim; within
    /// <c>values</c> it likewise replaces the identity keys and preserves every other values key
    /// verbatim, in its original order — the CLI's <c>appId</c>, <c>channel</c>, its render
    /// echoes. A record that is valid JSON but whose <c>values</c> is absent or not an object is
    /// not malformed: the identity-only values is written and the other top-level fields are
    /// preserved as usual. A record whose merged content is byte-identical to what is on disk
    /// is not written again: an unchanged identity must not touch the file, whose mtime noise
    /// would trigger rebuilds and git churn. Every write goes through a temp file and a
    /// replace, so two concurrent compilations of the same project (multi-targeting) never
    /// tear the record apart.</summary>
    private static void WriteRecord(GeneratorExecutionContext context, string projectDirectory, Component component)
    {
        // The record lives at the component root: looked up next to the project, then one level
        // up when the CLI put the project in src/ under the root — bounded at these two, never
        // walked further. Found neither: the project directory is the root to create the record at.
        var projectRecordPath = Path.Combine(projectDirectory, ".intropy", "scaffold.json");
        var parentDirectory = Path.GetDirectoryName(projectDirectory);
        var parentRecordPath = parentDirectory is null
            ? null
            : Path.Combine(parentDirectory, ".intropy", "scaffold.json");
        var path = File.Exists(projectRecordPath) ? projectRecordPath
            : parentRecordPath is not null && File.Exists(parentRecordPath) ? parentRecordPath
            : projectRecordPath;
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            var existing = File.Exists(path) ? File.ReadAllText(path) : null;
            string content;
            if (existing is null)
                content = Render(component);
            else if (TryReadTopLevelFields(existing, out var fields, out var existingValues))
                content = Merge(component, fields, existingValues);
            else
            {
                // Unreadable is unknown: the code owns the identity, so a malformed record is
                // replaced with the attribute's fields alone, and the loss is named, not silent.
                context.ReportDiagnostic(Diagnostic.Create(MalformedRecord, Location.None, path));
                content = Render(component);
            }

            if (existing is not null && string.Equals(existing, content, StringComparison.Ordinal))
                return;
            WriteAtomically(path, content);
        }
        catch (Exception e)
        {
            context.ReportDiagnostic(Diagnostic.Create(RecordNotWritten, Location.None, path, e.Message));
        }
    }

    /// <summary>Re-renders the identity-owned fields and appends the existing record's other
    /// fields behind them: the generator's canonical order for its own fields first, then the
    /// preserved fields in their original order, each value byte-for-byte as the record carried
    /// it — including nested objects and arrays, carried as raw text, never reparsed and never
    /// reformatted. The contract runs two levels deep: within <c>values</c> the identity keys
    /// are re-rendered and every other values member — the CLI's <c>appId</c> and
    /// <c>channel</c>, its render echoes — keeps its place behind them, likewise byte-for-byte
    /// and in its original order; owning the identity keys includes owning their absence, so an
    /// optional the attribute leaves unset drops its key even when the record carried it.</summary>
    private static string Merge(Component component, List<RecordField> fields, string? existingValues) =>
        Render(component, PreservedValues(existingValues), PreservedTopLevel(fields));

    /// <summary>The existing record's <c>values</c> members the generator preserves: read with
    /// the same tolerant reader, minus the identity keys the generator owns. A values that is
    /// absent or not an object — a record the CLI has not scaffolded, say — yields nothing to
    /// preserve: the identity-only values is written, and the record is not malformed.</summary>
    private static List<RecordField>? PreservedValues(string? existingValues)
    {
        if (existingValues is null || existingValues.Length == 0 || existingValues[0] != '{')
            return null;
        if (!TryReadMembers(existingValues, 0, out var members, out var end) || end != existingValues.Length)
            return null;
        var preserved = new List<RecordField>();
        foreach (var member in members)
            if (!OwnedValuesKey(member.Name))
                preserved.Add(member);
        return preserved;
    }

    /// <summary>The values keys the generator owns — the identity keys, the ones the attribute
    /// backs. Every other values key belongs to the record's other owner: the CLI's <c>appId</c>
    /// and <c>channel</c>, its render echoes (<c>componentClass</c>, <c>projectName</c>,
    /// <c>contracts</c>), and whatever a future schema adds.</summary>
    private static bool OwnedValuesKey(string name) => name is "componentName" or "organization" or
        "publishes" or "payloadType" or "port" or "endpointPath" or "eventSourceValue";

    /// <summary>The existing record's other top-level fields, rendered with their leading comma,
    /// behind the block kind.</summary>
    private static string PreservedTopLevel(List<RecordField> fields)
    {
        var preserved = new StringBuilder();
        foreach (var field in fields)
            preserved.Append(",\n  ").Append(Json(field.Name)).Append(": ").Append(field.Value);
        return preserved.ToString();
    }

    /// <summary>Writes the record through a temp file in the record's own directory — the one
    /// filesystem a rename can be atomic on — then replaces the live file with it. When the
    /// platform refuses it (the destination vanished under two racing compilations, say) or the
    /// filesystem does not support the swap at all — Docker volumes and network mounts have both
    /// been known — a delete-and-move writes the same bytes, one step less atomic. The fallback
    /// catches every failure the replace itself can name for the filesystem's sake: the only
    /// errors it lets through are the write's and the move's, which INTROPY1005 must report.</summary>
    private static void WriteAtomically(string path, string content)
    {
        var temporary = Path.Combine(Path.GetDirectoryName(path)!,
            Path.GetFileName(path) + "." + Guid.NewGuid().ToString("N") + ".tmp");
        try
        {
            File.WriteAllText(temporary, content, new UTF8Encoding(false));
            try
            {
                File.Replace(temporary, path, destinationBackupFileName: null);
            }
            catch (Exception e) when (e is IOException or PlatformNotSupportedException or
                UnauthorizedAccessException)
            {
                // The replace is the nicety, not the requirement: any filesystem that will not
                // carry it still gets the same bytes via delete-and-move.
                File.Delete(path);
                File.Move(temporary, path);
            }
        }
        catch
        {
            try { File.Delete(temporary); } catch { /* the original failure is the one to report */ }
            throw;
        }
    }

    /// <summary>Reads the existing record's top-level members, tolerant: field names are decoded,
    /// values are kept as raw JSON text — strings, objects and arrays to their own closing
    /// delimiter, atoms to the next delimiter. Field names the generator owns —
    /// <c>schemaVersion</c>, <c>values</c>, <c>blockKind</c> — are left out of
    /// <paramref name="fields"/>; they are replaced. The raw text of the <c>values</c> member is
    /// carried out separately, for the merge to read its members the same way. Anything the
    /// shape of an object with members cannot accommodate — a different top level, a broken
    /// string, an unbalanced brace, content trailing the object — returns false, and the caller
    /// treats the file as malformed.</summary>
    private static bool TryReadTopLevelFields(string text, out List<RecordField> fields, out string? valuesValue)
    {
        fields = [];
        valuesValue = null;
        var start = 0;
        if (start < text.Length && text[start] == '\uFEFF')
            start++;
        if (!TryReadMembers(text, start, out var members, out var end))
            return false;
        for (var i = end; i < text.Length; i++)
            if (!char.IsWhiteSpace(text[i]))
                return false;
        foreach (var member in members)
            switch (member.Name)
            {
                case "values" when valuesValue is null:
                    valuesValue = member.Value;
                    break;
                case "schemaVersion" or "values" or "blockKind":
                    break;
                default:
                    fields.Add(member);
                    break;
            }
        return true;
    }

    /// <summary>Reads the members of the JSON object at <paramref name="start"/>, tolerant: field
    /// names are decoded, values are kept as raw JSON text — strings, objects and arrays to
    /// their own closing delimiter, atoms to the next delimiter. Every member is returned, in
    /// the order the text carries them; the caller filters what it owns. Returns false for
    /// anything the shape of an object with members cannot accommodate — a broken string, an
    /// unbalanced brace, content trailing the object's closing brace.</summary>
    private static bool TryReadMembers(string text, int start, out List<RecordField> fields, out int end)
    {
        fields = [];
        end = 0;
        var i = start;

        void SkipWhitespace()
        {
            while (i < text.Length && char.IsWhiteSpace(text[i]))
                i++;
        }

        // Reads the quoted string at i, leaving i just past its closing quote; null when the
        // string is unterminated or carries a broken escape.
        string? ReadString()
        {
            i++;
            var decoded = new StringBuilder();
            while (i < text.Length)
            {
                var character = text[i++];
                if (character == '"')
                    return decoded.ToString();
                if (character != '\\')
                {
                    decoded.Append(character);
                    continue;
                }
                if (i >= text.Length)
                    return null;
                switch (text[i++])
                {
                    case '"': decoded.Append('"'); break;
                    case '\\': decoded.Append('\\'); break;
                    case '/': decoded.Append('/'); break;
                    case 'b': decoded.Append('\b'); break;
                    case 'f': decoded.Append('\f'); break;
                    case 'n': decoded.Append('\n'); break;
                    case 'r': decoded.Append('\r'); break;
                    case 't': decoded.Append('\t'); break;
                    case 'u':
                        if (i + 4 > text.Length || !int.TryParse(text.Substring(i, 4), NumberStyles.HexNumber,
                                CultureInfo.InvariantCulture, out var code))
                            return null;
                        decoded.Append((char)code);
                        i += 4;
                        break;
                    default:
                        return null;
                }
            }
            return null;
        }

        SkipWhitespace();
        if (i >= text.Length || text[i] != '{')
            return false;
        i++;
        while (true)
        {
            SkipWhitespace();
            if (i >= text.Length)
                return false;
            if (text[i] == '}')
            {
                end = i + 1;
                return true;
            }
            if (text[i] != '"')
                return false;
            var name = ReadString();
            if (name is null)
                return false;
            SkipWhitespace();
            if (i >= text.Length || text[i] != ':')
                return false;
            i++;
            SkipWhitespace();
            if (i >= text.Length)
                return false;
            var valueStart = i;
            switch (text[i])
            {
                case '"':
                    i++;
                    var terminated = false;
                    while (i < text.Length)
                    {
                        var character = text[i++];
                        if (character == '\\')
                        {
                            i++; // the escaped character itself is skipped blind: a trailing lone
                                 // backslash falls out of the loop without its closing quote
                            continue;
                        }
                        if (character == '"')
                        {
                            terminated = true;
                            break;
                        }
                    }
                    if (!terminated)
                        return false;
                    break;
                case '{':
                case '[':
                    var opening = text[valueStart];
                    var closing = opening == '{' ? '}' : ']';
                    var depth = 0;
                    var inString = false;
                    while (i < text.Length)
                    {
                        var character = text[i++];
                        if (inString)
                        {
                            if (character == '\\')
                            {
                                if (i >= text.Length)
                                    return false;
                                i++;
                            }
                            else if (character == '"')
                                inString = false;
                        }
                        else if (character == '"')
                            inString = true;
                        else if (character == opening)
                            depth++;
                        else if (character == closing)
                        {
                            depth--;
                            if (depth == 0)
                                break;
                        }
                    }
                    if (depth != 0 || inString)
                        return false;
                    break;
                default:
                    // An atom — a number, true, false, null — reads to the next delimiter.
                    while (i < text.Length && text[i] != ',' && text[i] != '}' && text[i] != ']' &&
                           !char.IsWhiteSpace(text[i]))
                        i++;
                    break;
            }

            fields.Add(new RecordField(name, text.Substring(valueStart, i - valueStart)));
            SkipWhitespace();
            if (i >= text.Length)
                return false;
            if (text[i] == ',')
            {
                i++;
                continue;
            }
            if (text[i] == '}')
            {
                end = i + 1;
                return true;
            }
            return false;
        }
    }

    /// <summary>The record's identity-owned half: schema version 1 over the attribute's values
    /// and the block kind. The optional <paramref name="preservedValues"/> carries the existing
    /// record's other <c>values</c> members, written behind the identity keys — their order and
    /// bytes kept exactly as the record had them. The optional <paramref name="preserved"/>
    /// carries the existing record's other top-level fields, already rendered with their leading
    /// comma, behind the block kind — the empty string when the record has none.</summary>
    private static string Render(Component c, List<RecordField>? preservedValues = null, string preserved = "")
    {
        var values = new StringBuilder();
        void Value(string key, string? value)
        {
            if (value is not { Length: > 0 })
                return;
            values.Append(values.Length == 0 ? string.Empty : ",\n    ")
                  .Append(Json(key)).Append(": ").Append(Json(value));
        }

        Value("componentName", c.ComponentName);
        Value("organization", c.Organization);
        Value("publishes", c.Publishes);
        Value("payloadType", c.PayloadType);
        Value("port", c.Port);
        Value("endpointPath", c.EndpointPath);
        Value("eventSourceValue", c.EventSource);
        if (preservedValues is not null)
            foreach (var member in preservedValues)
                values.Append(values.Length == 0 ? string.Empty : ",\n    ")
                      .Append(Json(member.Name)).Append(": ").Append(member.Value);

        return new StringBuilder()
            .Append("{\n  \"schemaVersion\": 1,\n  \"values\": {\n    ").Append(values)
            .Append("\n  },\n  \"blockKind\": ").Append(Json(c.BlockKind)).Append(preserved).Append("\n}\n")
            .ToString();
    }

    /// <summary>Quotes a string for the record, escaping what JSON needs escaped and nothing else.</summary>
    private static string Json(string value)
    {
        var quoted = new StringBuilder("\"");
        foreach (var character in value)
        {
            switch (character)
            {
                case '"':
                    quoted.Append("\\\"");
                    break;
                case '\\':
                    quoted.Append("\\\\");
                    break;
                case '\b':
                    quoted.Append("\\b");
                    break;
                case '\f':
                    quoted.Append("\\f");
                    break;
                case '\n':
                    quoted.Append("\\n");
                    break;
                case '\r':
                    quoted.Append("\\r");
                    break;
                case '\t':
                    quoted.Append("\\t");
                    break;
                default:
                    if (character < ' ')
                        quoted.Append("\\u").Append(((int)character).ToString("x4", CultureInfo.InvariantCulture));
                    else
                        quoted.Append(character);
                    break;
            }
        }
        return quoted.Append('"').ToString();
    }

    /// <summary>One top-level member of the existing record, for a merge that keeps it: the name
    /// decoded, the value carried as raw JSON text, byte-for-byte as the file had it.</summary>
    /// <param name="Name">The member's decoded name.</param>
    /// <param name="Value">The member's raw JSON value, from its first character to its last —
    /// quotes and brackets in, surrounding whitespace out.</param>
    private sealed record RecordField(string Name, string Value);

    /// <summary>One component's identity, as the record names it.</summary>
    /// <param name="ClassName">The component class's name, for diagnostics.</param>
    /// <param name="ComponentName">The record's <c>values.componentName</c>.</param>
    /// <param name="Organization">The record's <c>values.organization</c>.</param>
    /// <param name="Publishes">The record's <c>values.publishes</c>.</param>
    /// <param name="PayloadType">The record's <c>values.payloadType</c> — the payload type's fully
    /// qualified name.</param>
    /// <param name="EventSource">The record's <c>values.eventSourceValue</c>.</param>
    /// <param name="Port">The record's <c>values.port</c>, when the attribute sets it.</param>
    /// <param name="EndpointPath">The record's <c>values.endpointPath</c>, when the attribute sets it.</param>
    /// <param name="BlockKind">The record's <c>blockKind</c>, inferred from the base class.</param>
    private sealed record Component(string ClassName, string? ComponentName, string? Organization,
        string? Publishes, string? PayloadType, string? EventSource, string? Port, string? EndpointPath,
        string BlockKind);

    /// <summary>Collects the class declarations that either carry an <c>IntropyComponent</c>
    /// attribute or could be a component without one. Names only, no semantic work: the receiver
    /// runs on every keystroke; the attribute and base-class checks run once, in
    /// <see cref="Execute"/>, on the compilation's semantic model.</summary>
    private sealed class ComponentClassReceiver : ISyntaxReceiver
    {
        /// <summary>The candidate classes.</summary>
        public List<ClassDeclarationSyntax> Candidates { get; } = [];

        public void OnVisitSyntaxNode(SyntaxNode node)
        {
            if (node is not ClassDeclarationSyntax candidate)
                return;
            var attributeNamed = candidate.AttributeLists.Count > 0 &&
                candidate.AttributeLists.SelectMany(list => list.Attributes).Any(attribute =>
                    attribute.Name.ToString() is "IntropyComponent" or "IntropyComponentAttribute"
                        or "Intropy.Framework.Components.IntropyComponent"
                        or "Intropy.Framework.Components.IntropyComponentAttribute");
            if (attributeNamed || candidate.BaseList is not null)
                Candidates.Add(candidate);
        }
    }
}
