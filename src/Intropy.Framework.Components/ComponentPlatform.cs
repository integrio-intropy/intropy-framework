using Dapr.Client;
using Intropy.BusinessIncidentService.Client;
using Intropy.Framework.Core.Configuration;
using Intropy.IdempotencyService.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Components;

/// <summary>
/// The platform every component runs on, whatever its host: the Dapr client, the platform-service
/// clients and the framework identity. Configuration, logging and telemetry are the host's.
/// </summary>
public static class ComponentPlatform
{
    /// <summary>The configuration key of the Idempotency Service's Dapr app-id.</summary>
    public const string IdempotencyServiceAppIdKey = "PlatformServices:IdempotencyServiceAppId";

    /// <summary>The configuration key of the Business Incident Service's Dapr app-id.</summary>
    public const string BusinessIncidentServiceAppIdKey = "PlatformServices:BusinessIncidentServiceAppId";

    /// <summary>The Idempotency Service's app-id in the platform-services deployment.</summary>
    public const string DefaultIdempotencyServiceAppId = "idempotency-service.services";

    /// <summary>The Business Incident Service's app-id in the platform-services deployment.</summary>
    public const string DefaultBusinessIncidentServiceAppId = "business-incident-service.services";

    /// <summary>Registers the platform for the component <paramref name="manifest"/> names.</summary>
    internal static void Add(IServiceCollection services, IConfiguration configuration, ComponentManifest manifest)
    {
        services.AddSingleton(_ => new DaprClientBuilder().Build());

        // After the DaprClient above on purpose: AddIdempotencyServiceClient contributes its own
        // DaprClient registration (camelCase JSON, string enums), and the last registration wins.
        services.AddIdempotencyServiceClient(c =>
            c.AppId = configuration[IdempotencyServiceAppIdKey] ?? DefaultIdempotencyServiceAppId);
        services.AddBusinessIncidentServiceClient(c =>
            c.AppId = configuration[BusinessIncidentServiceAppIdKey] ?? DefaultBusinessIncidentServiceAppId);

        services.AddIntropyFramework(x =>
        {
            x.ComponentName = manifest.ComponentName;
            x.ServiceNamespace = manifest.Organization;
        });
    }
}
