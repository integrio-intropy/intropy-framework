using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Testing.Services;

namespace Intropy.Framework.Testing.Test.Services;

public class FakeBusinessIncidentServiceClientTests
{
    private static readonly Uri Source = new("urn:test-component");

    private static BusinessIncidentData Data(string description = "validation failed") => new()
    {
        Description = description,
        Context = new Dictionary<string, string> { ["field"] = "value" },
    };

    [Fact]
    public async Task Trigger_RecordsIncidentWithAllFields()
    {
        var client = new FakeBusinessIncidentServiceClient();
        var data = Data();

        await client.Trigger(Source, "order.invalid", "ce-1", data, "batch-7");

        var incident = Assert.Single(client.Incidents);
        Assert.Equal(Source, incident.Source);
        Assert.Equal("order.invalid", incident.Subject);
        Assert.Equal("ce-1", incident.Id);
        Assert.Same(data, incident.Data);
        Assert.Equal("batch-7", incident.BatchId);
    }

    [Fact]
    public async Task Resolve_AppendsToResolved_AndMarksMatchingIncident()
    {
        var client = new FakeBusinessIncidentServiceClient();
        await client.Trigger(Source, "order.invalid", "ce-1", Data(), null);

        await client.Resolve(Source, "order.invalid", "ce-1", null);

        var resolved = Assert.Single(client.Resolved);
        Assert.Equal("order.invalid", resolved.Subject);
        Assert.Equal("ce-1", resolved.Id);

        var list = await client.List(new IncidentFilter { Status = "Resolved" });
        Assert.Equal(1, list.Total);
    }

    [Fact]
    public async Task Resolve_DifferentSource_DoesNotMarkIncidentResolved()
    {
        var client = new FakeBusinessIncidentServiceClient();
        await client.Trigger(Source, "order.invalid", "ce-1", Data(), null);

        await client.Resolve(new Uri("urn:other-component"), "order.invalid", "ce-1", null);

        Assert.Single(client.Resolved);
        var list = await client.List(new IncidentFilter { Status = "Resolved" });
        Assert.Equal(0, list.Total);
    }

    [Fact]
    public async Task Resolve_Unmatched_RecordedWithoutThrowing()
    {
        var client = new FakeBusinessIncidentServiceClient();

        await client.Resolve(Source, "nothing.triggered", "ce-99", null);

        var resolved = Assert.Single(client.Resolved);
        Assert.Equal("ce-99", resolved.Id);
        Assert.Empty(client.Incidents);
    }

    [Fact]
    public async Task TriggerException_PropagatesAsTypedException_WithoutRecording()
    {
        var client = new FakeBusinessIncidentServiceClient
        {
            TriggerException = new BusinessIncidentServiceException(
                "service unavailable", new HttpRequestException()),
        };

        await Assert.ThrowsAsync<BusinessIncidentServiceException>(
            () => client.Trigger(Source, "order.invalid", "ce-1", Data(), null));
        Assert.Empty(client.Incidents);
    }

    [Fact]
    public async Task GetById_ServesFromRecordedState_ReflectsResolution()
    {
        var client = new FakeBusinessIncidentServiceClient();
        await client.Trigger(Source, "order.invalid", "ce-1", Data(), null);
        var id = (await client.List()).Items.Single().Id;

        var before = await client.GetById(id);
        Assert.NotNull(before);
        Assert.Equal("Triggered", before.Status);

        await client.Resolve(Source, "order.invalid", "ce-1", null);

        var after = await client.GetById(id);
        Assert.NotNull(after);
        Assert.Equal("Resolved", after.Status);
        Assert.NotNull(after.ResolvedAt);
    }

    [Fact]
    public async Task GetById_UnknownId_ReturnsNull()
    {
        var client = new FakeBusinessIncidentServiceClient();

        var response = await client.GetById(Guid.NewGuid());

        Assert.Null(response);
    }

    [Fact]
    public async Task GetEvents_ReturnsTriggeredThenResolvedEvents()
    {
        var client = new FakeBusinessIncidentServiceClient();
        await client.Trigger(Source, "order.invalid", "ce-1", Data(), null);
        var id = (await client.List()).Items.Single().Id;

        await client.Resolve(Source, "order.invalid", "ce-1", null);

        var events = await client.GetEvents(id);
        Assert.Equal(2, events.Count);
        Assert.Equal(EventTypes.Triggered, events[0].EventType);
        Assert.Equal(EventTypes.Resolved, events[1].EventType);
    }

    [Fact]
    public async Task ResolveManual_MarksResolved_ReturnsTrue_UnknownIdReturnsFalse()
    {
        var client = new FakeBusinessIncidentServiceClient();
        await client.Trigger(Source, "order.invalid", "ce-1", Data(), null);
        var id = (await client.List()).Items.Single().Id;

        Assert.True(await client.ResolveManual(id));
        Assert.False(await client.ResolveManual(Guid.NewGuid()));

        var response = await client.GetById(id);
        Assert.NotNull(response);
        Assert.Equal("Resolved", response.Status);
    }
}
