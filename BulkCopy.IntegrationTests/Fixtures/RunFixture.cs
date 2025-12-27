namespace BulkCopy.IntegrationTests.Fixtures;

public class RunFixture : TestFixture
{
    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await RunBulkCopy();
    }
}