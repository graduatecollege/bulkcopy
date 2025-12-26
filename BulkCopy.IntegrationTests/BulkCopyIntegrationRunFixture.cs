namespace BulkCopy.IntegrationTests;

public class BulkCopyIntegrationRunFixture : BulkCopyIntegrationTestFixture
{
    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await RunBulkCopy();
    }
}