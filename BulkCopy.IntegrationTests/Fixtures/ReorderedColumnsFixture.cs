namespace BulkCopy.IntegrationTests.Fixtures;

public sealed class ReorderedColumnsFixture : TestFixture
{
    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();

        var path = CreateReorderedColumnsTestCsvFile();
        await RunBulkCopy(path);
    }
}