# BulkCopy Integration Tests (C# with Testcontainers)

This project contains C#-based integration tests for the BulkCopy application using xUnit and Testcontainers.

## Prerequisites

- .NET 10 SDK
- Docker installed and running
- The BulkCopy application built in Release mode

## Advantages Over Bash Script

The C# integration tests provide several benefits over the previous bash script approach:

1. **Structured Testing Framework**: Uses xUnit with clear test structure, assertions, and reporting
2. **Better Maintainability**: ~300 lines of structured C# code vs 345 lines of bash
3. **Type Safety**: Compile-time checking of test logic
4. **Better IDE Support**: IntelliSense, debugging, refactoring tools
5. **Automatic Container Management**: Testcontainers handles Docker lifecycle automatically
6. **Parallel Execution**: Can run multiple tests in parallel
7. **Better Error Messages**: xUnit provides detailed assertion failure messages
8. **CI/CD Integration**: Native integration with dotnet test and test reporting tools
9. **Reusable Helper Methods**: Easy to share test utilities across multiple test classes
10. **Professional Tooling**: Built-in support for test filtering, test discovery, and coverage reporting

## Running the Tests

### Run all integration tests:
```bash
cd BulkCopy.IntegrationTests
dotnet test
```

### Run with detailed output:
```bash
dotnet test --verbosity normal
```

### Run with test results report:
```bash
dotnet test --logger "trx;LogFileName=test-results.trx"
```

### Debug a specific test:
```bash
dotnet test --filter "BulkCopy_WithValidAndInvalidRows_ImportsValidRowsAndLogsErrors"
```

## What the Tests Verify

The integration test suite verifies:

### Data Import
- ✅ 20 valid rows are successfully imported
- ✅ 5 invalid rows (6, 11, 16, 23, 24) are correctly skipped
- ✅ Sample good rows (1, 5, 10, 15, 20) are present in the table

### Error Logging
- ✅ Error table is automatically created in the error database
- ✅ Exactly 5 errors are logged (one for each bad row)
- ✅ Error table has correct schema (8 columns):
  - Id, SourceDatabase, SourceTable, RowNumber
  - CsvHeaders, CsvRowData, ErrorMessage, ErrorTimestamp
- ✅ Error logs contain correct source database and table
- ✅ Error row numbers match the bad rows (6, 11, 16, 23, 24)
- ✅ CSV headers are captured in error logs
- ✅ CSV row data is present in error logs
- ✅ Error messages are recorded

## Test Data

The test creates a CSV file with 25 rows:
- **Good rows**: 20 rows with valid data
- **Bad rows**: 5 rows with invalid data
  - Row 6: Invalid age ("BadAge")
  - Row 11: Invalid age ("InvalidAge")  
  - Row 16: Multiple errors (invalid age, salary, score)
  - Row 23: Invalid birth date
  - Row 24: Code too long for column (EMPL024LONG > 7 chars)

## Container Management

Testcontainers automatically:
- Pulls the SQL Server 2022 Docker image if not present
- Starts a fresh SQL Server container for each test run
- Provides a connection string to the container
- Cleans up the container after tests complete
- Handles port mapping automatically

## Debugging Tests

To debug tests in Visual Studio Code or Visual Studio:
1. Set breakpoints in the test code
2. Use the "Debug Test" option in the IDE
3. The container will remain running during the debug session
4. Step through test code and inspect database state

## Extending the Tests

To add new test cases:

```csharp
[Fact]
public async Task NewTestCase_Scenario_ExpectedBehavior()
{
    // Arrange
    // ... setup test data
    
    // Act
    // ... execute BulkCopy
    
    // Assert
    // ... verify results
    Assert.Equal(expected, actual);
}
```

Helper methods are provided for common operations:
- `GetRowCount()` - Count rows in a table
- `GetCountForIds()` - Count specific rows by ID
- `TableExists()` - Check if a table exists
- `GetColumnCount()` - Count columns in a table
- And more...

## CI/CD Integration

The tests integrate seamlessly with CI/CD pipelines:

```yaml
# GitHub Actions example
- name: Run Integration Tests
  run: dotnet test BulkCopy.IntegrationTests/BulkCopy.IntegrationTests.csproj
```

The test results can be published as test artifacts and displayed in the CI/CD UI.

## Comparison with Bash Script

| Feature | Bash Script | C# with Testcontainers |
|---------|-------------|----------------------|
| Lines of code | 345 | ~300 |
| Test structure | Manual | xUnit framework |
| Assertions | String comparison | Strong-typed assertions |
| Container mgmt | Manual docker commands | Automatic with Testcontainers |
| IDE support | Limited | Full IntelliSense/debugging |
| Error messages | Generic | Detailed assertion failures |
| Parallel execution | No | Yes |
| CI/CD integration | Custom | Native dotnet test |
| Reusability | Copy/paste | Inheritance/composition |
| Maintainability | Low | High |

## Notes

- Docker must be running before executing tests
- First test run may take longer while pulling the SQL Server image (~700MB)
- Each test run uses a fresh SQL Server instance for isolation
- The BulkCopy application must be built before running tests
- Cleanup is automatic - no manual container removal needed
