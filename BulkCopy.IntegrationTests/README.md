# BulkCopy Integration Tests (C# with Testcontainers)

This project contains C#-based integration tests for the BulkCopy application using xUnit and Testcontainers.

## Prerequisites

- .NET 10 SDK
- Docker installed and running
- The BulkCopy application built in Release mode

## Running the Tests

### Run all integration tests:

```bash
dotnet test
```

### Run a specific test:

```bash
dotnet test --filter "BulkCopy_WithValidAndInvalidRows_ImportsValidRowsAndLogsErrors"
```
