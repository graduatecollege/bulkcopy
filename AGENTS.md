# AGENTS instructions

This repo is a CLI for C#/.NET Core 10 SqlBulkCopy customized for our pipelines.

## Guidelines

- Don't add more external dependencies.
- The CLI is cross-platform (Windows, Linux, MacOS).
- Performance is important but second to maintainability.

## Testing

- Isolated unit tests are in `BulkCopy.Tests`. Run these when developing.
- Integration tests are in `BulkCopy.IntegrationTests`. Run these at the end of development.
