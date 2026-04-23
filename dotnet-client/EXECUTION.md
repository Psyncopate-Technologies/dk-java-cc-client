# .NET Kafka Client - Execution Guide

## Overview
This .NET client connects to Confluent Cloud using Microsoft Entra ID (Azure AD) OAuth/OIDC authentication.

## Prerequisites

### System Requirements
- .NET 8.0 SDK (or .NET 6.0/7.0 with minor changes)
- Network access to:
  - Confluent Cloud broker: `lkc-1o1jkv.eastus.azure.private.confluent.cloud:9092`
  - Microsoft login: `login.microsoftonline.com`
  - NuGet: `api.nuget.org` (for package restore)

### Required NuGet Packages

| Package | Version | Purpose |
|---------|---------|---------|
| Confluent.Kafka | 2.3.0 | Kafka producer/consumer |
| Azure.Identity | 1.10.4 | Azure AD token acquisition |
| Microsoft.Extensions.Configuration | 8.0.0 | Configuration management |
| Microsoft.Extensions.Configuration.Json | 8.0.0 | JSON config file support |
| Microsoft.Extensions.Configuration.EnvironmentVariables | 8.0.0 | Environment variable support |

## Installation

### Option 1: Install via dotnet CLI (requires internet)
```bash
# Navigate to project directory
cd dotnet-client

# Restore packages
dotnet restore
```

### Option 2: Offline Installation (for restricted VMs)

#### Step 1: Download packages on a machine with internet access
```bash
# Create a local package folder
mkdir nuget-packages

# Download packages
dotnet restore --packages ./nuget-packages
```

Or manually download `.nupkg` files from NuGet:
- https://www.nuget.org/packages/Confluent.Kafka/2.3.0
- https://www.nuget.org/packages/Azure.Identity/1.10.4
- https://www.nuget.org/packages/Azure.Core/1.36.0
- https://www.nuget.org/packages/Microsoft.Identity.Client/4.58.1
- https://www.nuget.org/packages/Microsoft.Extensions.Configuration/8.0.0
- https://www.nuget.org/packages/Microsoft.Extensions.Configuration.Json/8.0.0
- https://www.nuget.org/packages/Microsoft.Extensions.Configuration.EnvironmentVariables/8.0.0

#### Step 2: Create NuGet.Config for offline restore
Create a `NuGet.Config` file in the project directory:
```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="local" value="./nuget-packages" />
  </packageSources>
</configuration>
```

#### Step 3: Restore from local source
```bash
dotnet restore --source ./nuget-packages
```

### Install .NET SDK on VM

#### Ubuntu/Debian
```bash
# Add Microsoft package repository
wget https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
rm packages-microsoft-prod.deb

# Install .NET SDK
sudo apt-get update
sudo apt-get install -y dotnet-sdk-8.0
```

#### RHEL/CentOS/Fedora
```bash
# Add Microsoft repository
sudo rpm -Uvh https://packages.microsoft.com/config/rhel/8/packages-microsoft-prod.rpm

# Install .NET SDK
sudo dnf install dotnet-sdk-8.0
```

#### Windows
Download installer from: https://dotnet.microsoft.com/download/dotnet/8.0

#### Offline .NET SDK Installation
Download the SDK binary from Microsoft:
- Linux x64: https://dotnet.microsoft.com/download/dotnet/8.0
- Extract and add to PATH:
```bash
mkdir -p $HOME/dotnet
tar -xzf dotnet-sdk-8.0.xxx-linux-x64.tar.gz -C $HOME/dotnet
export DOTNET_ROOT=$HOME/dotnet
export PATH=$PATH:$HOME/dotnet
```

## Configuration

### 1. Set Environment Variable for Client Secret
```bash
# Linux/macOS
export AZURE_CLIENT_SECRET="your-client-secret-here"

# Windows PowerShell
$env:AZURE_CLIENT_SECRET="your-client-secret-here"

# Windows CMD
set AZURE_CLIENT_SECRET=your-client-secret-here
```

### 2. Update appsettings.json
Edit `appsettings.json` and replace the placeholder values:

```json
{
  "Kafka": {
    "BootstrapServers": "lkc-1o1jkv.eastus.azure.private.confluent.cloud:9092",
    "LogicalCluster": "lkc-vgr7p5",
    "IdentityPoolId": "<POOL_ID>",      // Replace with your Identity Pool ID
    "Topic": "dkp-dotnet-client-test",
    "GroupId": "dkp-dotnet-oidc-test-group"
  },
  "AzureAd": {
    "TenantId": "1b9dca15-4db4-4905-8725-d318d11c6875",
    "ClientId": "<APP_ID>",              // Replace with your Client ID
    "Scope": "api://<APP_ID>/.default"   // Replace with your App ID
  }
}
```

## Building the Application

### Build for development
```bash
dotnet build
```

### Build for production (self-contained, no .NET runtime needed on target)
```bash
# Linux x64
dotnet publish -c Release -r linux-x64 --self-contained true -o ./publish

# Windows x64
dotnet publish -c Release -r win-x64 --self-contained true -o ./publish
```

## Running the Client

### From source (requires .NET SDK)
```bash
# Produce messages only
dotnet run -- produce

# Consume messages only
dotnet run -- consume

# Both produce and consume
dotnet run -- both
# or simply
dotnet run
```

### From published binary
```bash
# Navigate to publish folder
cd publish

# Run (Linux)
./KafkaOidcClient produce

# Run (Windows)
KafkaOidcClient.exe produce
```

## Troubleshooting

### SSL/Certificate Errors
If you encounter SSL certificate errors:

1. Ensure CA certificates are installed:
   ```bash
   # Ubuntu/Debian
   sudo apt-get install ca-certificates

   # Update certificates
   sudo update-ca-certificates
   ```

2. For self-signed certs, you may need to disable verification (not recommended for production):
   ```csharp
   SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.None
   ```

### librdkafka Native Library Issues
The `Confluent.Kafka` package depends on `librdkafka`. If you see native library errors:

```bash
# Ubuntu/Debian
sudo apt-get install librdkafka-dev

# RHEL/CentOS
sudo yum install librdkafka-devel
```

### Token Acquisition Errors
- Verify `AZURE_CLIENT_SECRET` environment variable is set
- Check that `ClientId` and `TenantId` in `appsettings.json` are correct
- Ensure the Azure App Registration has proper API permissions

### Connection Timeouts
- Verify network connectivity to the Confluent Cloud broker
- Check firewall rules allow outbound connections on port 9092
- Verify the bootstrap server address is correct

### Proxy Configuration
If behind a proxy, set environment variables:
```bash
export HTTP_PROXY=http://proxy:port
export HTTPS_PROXY=http://proxy:port
export NO_PROXY=localhost,127.0.0.1
```

## File Structure
```
dotnet-client/
├── KafkaOidcClient.csproj    # Project file with dependencies
├── appsettings.json          # Configuration settings
├── Program.cs                # Entry point
├── OAuthTokenProvider.cs     # Azure AD token acquisition
├── KafkaProducerService.cs   # Kafka producer
├── KafkaConsumerService.cs   # Kafka consumer
└── EXECUTION.md              # This file
```

## Dependencies Overview for Offline Installation

### Direct NuGet Packages (download these)
1. `Confluent.Kafka.2.3.0.nupkg`
2. `Azure.Identity.1.10.4.nupkg`
3. `Microsoft.Extensions.Configuration.8.0.0.nupkg`
4. `Microsoft.Extensions.Configuration.Json.8.0.0.nupkg`
5. `Microsoft.Extensions.Configuration.EnvironmentVariables.8.0.0.nupkg`

### Transitive Dependencies (also needed)
- Azure.Core
- Microsoft.Identity.Client
- Microsoft.Identity.Client.Extensions.Msal
- System.Text.Json
- Microsoft.Extensions.Configuration.Abstractions
- Microsoft.Extensions.Configuration.FileExtensions
- Microsoft.Extensions.FileProviders.Physical
- Microsoft.Extensions.FileProviders.Abstractions
- Microsoft.Extensions.Primitives

Use `dotnet restore --packages ./local-packages` to download all dependencies at once, then transfer the entire `local-packages` folder to the VM.
