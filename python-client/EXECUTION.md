# Python Kafka Client - Execution Guide

## Overview
This Python client connects to Confluent Cloud using Microsoft Entra ID (Azure AD) OAuth/OIDC authentication.

## Prerequisites

### System Requirements
- Python 3.8 or higher
- pip (Python package manager)
- Network access to:
  - Confluent Cloud broker: `lkc-1o1jkv.eastus.azure.private.confluent.cloud:9092`
  - Microsoft login: `login.microsoftonline.com`

### Required Libraries

#### Option 1: Install via pip (requires internet)
```bash
pip install confluent-kafka>=2.3.0 msal>=1.26.0
```

#### Option 2: Install from requirements.txt
```bash
pip install -r requirements.txt
```

#### Option 3: Offline Installation (for restricted VMs)
Download these wheel files on a machine with internet access, then transfer to VM:

1. **confluent-kafka** - Kafka client library
   - Download from: https://pypi.org/project/confluent-kafka/#files
   - Choose the wheel matching your Python version and OS (e.g., `confluent_kafka-2.3.0-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl` for Python 3.10 on Linux x64)

2. **msal** - Microsoft Authentication Library
   - Download from: https://pypi.org/project/msal/#files
   - Also download dependencies:
     - `requests`
     - `PyJWT`
     - `cryptography`

Install offline:
```bash
pip install --no-index --find-links=/path/to/wheels/ confluent-kafka msal
```

### Dependencies Summary Table

| Library | Version | Purpose | PyPI URL |
|---------|---------|---------|----------|
| confluent-kafka | >=2.3.0 | Kafka producer/consumer | https://pypi.org/project/confluent-kafka/ |
| msal | >=1.26.0 | Azure AD token acquisition | https://pypi.org/project/msal/ |
| requests | >=2.0.0 | HTTP client (msal dependency) | https://pypi.org/project/requests/ |
| PyJWT | >=1.0.0 | JWT handling (msal dependency) | https://pypi.org/project/PyJWT/ |
| cryptography | >=2.5 | Crypto operations (msal dependency) | https://pypi.org/project/cryptography/ |

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

### 2. Update config.py
Edit `config.py` and replace the placeholder values:

```python
# Replace these values:
CLIENT_ID = "<APP_ID>"           # Your Azure App Registration Client ID
SCOPE = "api://<APP_ID>/.default" # Your App ID in the scope
IDENTITY_POOL_ID = "<POOL_ID>"   # Your Confluent Identity Pool ID
```

## Running the Client

### Produce Messages
```bash
python producer.py
```

### Consume Messages
```bash
python consumer.py
```
Press `Ctrl+C` to stop consuming.

### Combined Example
```bash
# Produce only
python client_example.py produce

# Consume only
python client_example.py consume

# Both (produce then consume)
python client_example.py both

# Custom topic and message count
python client_example.py produce --topic my-topic --count 20
```

## Troubleshooting

### SSL/Certificate Errors
If you encounter SSL certificate errors, you may need to:

1. Install CA certificates:
   ```bash
   # Ubuntu/Debian
   sudo apt-get install ca-certificates

   # RHEL/CentOS
   sudo yum install ca-certificates
   ```

2. Or specify the CA cert location in config:
   ```python
   config = {
       # ... other config ...
       "ssl.ca.location": "/etc/ssl/certs/ca-certificates.crt",
   }
   ```

### Token Acquisition Errors
- Verify `AZURE_CLIENT_SECRET` environment variable is set
- Check that CLIENT_ID and TENANT_ID are correct
- Ensure the Azure App Registration has proper API permissions

### Connection Timeouts
- Verify network connectivity to the Confluent Cloud broker
- Check firewall rules allow outbound connections on port 9092
- Verify the bootstrap server address is correct

## File Structure
```
python-client/
├── config.py           # Configuration settings
├── oauth_callback.py   # OAuth token callback
├── producer.py         # Kafka producer
├── consumer.py         # Kafka consumer
├── client_example.py   # Combined CLI tool
├── requirements.txt    # Python dependencies
└── EXECUTION.md        # This file
```
