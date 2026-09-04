[![MseeP.ai Security Assessment Badge](https://mseep.net/pr/dragon1086-kospi-kosdaq-stock-server-badge.png)](https://mseep.ai/app/dragon1086-kospi-kosdaq-stock-server)

# kospi-kosdaq-stock-server

[![PyPI version](https://badge.fury.io/py/kospi-kosdaq-stock-server.svg)](https://badge.fury.io/py/kospi-kosdaq-stock-server)
[![LightNow](https://lightnow.ai/badge/io.github.dragon1086/kospi-kosdaq-stock-server)](https://lightnow.ai/servers/io.github.dragon1086/kospi-kosdaq-stock-server)

<a href="https://glama.ai/mcp/servers/i1judi5h55">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/i1judi5h55/badge" />
</a>

An MCP server that provides KOSPI/KOSDAQ stock data from KRX Data Marketplace.

## What's New in v0.3.0

Since December 27, 2024, KRX Data Marketplace requires **Kakao/Naver login** for data access. This version implements:

- **Direct KRX API integration** with Kakao OAuth login
- **Playwright-based headless browser** for authentication
- **Automatic session management** with 4-hour timeout and auto re-login
- **No more pykrx dependency** for core functionality

## Features

- Lookup KOSPI/KOSDAQ ticker symbols and names
- Retrieve OHLCV (Open/High/Low/Close/Volume) data for stocks
- Retrieve market capitalization data
- Retrieve fundamental data (PER/PBR/Dividend Yield)
- Retrieve trading volume by investor type (institutional, foreign, individual)
- Retrieve index OHLCV data (KOSPI, KOSDAQ indices)

## Requirements

- Python 3.10+
- Kakao account (2FA must be disabled)
- Playwright Chromium browser

## Environment Variables

```bash
# Required: Kakao login credentials
KAKAO_ID=your_kakao_id
KAKAO_PW=your_kakao_password
```

> **Important**: Your Kakao account must have 2-step verification (2FA) disabled.
> On first login, you may need to approve the login request via KakaoTalk.

## Installation

### Prerequisites

```bash
# Install Playwright and Chromium browser
pip install playwright
playwright install chromium
```

### Installing via Smithery

```bash
npx -y @smithery/cli install @dragon1086/kospi-kosdaq-stock-server --client claude
```

### Manual Installation

```bash
# Create and activate a virtual environment
uv venv .venv
source .venv/bin/activate  # On Unix/macOS
# .venv\Scripts\activate   # On Windows

# Install the package
uv pip install kospi-kosdaq-stock-server

# Install Playwright browser
playwright install chromium
```

## Configuration for Claude Desktop

### macOS

1. Open the config file:
```bash
code ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

2. Add the server configuration:
```json
{
  "mcpServers": {
    "kospi-kosdaq": {
      "command": "uvx",
      "args": ["kospi_kosdaq_stock_server"],
      "env": {
        "KAKAO_ID": "your_kakao_id",
        "KAKAO_PW": "your_kakao_password"
      }
    }
  }
}
```

### Windows

1. Open the config file at `%APPDATA%/Claude/claude_desktop_config.json`
2. Add the same configuration as above

3. Restart Claude Desktop

## Available Tools

### `load_all_tickers`
Loads all ticker symbols and names for KOSPI and KOSDAQ.
- No arguments required
- Returns: Dictionary mapping ticker codes to stock names

### `get_stock_ohlcv`
Retrieves OHLCV (Open/High/Low/Close/Volume) data for a specific stock.
- `fromdate` (string, required): Start date (YYYYMMDD)
- `todate` (string, required): End date (YYYYMMDD)
- `ticker` (string, required): Stock ticker symbol (e.g., "005930")
- `adjusted` (boolean, optional): Use adjusted prices (default: True)

### `get_stock_market_cap`
Retrieves market capitalization data for a specific stock.
- `fromdate` (string, required): Start date (YYYYMMDD)
- `todate` (string, required): End date (YYYYMMDD)
- `ticker` (string, required): Stock ticker symbol

### `get_stock_fundamental`
Retrieves fundamental data (PER/PBR/Dividend Yield) for a specific stock.
- `fromdate` (string, required): Start date (YYYYMMDD)
- `todate` (string, required): End date (YYYYMMDD)
- `ticker` (string, required): Stock ticker symbol

### `get_stock_trading_volume`
Retrieves trading volume by investor type for a specific stock.
- `fromdate` (string, required): Start date (YYYYMMDD)
- `todate` (string, required): End date (YYYYMMDD)
- `ticker` (string, required): Stock ticker symbol
- `detail` (boolean, optional): If true, returns 12 investor types; if false (default), returns 5 aggregated types

### `get_index_ohlcv`
Retrieves OHLCV data for market indices.
- `fromdate` (string, required): Start date (YYYYMMDD)
- `todate` (string, required): End date (YYYYMMDD)
- `ticker` (string, required): Index ticker (e.g., "1001" for KOSPI, "2001" for KOSDAQ)
- `freq` (string, optional): Frequency - "d" (daily), "m" (monthly), "y" (yearly). Default: "d"

## Available Resources

### `stock://tickers`
Returns all KOSPI/KOSDAQ ticker symbols and names.

### `stock://index-tickers`
Returns index ticker information:
- KOSPI: 1001, KOSPI 200: 1028, KOSPI 100: 1034, KOSPI 50: 1035
- KOSDAQ: 2001, KOSDAQ 150: 2203

### `stock://data-sources`
Returns current data source status.

## Docker Support

### Using Docker Compose

```bash
# Build and run
docker-compose up -d

# View logs
docker-compose logs -f
```

### Environment Variables for Docker

Create a `.env` file:
```bash
KAKAO_ID=your_kakao_id
KAKAO_PW=your_kakao_password
```

## Troubleshooting

### "KakaoTalk login notification" popup

On first login, Kakao may require approval via KakaoTalk:
1. Run with `headless=False` to see the browser
2. Approve the login in KakaoTalk
3. Cookies will be saved for future sessions

### 401 Unauthorized / Session Expired

Session expires after ~4 hours. The server auto-renews, but if it fails:
1. Delete `~/.krx_session.json`
2. Restart the server

### Linux Headless Environment

```bash
# Install required packages on Ubuntu/Debian
apt-get install -y libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                MCP Server (FastMCP)                 │
│              kospi_kosdaq_stock_server.py           │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│               KRXDataClient                         │
│  - get_market_ohlcv()                               │
│  - get_market_cap()                                 │
│  - get_market_fundamental()                         │
│  - get_market_trading_volume_by_date()              │
│  - get_index_ohlcv()                                │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│             KakaoAuthManager                        │
│  - Playwright headless browser                      │
│  - Kakao OAuth login                                │
│  - Session cookie management                        │
│  - Auto re-login on session expiry (4h)             │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│           KRX Data Marketplace                      │
│             data.krx.co.kr                          │
└─────────────────────────────────────────────────────┘
```

## Known Limitations

- Kakao accounts with 2FA enabled are not supported
- First login may require KakaoTalk approval
- Session validity: ~4 hours (auto-renewal supported)
- Naver login is not yet implemented

## Usage Example

```
Human: Please load all available stock tickers.
Assistant: I'll load all KOSPI and KOSDAQ stock tickers.

> Using tool 'load_all_tickers'...
Successfully loaded 2,738 stock tickers.
```

```
Human: Show me Samsung Electronics' stock data for December 2024.
Assistant: I'll retrieve Samsung Electronics' (005930) OHLCV data.

> Using tool 'get_stock_ohlcv'...
Date        Open      High      Low       Close     Volume
2024-12-20  53,800    54,200    53,500    53,900    8,234,521
2024-12-19  54,000    54,300    53,700    53,800    7,123,456
...
```

## License

MIT License

## Contributing

Issues and pull requests are welcome!

## Changelog

### v0.3.0 (2025-01-04)
- **Breaking**: Removed pykrx dependency for core functionality
- Added KRX Data Marketplace direct integration with Kakao OAuth
- Added Playwright-based headless authentication
- Added automatic session management
- Added index OHLCV support

### v0.2.x
- pykrx-based implementation (deprecated due to KRX login requirement)
