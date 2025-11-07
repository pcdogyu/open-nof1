"""
Local configuration for sensitive credentials.

Keep this file out of version control (see .gitignore). Update the values as
needed for your environment.
"""

INFLUX_URL = "http://localhost:8086"
INFLUX_ORG = "hy"
INFLUX_BUCKET = "orderflow"

# Auto-generated InfluxDB token (scope manually configure in Influx UI).
INFLUX_TOKEN = "kf7vIL3tUMSvGNMS4mbyeFpSG9wmM40Adw-sx_2DicV5CXKuGNJp4pPy1i3cyzilUn9--2VoBOND8uGZovj7vA=="

# Optional: admin credentials if needed for scripts (avoid using in production).
INFLUX_USERNAME = "admin"
INFLUX_PASSWORD = "Admin123"

# Supported OKX swap instruments for the default pipeline run.
TRADABLE_INSTRUMENTS = [   'XRP-USDT-SWAP',
    'BNB-USDT-SWAP',
    'BTC-USDT-SWAP',
    'ETH-USDT-SWAP',
    'SOL-USDT-SWAP',
    'DOGE-USDT-SWAP',
    'ADA-USDT-SWAP',
    'LTC-USDT-SWAP']

# Default polling interval (seconds) for the market data pipeline loop.
PIPELINE_POLL_INTERVAL = 120

# Web OKX page cache TTL (seconds) for Influx-backed snapshots.
OKX_CACHE_TTL_SECONDS = 600

# Scheduler defaults (seconds)
MARKET_SYNC_INTERVAL = 60
AI_INTERACTION_INTERVAL = 300

# OKX demo accounts (API keys / secrets / passphrases).
# Populate secrets/passphrases when available.
OKX_ACCOUNTS = {
    "moni": {
        "api_key": "d7f7bde1-b1b7-427b-b97a-37087c8560db",
        "api_secret": "8A831D36FBC2EB7010B3B37D7D495F9A",
        "passphrase": "123456.coM",
        "model_id": "deepseek-v1",
        "account_id": "okx_moni",
        "starting_equity": 10000.0,
        "base_currency": "USDT",
    },
}

DEEPSEEK_API_KEY = "sk-1234567890"

# Placeholder for Qwen / 通义千问 API key (populate with your credential).
QWEN_API_KEY = "QW-1234567890"

# Default model management metadata used by the web dashboard.
MODEL_DEFAULTS = {   'deepseek-v1': {   'display_name': 'DeepSeek 交易模型',
                       'provider': 'DeepSeek',
                       'enabled': True,
                       'api_key': 'sk-1234567890'},
    'qwen-v1': {'display_name': 'Qwen 千问模型', 'provider': '阿里云通义', 'enabled': False, 'api_key': ''},
    'grok-v1': {   'display_name': 'Grok 交易模型',
                   'provider': 'xAI Grok',
                   'enabled': False,
                   'api_key': ''},
    'chatgpt-v1': {   'display_name': 'ChatGPT 交易模型',
                      'provider': 'OpenAI',
                      'enabled': False,
                      'api_key': ''},
    'claude-v1': {   'display_name': 'Claude 交易模型',
                     'provider': 'Anthropic',
                     'enabled': False,
                     'api_key': ''},
    'gemini-v1': {   'display_name': 'Gemini 交易模型',
                     'provider': 'Google DeepMind',
                     'enabled': False,
                     'api_key': ''}}
