import os
from dotenv import load_dotenv
from pathlib import Path

import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger().handlers[0].setLevel(logging.INFO)

env_file = Path(__file__).parent / ".env"
if not env_file.exists():
    raise FileNotFoundError(f"Environment file not found: {env_file}")

logging.info(f"Loading {env_file}")
load_dotenv(dotenv_path=env_file, override=True)

# ────── Azure PostgreSQL ──────
AZURE_POSTGRESQL_DB_HOST = os.getenv("AZURE_POSTGRESQL_DB_HOST")
AZURE_POSTGRESQL_DB_USER = os.getenv("AZURE_POSTGRESQL_DB_USER")
AZURE_POSTGRESQL_DB_PASSWORD = os.getenv("AZURE_POSTGRESQL_DB_PASSWORD")
AZURE_POSTGRESQL_DB_PORT = os.getenv("AZURE_POSTGRESQL_DB_PORT")
AZURE_POSTGRESQL_DB_NAME = os.getenv("AZURE_POSTGRESQL_DB_NAME")

# ────── Slack ──────
SLACK_DEFAULT_GROUP_ID = os.getenv("SLACK_DEFAULT_GROUP_ID")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_DEFAULT_CHANNEL_NAME = os.getenv("SLACK_DEFAULT_CHANNEL_NAME")
SLACK_DEFAULT_WEBHOOK_URL = os.getenv("SLACK_DEFAULT_WEBHOOK_URL")

# ────── POWER──────
# GRIDSTATUS CREDENTIALS
GRIDSTATUS_API_KEY = os.getenv("GRIDSTATUS_API_KEY")

# PJM CREDENTIALS
PJM_API_KEY = os.getenv("PJM_API_KEY")

# ────── WSI ──────
WSI_TRADER_USERNAME = os.getenv("WSI_TRADER_USERNAME")
WSI_TRADER_NAME = os.getenv("WSI_TRADER_NAME")
WSI_TRADER_PASSWORD = os.getenv("WSI_TRADER_PASSWORD")

# ────── METEOLOGICA ──────
# Lower 48 (US48 aggregate) account
XTRADERS_API_USERNAME_L48 = os.getenv("XTRADERS_API_USERNAME_L48")
XTRADERS_API_PASSWORD_L48 = os.getenv("XTRADERS_API_PASSWORD_L48")

# ISO-level account (PJM, ERCOT, MISO, etc.)
XTRADERS_API_USERNAME_ISO = os.getenv("XTRADERS_API_USERNAME_ISO")
XTRADERS_API_PASSWORD_ISO = os.getenv("XTRADERS_API_PASSWORD_ISO")
