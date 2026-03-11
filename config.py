import os

DATABASE_URL = os.environ.get("DATABASE_URL", "")
FORMSPREE_ENDPOINT = os.environ.get("FORMSPREE_ENDPOINT", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "")

STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PRICE_ID = os.environ.get("STRIPE_PRICE_ID", "")

ST_CLASSIFIEDS_URL = "https://www.stclassifieds.sg/section/sub/list/properties/759"
ST_TELEGRAM_CHAT_ID = os.environ.get("ST_TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID)

CEA_DATASET_ID = "d_07c63be0f37e6e59c07a4ddc2fd87fcb"
CEA_BASE_URL = f"https://api-open.data.gov.sg/v1/public/api/datasets/{CEA_DATASET_ID}"
INITIATE_URL = f"{CEA_BASE_URL}/initiate-download"
POLL_URL = f"{CEA_BASE_URL}/poll-download"

POLL_INTERVAL = 30  # seconds between poll requests (strict rate limit)
POLL_MAX_ATTEMPTS = 20
