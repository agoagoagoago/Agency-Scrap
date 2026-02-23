import os

DATABASE_URL = os.environ["DATABASE_URL"]
FORMSPREE_ENDPOINT = os.environ.get("FORMSPREE_ENDPOINT", "")

CEA_DATASET_ID = "d_07c63be0f37e6e59c07a4ddc2fd87fcb"
CEA_BASE_URL = f"https://api-open.data.gov.sg/v1/public/api/datasets/{CEA_DATASET_ID}"
INITIATE_URL = f"{CEA_BASE_URL}/initiate-download"
POLL_URL = f"{CEA_BASE_URL}/poll-download"

POLL_INTERVAL = 13  # seconds between poll requests (5 req/min limit)
POLL_MAX_ATTEMPTS = 10
