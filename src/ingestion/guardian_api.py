import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("GUARDIAN_API_KEY")
BASE_URL = os.getenv("GUARDIAN_BASE_URL", "https://content.guardianapis.com/search")

def fetch_guardian_articles(from_date=None, to_date=None, page_size=50, max_pages=40, order_by="newest"):
    """
    Fetch articles from The Guardian Open Platform API (with pagination).
    - page_size: max 50 (API limit)
    - max_pages: default 40 (~2,000 articles)
    - from_date/to_date: optional, may be ignored by free tier
    """

    if not API_KEY:
        raise ValueError("Missing GUARDIAN_API_KEY in environment variables.")

    all_results = []
    print(f"🔎 Starting Guardian API fetch | {from_date} → {to_date}")

    for page in range(1, max_pages + 1):
        params = {
            "api-key": API_KEY,
            "order-by": order_by,
            "page-size": page_size,
            "page": page,
            "show-fields": ",".join([
                "headline",
                "trailText",
                "body",
                "byline",
                "publication",
                "thumbnail",
                "wordcount"
            ])
        }

        if from_date:
            params["from-date"] = from_date
        if to_date:
            params["to-date"] = to_date

        try:
            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            results = data.get("response", {}).get("results", [])
            if not results:
                print(f"⚠️ No more results at page {page}. Stopping early.")
                break

            all_results.extend(results)
            print(f"📄 Page {page}: {len(results)} articles fetched (total={len(all_results)})")

            # Respect Guardian rate limits (12 requests/s)
            time.sleep(0.1)

        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching page {page}: {e}")
            break

    print(f"✅ Finished fetching {len(all_results)} articles total.")
    return all_results