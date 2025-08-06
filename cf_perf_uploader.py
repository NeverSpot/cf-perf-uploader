import os
import time
import math
import random
import hashlib
import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
CF_API_KEY = os.getenv("CF_API_KEY")
CF_API_SECRET = os.getenv("CF_API_SECRET")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def cf_api_call(method, params={}):
    base_url = f"https://codeforces.com/api/{method}"
    params["apiKey"] = CF_API_KEY
    params["time"] = str(int(time.time()))
    sorted_params = sorted(params.items())
    query = "&".join(f"{k}={v}" for k, v in sorted_params)
    rand = str(random.randint(100000, 999999))
    to_hash = f"{rand}/{method}?{query}#{CF_API_SECRET}"
    api_sig = hashlib.sha512(to_hash.encode()).hexdigest()
    full_url = f"{base_url}?{query}&apiSig={rand}{api_sig}"
    response = requests.get(full_url)
    data = response.json()
    if data["status"] != "OK":
        raise Exception(f"CF API error: {data.get('comment')}")
    return data["result"]

def expected_score(r1, r2):
    return 1 / (1 + 10 ** ((r2 - r1) / 400))

def expected_rank(my_rating, others):
    return 0.5 + sum(expected_score(r, my_rating) for r in others)

def compute_performance(actual_rank, all_ratings):
    low, high = 0, 5000
    for _ in range(20):
        mid = (low + high) / 2
        est = expected_rank(mid, all_ratings)
        if est < actual_rank:
            high = mid
        else:
            low = mid
    return round((low + high) / 2)

def fetch_contest_standings(contest_id):
    return cf_api_call("contest.standings", {
        "contestId": contest_id,
        "showUnofficial": "false"
    })["rows"]

def process_and_upload(contest_id, division="Div2"):
    try:
        # Skip if already uploaded
        exists = supabase.table("contest_perf").select("contest_id").eq("contest_id", contest_id).execute()
        if exists.data:
            print(f"✅ Contest {contest_id} already uploaded. Skipping.")
            return

        rows = fetch_contest_standings(contest_id)
        users = []
        all_ratings = []

        for row in rows:
            party = row["party"]
            if party["participantType"] != "CONTESTANT":
                continue
            if len(party["members"]) != 1:
                continue
            handle = party["members"][0]["handle"]
            if "oldRating" not in row:
                continue
            old_rating = row["oldRating"]
            if old_rating is None:
                continue
            rank = row["rank"]

            all_ratings.append(old_rating)
            users.append({
                "handle": handle,
                "rank": rank,
                "old_rating": old_rating
            })

        print(f"→ {contest_id}: Processing {len(users)} users...")
        for user in users:
            user["performance"] = compute_performance(user["rank"], all_ratings)

        supabase.table("contest_perf").upsert({
            "contest_id": contest_id,
            "division": division,
            "data": users
        }).execute()

        print(f"✅ Uploaded contest {contest_id} to Supabase")
        time.sleep(1.5)  # safe delay for rate limits

    except Exception as e:
        print(f"❌ Error with contest {contest_id}: {e}")

def fetch_latest_contests(n=300):
    contests = cf_api_call("contest.list")
    filtered = [c for c in contests if c["phase"] == "FINISHED" and not c["name"].startswith("Unrated")]
    return filtered[:n]

if __name__ == "__main__":
    print("📥 Fetching latest rated contests...")
    recent = fetch_latest_contests(300)
    for contest in reversed(recent):  # Oldest first
        process_and_upload(contest_id=contest["id"])
