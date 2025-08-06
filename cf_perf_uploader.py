import requests
import math
import os
import time
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def expected_score(rating_a, rating_b):
    return 1 / (1 + 10 ** ((rating_b - rating_a) / 400))

def expected_rank(my_rating, others_ratings):
    return 0.5 + sum(expected_score(r, my_rating) for r in others_ratings)

def compute_performance(actual_rank, all_ratings):
    low, high = 0, 5000
    for _ in range(20):
        mid = (low + high) / 2
        exp_rank = expected_rank(mid, all_ratings)
        if exp_rank < actual_rank:
            high = mid
        else:
            low = mid
    return round((low + high) / 2)

def fetch_contest_standings(contest_id):
    url = f"https://codeforces.com/api/contest.standings?contestId={contest_id}&showUnofficial=false"
    response = requests.get(url)
    data = response.json()
    if data["status"] != "OK":
        raise Exception(f"Failed to fetch contest {contest_id}: {data.get('comment')}")
    return data["result"]["rows"]

def process_and_upload(contest_id, division="Div2"):
    try:
        # skip if already uploaded
        existing = supabase.table("contest_perf").select("contest_id").eq("contest_id", contest_id).execute()
        if existing.data:
            print(f"✅ Contest {contest_id} already in DB. Skipping.")
            return

        rows = fetch_contest_standings(contest_id)
        all_ratings = []
        users = []

        for row in rows:
            party = row["party"]
            if party["participantType"] != "CONTESTANT":
                continue
            members = party["members"]
            if len(members) != 1:
                continue
            member = members[0]
            handle = member["handle"]
            if "oldRating" not in row:
                continue
            old_rating = row["oldRating"] = member.get("oldRating", None)
            if old_rating is None:
                continue
            rank = row["rank"]

            all_ratings.append(old_rating)
            users.append({
                "handle": handle,
                "rank": rank,
                "old_rating": old_rating
            })

        print(f"→ {contest_id}: Calculating performance for {len(users)} participants...")
        for user in users:
            user["performance"] = compute_performance(user["rank"], all_ratings)

        payload = {
            "contest_id": contest_id,
            "division": division,
            "data": users
        }

        supabase.table("contest_perf").upsert(payload).execute()
        print(f"✅ Uploaded contest {contest_id}")
        time.sleep(2.5)  # to avoid CF rate limits
    except Exception as e:
        print(f"❌ Error processing contest {contest_id}: {e}")

def fetch_latest_rated_contests(n=300):
    url = "https://codeforces.com/api/contest.list"
    response = requests.get(url)
    contests = response.json()["result"]
    filtered = [c for c in contests if c["phase"] == "FINISHED" and not c["name"].startswith("Unrated")]
    return filtered[:n]

if __name__ == "__main__":
    print("Fetching latest rated Codeforces contests...")
    latest_contests = fetch_latest_rated_contests(300)
    for contest in reversed(latest_contests):  # Oldest first
        process_and_upload(contest_id=contest["id"])
