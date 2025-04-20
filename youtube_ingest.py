import os
import json
import csv
import requests
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")

# Load channel IDs from config file
def load_channel_ids(config_path="config/channel_ids.json"):
    with open(config_path, "r") as file:
        data = json.load(file)
    return data.get("channel_ids", [])

# Fetch videos for a given channel
def fetch_channel_videos(api_key, channel_id):
    videos = []
    base_url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "key": api_key,
        "channelId": channel_id,
        "part": "snippet",
        "order": "date",
        "maxResults": 10
    }

    response = requests.get(base_url, params=params)
    results = response.json()

    for item in results.get("items", []):
        if item["id"]["kind"] == "youtube#video":
            video_id = item["id"]["videoId"]
            title = item["snippet"]["title"]
            published_at = item["snippet"]["publishedAt"]
            description = item["snippet"].get("description", "")

            # Fetch statistics
            stats_url = "https://www.googleapis.com/youtube/v3/videos"
            stats_params = {
                "key": api_key,
                "part": "statistics",
                "id": video_id
            }
            stats_response = requests.get(stats_url, params=stats_params)
            stats_data = stats_response.json()

            statistics = stats_data.get("items", [{}])[0].get("statistics", {})

            video_data = {
                "channel_id": channel_id,
                "video_id": video_id,
                "title": title,
                "published_at": published_at,
                "view_count": statistics.get("viewCount", 0),
                "like_count": statistics.get("likeCount", 0),
                "comment_count": statistics.get("commentCount", 0),
                "description": description
            }

            print(f"DEBUG: {video_data}")  # Helpful debug print
            videos.append(video_data)

    return videos

# Save videos to a CSV file
def save_to_csv(videos, filename="youtube_data.csv"):
    fieldnames = [
        "channel_id", "video_id", "title", "published_at",
        "view_count", "like_count", "comment_count", "description"
    ]

    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for video in videos:
            writer.writerow(video)

# Main script
if __name__ == "__main__":
    if not API_KEY:
        print("❌ API key not found in .env file.")
        exit(1)

    channel_ids = load_channel_ids()
    all_videos = []

    for channel_id in channel_ids:
        print(f"Fetching videos for channel: {channel_id}")
        videos = fetch_channel_videos(API_KEY, channel_id)
        all_videos.extend(videos)

    if all_videos:
        save_to_csv(all_videos)
        print("✅ YouTube data saved to youtube_data.csv")
    else:
        print("⚠️ No video data found.")
