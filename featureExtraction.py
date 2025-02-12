#!/usr/bin/env python3

import csv
import os
import time
import requests
from datetime import datetime
from dotenv import load_dotenv

# --------------------------------
# 1. Load environment variables
# --------------------------------
load_dotenv()  # Looks for a .env file in the current working directory
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

if not GITHUB_TOKEN:
    print("Error: GITHUB_TOKEN environment variable not set.")
    exit(1)

# --------------------------------
# Configuration
# --------------------------------
CONTRACTS_CSV = "contracts.csv"
CONTRACT_REFERENCES_CSV = "contract_references.csv"

# GitHub search endpoint
GITHUB_SEARCH_URL = "https://api.github.com/search/code"

# Headers for GitHub (with text-match for snippet data)
GITHUB_HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3.text-match+json"
}

# Rate limit / throttle
REQUEST_TIMEOUT = 30
RATE_LIMIT_WAIT_SECS = 10
SLEEP_BETWEEN_SEARCHES = 2


def load_contracts_from_csv(csv_path):
    """
    Load a list of contracts from a CSV file.
    CSV columns (example):
      - address
      - origin_key
      - name
      - owner_project
      - usage_category
    """
    contracts = []
    try:
        with open(csv_path, mode="r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                contract = {
                    "address": row.get("address", "").strip(),
                    "origin_key": row.get("origin_key", "").strip(),
                    "name": row.get("name", "").strip(),
                    "owner_project": row.get("owner_project", "").strip(),
                    "usage_category": row.get("usage_category", "").strip(),
                }
                # Only add if there's an address
                if contract["address"]:
                    contracts.append(contract)

    except FileNotFoundError:
        print(f"Error: CSV file {csv_path} not found.")
        exit(1)

    return contracts


def search_github_for_address(address):
    """
    Searches GitHub for a specific contract address
    and returns a list of references (dicts) with snippet_text included.
    """
    params = {
        "q": address,
        "per_page": 100  # GitHub max is 100 results per page.
    }

    retry_count = 0
    max_retries = 3

    while retry_count < max_retries:
        try:
            response = requests.get(
                GITHUB_SEARCH_URL,
                headers=GITHUB_HEADERS,
                params=params,
                timeout=REQUEST_TIMEOUT
            )

            # Handle rate limiting
            if response.status_code == 403:
                remaining = int(response.headers.get("X-RateLimit-Remaining", 0))
                if remaining == 0:
                    print(f"[Rate Limit] Exceeded. Sleeping for {RATE_LIMIT_WAIT_SECS}s...")
                    time.sleep(RATE_LIMIT_WAIT_SECS)
                    continue

            response.raise_for_status()
            data = response.json()
            items = data.get("items", [])

            references = []
            seen_repos = set()

            for item in items:
                repo = item["repository"]
                repo_full_name = repo["full_name"]

                # Gather snippet text from text matches
                text_matches = item.get("text_matches", [])
                snippet_text = ""
                if text_matches:
                    # Grab the first match's "fragment"
                    snippet_text = text_matches[0].get("fragment", "")
                    # Optionally gather multiple matches:
                    # snippet_text = "\n---\n".join(tm.get("fragment", "") for tm in text_matches)

                # Avoid duplicates by checking the repo_full_name
                if repo_full_name not in seen_repos:
                    references.append({
                        "repo_full_name": repo_full_name,
                        "file_path": item.get("path", ""),
                        "file_url": item.get("html_url", ""),
                        "repo_description": repo.get("description", ""),
                        "repo_stars": repo.get("stargazers_count", 0),
                        "snippet_text": snippet_text,
                    })
                    seen_repos.add(repo_full_name)

            return references

        except requests.exceptions.RequestException as e:
            print(f"[Error] GitHub search for {address}, retry {retry_count + 1}/{max_retries}: {e}")
            retry_count += 1
            # Exponential backoff
            time.sleep(2 ** retry_count)

    return []


def append_references_to_csv(csv_path, contract, references):
    """
    Append references found for a given contract to the output CSV,
    including snippet_text.
    """
    file_exists = os.path.isfile(csv_path)

    fieldnames = [
        "address",
        "origin_key",
        "name",
        "owner_project",
        "usage_category",
        "repo_full_name",
        "file_path",
        "file_url",
        "repo_description",
        "repo_stars",
        "snippet_text",
        "timestamp"
    ]

    with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        for ref in references:
            writer.writerow({
                "address": contract["address"],
                "origin_key": contract["origin_key"],
                "name": contract["name"],
                "owner_project": contract["owner_project"],
                "usage_category": contract["usage_category"],
                "repo_full_name": ref["repo_full_name"],
                "file_path": ref["file_path"],
                "file_url": ref["file_url"],
                "repo_description": ref["repo_description"],
                "repo_stars": ref["repo_stars"],
                "snippet_text": ref["snippet_text"],
                "timestamp": datetime.utcnow().isoformat()
            })


def main():
    # 1. Load contracts from CSV
    contracts = load_contracts_from_csv(CONTRACTS_CSV)
    if not contracts:
        print("No contracts loaded from CSV.")
        return

    # 2. Iterate over contracts, search GitHub, and append results
    for idx, contract in enumerate(contracts, start=1):
        address = contract["address"]
        print(f"{idx}/{len(contracts)} - Searching GitHub for address: {address}")

        references = search_github_for_address(address)

        if references:
            append_references_to_csv(CONTRACT_REFERENCES_CSV, contract, references)
            print(f"  Found and wrote {len(references)} references for {address}.")
        else:
            print(f"  No references found for {address}.")

        # Sleep a bit between searches to avoid secondary rate-limit issues
        time.sleep(SLEEP_BETWEEN_SEARCHES)


if __name__ == "__main__":
    main()
