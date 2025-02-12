import requests
import os
import json
from dotenv import load_dotenv
import time
from typing import Dict, List, Optional
from datetime import datetime
import urllib.parse
import argparse  # Import argparse for command-line arguments

# Load environment variables
load_dotenv()

# API Configuration
AIRTABLE_TOKEN = os.getenv('REACT_APP_AIRTABLE_TOKEN')
AIRTABLE_BASE_URL = 'https://api.airtable.com/v0/appZWDvjvDmVnOici'
TABLE_NAME = 'tblcXnFAf0IEvAQA6'
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')

# API Headers
AIRTABLE_HEADERS = {
    'Authorization': f'Bearer {AIRTABLE_TOKEN}',
    'Content-Type': 'application/json'
}

GITHUB_HEADERS = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}


class AirtableAPI:
    """Handle all Airtable API interactions."""

    @staticmethod
    def get_origin_keys() -> List[str]:
        """Fetch all unique origin_keys from Airtable."""
        url = f"{AIRTABLE_BASE_URL}/{TABLE_NAME}?fields[]=origin_key"

        try:
            response = requests.get(url, headers=AIRTABLE_HEADERS)
            response.raise_for_status()
            data = response.json()

            # Extract and deduplicate origin_keys
            origin_keys = {
                record['fields'].get('origin_key')
                for record in data.get('records', [])
                if record.get('fields', {}).get('origin_key')
            }

            return sorted(list(origin_keys))

        except requests.exceptions.RequestException as e:
            print(f"Error fetching origin keys: {str(e)}")
            return []

    @staticmethod
    def fetch_contracts_by_origin(origin_key: str) -> List[Dict]:
        """Fetch all unprocessed contracts for a specific origin_key."""
        all_records = []

        # Filter for records with empty repo_count
        filter_formula = f"AND(origin_key = '{origin_key}', repo_count = '')"
        base_url = (
            f"{AIRTABLE_BASE_URL}/{TABLE_NAME}"
            f"?filterByFormula={urllib.parse.quote(filter_formula)}"
            f"&fields[]=address&fields[]=origin_key&fields[]=repo_count"
        )

        offset = None
        page = 1

        while True:
            try:
                # Add offset for pagination if it exists
                url = f"{base_url}&offset={offset}" if offset else base_url
                response = requests.get(url, headers=AIRTABLE_HEADERS)
                response.raise_for_status()

                data = response.json()
                records = data.get('records', [])
                all_records.extend(records)

                print(f"[{origin_key}] Page {page}: Fetched {len(records)} records. Total: {len(all_records)}")

                # Check for more pages
                offset = data.get('offset')
                if not offset:
                    break

                page += 1
                time.sleep(0.2)  # Respect rate limits

            except requests.exceptions.RequestException as e:
                print(f"Error fetching page {page} for {origin_key}: {str(e)}")
                break

        print(f"[{origin_key}] Total unprocessed records: {len(all_records)}")
        return all_records

    @staticmethod
    def update_record(record_id: str, contract_address: str, repos_data: List[Dict]) -> bool:
        """Update an Airtable record with GitHub search results."""
        url = f"{AIRTABLE_BASE_URL}/{TABLE_NAME}/{record_id}"

        # Get unique repository paths
        repo_paths = list(set(repo['repo_name'] for repo in repos_data))

        request_body = {
            "fields": {
                "github_found": bool(repos_data),
                "repo_count": len(repos_data),
                "repo_paths": '\n'.join(repo_paths) if repo_paths else ''
            },
            "typecast": True
        }

        max_retries = 3
        current_retry = 0

        while current_retry < max_retries:
            try:
                response = requests.patch(
                    url,
                    headers=AIRTABLE_HEADERS,
                    json=request_body,
                    verify=True,
                    timeout=30
                )
                response.raise_for_status()
                print(f"Updated record {record_id} with {len(repos_data)} repositories found")
                return True

            except requests.exceptions.RequestException as e:
                current_retry += 1
                print(f"Error updating record {record_id} (Attempt {current_retry}/{max_retries}): {str(e)}")

                if current_retry < max_retries:
                    time.sleep(2 ** current_retry)  # Exponential backoff
                    continue

                return False


class GitHubAPI:
    """Handle all GitHub API interactions."""

    @staticmethod
    def search_contract(address: str) -> List[Dict]:
        """Search GitHub for a contract address."""
        url = "https://api.github.com/search/code"
        params = {
            "q": address,
            "per_page": 100
        }

        max_retries = 3
        current_retry = 0

        while current_retry < max_retries:
            try:
                response = requests.get(
                    url,
                    headers=GITHUB_HEADERS,
                    params=params,
                    verify=True,
                    timeout=30
                )

                # Handle rate limiting
                if response.status_code == 403:
                    remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                    if remaining == 0:
                        # Wait 10 seconds by default or use a dynamic time based on headers
                        sleep_time = 10
                        print(f"Rate limit exceeded. Waiting {sleep_time} seconds...")
                        time.sleep(sleep_time)
                        continue

                response.raise_for_status()
                search_results = response.json()

                # Process results
                repos_data = []
                seen_repos = set()

                for item in search_results.get('items', []):
                    repo = item['repository']
                    repo_full_name = repo['full_name']

                    if repo_full_name not in seen_repos:
                        repos_data.append({
                            'repo_name': repo_full_name,
                            'description': repo.get('description'),
                            'stars': repo.get('stargazers_count'),
                            'url': repo['html_url'],
                            'file_path': item['path'],
                            'file_url': item['html_url']
                        })
                        seen_repos.add(repo_full_name)

                return repos_data

            except requests.exceptions.RequestException as e:
                current_retry += 1
                print(f"Error searching GitHub (Attempt {current_retry}/{max_retries}): {str(e)}")

                if current_retry < max_retries:
                    time.sleep(2 ** current_retry)  # Exponential backoff
                    continue

                return []


def process_contracts(origin_key: str):
    """Main function to process contracts by origin_key."""
    try:
        # Fetch unprocessed records for the specified origin
        records = AirtableAPI.fetch_contracts_by_origin(origin_key)

        if not records:
            print(f"No unprocessed records for {origin_key}")
            return

        # Process each record
        for index, record in enumerate(records, 1):
            record_id = record['id']
            fields = record.get('fields', {})
            contract_address = fields.get('address')

            if not contract_address:
                print(f"Skipping record {record_id} - no contract address")
                continue

            print(f"Processing {index}/{len(records)}: {contract_address}")

            # Search GitHub
            repos_data = GitHubAPI.search_contract(contract_address)

            # Update Airtable
            AirtableAPI.update_record(record_id, contract_address, repos_data)

            # Rate limiting delay
            time.sleep(2)

        print(f"Completed processing {origin_key}")

    except Exception as e:
        print(f"Error in main process: {str(e)}")
        raise


def main():
    """Entry point of the script."""
    try:
        # Validate environment variables
        if not all([AIRTABLE_TOKEN, GITHUB_TOKEN]):
            raise EnvironmentError("Missing required environment variables")

        # Set up argument parsing
        parser = argparse.ArgumentParser(description="Process contracts for a specific origin_key.")
        parser.add_argument('--origin_key', type=str, required=True, help="The origin_key to process")
        args = parser.parse_args()

        # Run the main process with the specified origin_key
        process_contracts(args.origin_key)

    except Exception as e:
        print(f"Script failed: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()