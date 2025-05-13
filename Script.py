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
TARGET_VIEW_ID = 'viwx6juMBenBuY6hs'  # MEV Prefilter viwF2Xc24CGNO7u5C, Targeted: viwx6juMBenBuY6hs

# API Headers
AIRTABLE_HEADERS = {
    'Authorization': f'Bearer {AIRTABLE_TOKEN}',
    'Content-Type': 'application/json'
}

GITHUB_HEADERS = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

# List of GitHub repositories to exclude from search results
EXCLUDED_REPOS = [
    "HelayLiu/utils_download",
    "KeystoneHQ/Smart-Contract-Metadata-Registry",
    "tangtj/"
]


class AirtableAPI:
    """Handle all Airtable API interactions."""

    @staticmethod
    def save_no_github_contracts(contract_address: str, record_id: str, origin_key: str = None):
        """Save contracts with no GitHub repositories to a JSON file."""
        filename = 'no_github_contracts.json'
        timestamp = datetime.now().isoformat()
        
        # Create the data structure for this contract
        contract_data = {
            'contract_address': contract_address,
            'record_id': record_id,
            'origin_key': origin_key,
            'timestamp': timestamp
        }
        
        try:
            # Load existing data if file exists
            existing_data = []
            if os.path.exists(filename):
                with open(filename, 'r') as f:
                    existing_data = json.load(f)
            
            # Add new contract data
            existing_data.append(contract_data)
            
            # Save updated data
            with open(filename, 'w') as f:
                json.dump(existing_data, f, indent=2)
                
            print(f"Saved contract {contract_address} to {filename}")
            
        except Exception as e:
            print(f"Error saving to {filename}: {str(e)}")

    @staticmethod
    def get_view_structure(view_id: str = TARGET_VIEW_ID) -> Dict:
        """Fetch basic information about a view to provide context."""
        url = f"{AIRTABLE_BASE_URL}/{TABLE_NAME}?view={view_id}"
        
        try:
            response = requests.get(url, headers=AIRTABLE_HEADERS)
            response.raise_for_status()
            data = response.json()
            
            # Extract basic view information
            records = data.get('records', [])
            
            # Get field names from the first record
            field_names = []
            if records and len(records) > 0:
                field_names = list(records[0].get('fields', {}).keys())
            
            # Create a simple structure summary
            view_structure = {
                'view_id': view_id,
                'total_records': len(records),
                'field_names': field_names,
                'sample_record': records[0].get('fields') if records else None
            }
            
            return view_structure
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching view structure: {str(e)}")
            return {'error': str(e)}

    @staticmethod
    def fetch_all_unprocessed_contracts() -> List[Dict]:
        """Fetch all unprocessed contracts from the target view regardless of origin_key."""
        all_records = []

        # Filter for records where repo_count field doesn't exist or is null
        # This is different from records where repo_count=0, which are already processed
        filter_formula = "NOT(repo_count)"
        base_url = (
            f"{AIRTABLE_BASE_URL}/{TABLE_NAME}"
            f"?view={TARGET_VIEW_ID}"
            f"&filterByFormula={urllib.parse.quote(filter_formula)}"
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

                print(f"Page {page}: Fetched {len(records)} records. Total: {len(all_records)}")

                # Check for more pages
                offset = data.get('offset')
                if not offset:
                    break

                page += 1
                time.sleep(0.2)  # Respect rate limits

            except requests.exceptions.RequestException as e:
                print(f"Error fetching page {page}: {str(e)}")
                break

        print(f"Total unprocessed records (NULL repo_count): {len(all_records)}")
        return all_records

    @staticmethod
    def update_record(record_id: str, github_found: bool, repo_count: int) -> bool:
        """Update an Airtable record with GitHub search results, setting github_found and repo_count."""
        url = f"{AIRTABLE_BASE_URL}/{TABLE_NAME}/{record_id}"

        # Update both github_found and repo_count fields
        # Don't update repo_paths
        request_body = {
            "fields": {
                "github_found": github_found,
                "repo_count": repo_count
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
                status = "FOUND" if github_found else "NOT FOUND"
                print(f"Updated record {record_id} - GitHub repositories: {status} (count: {repo_count})")
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
    def search_contract(address: str) -> (bool, int):
        """Search GitHub for a contract address and return whether any results were found."""
        url = "https://api.github.com/search/code"
        params = {
            "q": address,
            "per_page": 5  # Increased to fetch more items to filter out excluded repos
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
                
                # Filter out results from excluded repositories
                valid_items = []
                for item in search_results.get('items', []):
                    repo_full_name = item.get('repository', {}).get('full_name', '')
                    should_exclude = False
                    
                    for excluded_repo in EXCLUDED_REPOS:
                        if excluded_repo.endswith('/'):  # This is a user/org prefix
                            if repo_full_name.startswith(excluded_repo):
                                should_exclude = True
                                break
                        elif repo_full_name == excluded_repo:  # Exact match
                            should_exclude = True
                            break
                    
                    if not should_exclude:
                        valid_items.append(item)
                
                # Calculate valid count after filtering
                valid_count = len(valid_items)
                
                # If we have partial data (due to GitHub pagination), make a second API call for the total count
                if valid_count > 0 and len(search_results.get('items', [])) >= 100:
                    # For accurate total count, we need to make additional calls
                    # This is a simplified approach - for a complete solution, implement proper pagination
                    print(f"Found more than 100 results for {address}, calculating filtered count...")
                    total_valid_count = GitHubAPI._calculate_filtered_count(address)
                    return (total_valid_count > 0, total_valid_count)
                
                return (valid_count > 0, valid_count)

            except requests.exceptions.RequestException as e:
                current_retry += 1
                print(f"Error searching GitHub (Attempt {current_retry}/{max_retries}): {str(e)}")

                if current_retry < max_retries:
                    time.sleep(2 ** current_retry)  # Exponential backoff
                    continue

                return (False, 0)
    
    @staticmethod
    def _calculate_filtered_count(address: str) -> int:
        """Calculate the total count of results after filtering excluded repositories."""
        # In a real implementation, this would handle pagination properly
        # For now, we'll just use a simple approach with a larger page size
        
        url = "https://api.github.com/search/code"
        params = {
            "q": address,
            "per_page": 5
        }
        
        try:
            response = requests.get(
                url,
                headers=GITHUB_HEADERS,
                params=params,
                verify=True,
                timeout=30
            )
            response.raise_for_status()
            search_results = response.json()
            
            # Filter out results from excluded repositories
            valid_count = 0
            for item in search_results.get('items', []):
                repo_full_name = item.get('repository', {}).get('full_name', '')
                should_exclude = False
                
                for excluded_repo in EXCLUDED_REPOS:
                    if excluded_repo.endswith('/'):  # This is a user/org prefix
                        if repo_full_name.startswith(excluded_repo):
                            should_exclude = True
                            break
                    elif repo_full_name == excluded_repo:  # Exact match
                        should_exclude = True
                        break
                
                if not should_exclude:
                    valid_count += 1
            
            # Note: this is still not handling pagination properly
            # In a production environment, you would continue fetching pages until all results are processed
            return valid_count
            
        except requests.exceptions.RequestException as e:
            print(f"Error calculating filtered count: {str(e)}")
            return 0


def process_all_contracts():
    """Process all unprocessed contracts from the target view."""
    try:
        # Fetch all unprocessed records directly from the view
        records = AirtableAPI.fetch_all_unprocessed_contracts()

        if not records:
            print(f"No unprocessed records with NULL repo_count in view {TARGET_VIEW_ID}")
            return

        # Process each record
        for index, record in enumerate(records, 1):
            record_id = record['id']
            fields = record.get('fields', {})
            contract_address = fields.get('address')
            origin_key = fields.get('origin_key')

            if not contract_address:
                print(f"Skipping record {record_id} - no contract address")
                continue

            print(f"Processing {index}/{len(records)}: {contract_address}")

            # Search GitHub
            github_found, repo_count = GitHubAPI.search_contract(contract_address)

            # If no GitHub repositories found, save to our tracking file
            if not github_found:
                AirtableAPI.save_no_github_contracts(contract_address, record_id, origin_key)

            # Always update Airtable with github_found and repo_count
            # This marks the contract as processed and stores the result
            AirtableAPI.update_record(record_id, github_found, repo_count)

            # Rate limiting delay
            time.sleep(2)

        print(f"Completed processing all contracts")

    except Exception as e:
        print(f"Error in process: {str(e)}")
        raise


def main():
    """Entry point of the script."""
    try:
        # Validate environment variables
        if not all([AIRTABLE_TOKEN, GITHUB_TOKEN]):
            raise EnvironmentError("Missing required environment variables")

        # Set up argument parsing
        parser = argparse.ArgumentParser(description="Process contracts from the target view.")
        group = parser.add_mutually_exclusive_group()
        group.add_argument('--all', action='store_true', help="Process all unprocessed contracts from the target view")
        group.add_argument('--check_view', action='store_true', help="Check the structure of the target view")
        group.add_argument('--list_excluded', action='store_true', help="List the excluded repositories")
        args = parser.parse_args()

        if args.check_view:
            # Check the view structure
            view_structure = AirtableAPI.get_view_structure()
            print(json.dumps(view_structure, indent=2, default=str))
        elif args.list_excluded:
            # List the excluded repositories
            print("Excluded repositories:")
            for repo in EXCLUDED_REPOS:
                print(f"- {repo}")
        else:  # Default to processing all contracts
            process_all_contracts()

    except Exception as e:
        print(f"Script failed: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()