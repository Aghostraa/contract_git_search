import requests
import anthropic
import re
import os
import json
import time
import difflib
import urllib.parse
import argparse
from dotenv import load_dotenv
from typing import Dict, List, Optional

# Load environment variables from .env file (if present)
load_dotenv()

# -------------------------------
# Environment & API Configuration
# -------------------------------

# Airtable configuration
AIRTABLE_TOKEN = os.getenv('REACT_APP_AIRTABLE_TOKEN')
AIRTABLE_BASE_URL = 'https://api.airtable.com/v0/appZWDvjvDmVnOici'
TABLE_NAME = 'tblcXnFAf0IEvAQA6'

# GitHub configuration
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
# Use GitHub's text-match preview header to get code snippet fragments
GITHUB_HEADERS = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3.text-match+json'
}

# Anthropic API configuration
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
# Default API URL; override via environment if needed.
ANTHROPIC_API_URL = os.getenv('ANTHROPIC_API_URL', 'https://api.anthropic.com/v1/complete')

# Airtable HTTP headers (used for every request)
AIRTABLE_HEADERS = {
    'Authorization': f'Bearer {AIRTABLE_TOKEN}',
    'Content-Type': 'application/json'
}

# -------------------------------
# Helper Functions
# -------------------------------

def sanitize_hex_addresses(text: str, target_address: str) -> str:
    """Sanitizes Ethereum addresses, replacing target with target_SC and truncating others."""
    pattern = re.compile(r'0x[a-fA-F0-9]{40}')
    target_lower = target_address.lower()
    address_cache = {}

    def replacer(match):
        addr = match.group(0)
        if addr in address_cache:
            return address_cache[addr]

        result = "target_SC" if addr.lower() == target_lower else f"{addr[:6]}...{addr[-4:]}"
        address_cache[addr] = result
        return result

    return pattern.sub(replacer, text)


def compress_snippet(snippet: str, target_marker: str = "target_SC", max_length: int = 300) -> str:
    """Smart snippet compression preserving context around markers."""
    if not snippet:
        return ""

    lines = snippet.splitlines()

    def find_block_bounds(idx: int) -> tuple[int, int]:
        start = max(0, idx - 1)
        end = min(len(lines), idx + 2)

        # Extend to include complete function/class definition
        while start > 0 and lines[start - 1].strip().startswith(('def ', 'class ', 'contract ')):
            start -= 1
        while end < len(lines) and lines[end - 1].strip().endswith((':', '{', '(')):
            end += 1
        return start, end

    # Find target marker with context
    for idx, line in enumerate(lines):
        if target_marker in line:
            start, end = find_block_bounds(idx)
            return "\n".join(lines[start:end])

    # Progressive trimming
    length = 0
    last_break = 0

    for idx, line in enumerate(lines):
        length += len(line) + 1
        if not line.strip() or line.strip().endswith(('}', ')', ']')):
            last_break = idx
        if length > max_length:
            break_idx = last_break if last_break > 0 else idx
            return "\n".join(lines[:break_idx]) + "\n..."

    return snippet


def are_similar(snippet_a: str, snippet_b: str, threshold: float = 0.9) -> bool:
    """Improved code similarity detection."""

    def normalize(text: str) -> str:
        # Remove comments and normalize whitespace
        text = re.sub(r'\/\*.*?\*\/|\/\/.*$', '', text, flags=re.MULTILINE)
        return ' '.join(text.lower().split())

    a_norm = normalize(snippet_a)
    b_norm = normalize(snippet_b)

    if len(a_norm) < 50 or len(b_norm) < 50:
        return a_norm == b_norm

    return difflib.SequenceMatcher(None, a_norm, b_norm).ratio() >= threshold


def deduplicate_snippets(snippets: List[str], min_length: int = 10) -> List[str]:
    """Smart snippet deduplication with content quality scoring."""
    if not snippets:
        return []

    def score_snippet(s: str) -> float:
        length_score = min(len(s) / 1000, 1.0)
        has_contract = bool(re.search(r'(function|contract|class)\s+\w+', s))
        has_target = 'target_SC' in s
        return length_score + (0.5 if has_contract else 0) + (0.3 if has_target else 0)

    valid_snippets = [(s, score_snippet(s)) for s in snippets if len(s.strip()) >= min_length]
    valid_snippets.sort(key=lambda x: x[1], reverse=True)

    unique = []
    seen = set()

    for snippet, _ in valid_snippets:
        key = re.sub(r'\s+', ' ', snippet.strip().lower())
        key = re.sub(r'[\'"].*?[\'"]', '"..."', key)

        if key not in seen and not any(are_similar(snippet, u) for u in unique):
            unique.append(snippet)
            seen.add(key)

    return unique

# Define valid usage categories (from the provided tag definitions


def get_ai_metadata(snippets: List[str], repo_paths: List[str], target_address: str) -> Optional[dict]:
    """Analyzes smart contract code snippets to extract metadata."""
    if not ANTHROPIC_API_KEY:
        print("Missing Anthropic API key.")
        return None

    # Process snippets
    processed_snippets = []
    for snippet in snippets:
        if "abi" in snippet.lower():
            continue

        # Apply transformations
        snippet = snippet.replace("Targer_SC", "target_SC")
        snippet = sanitize_hex_addresses(snippet, target_address)
        snippet = compress_snippet(snippet, max_length=300)
        processed_snippets.append(snippet)

    # Filter and sort repositories
    excluded_repos = {
        "0xtorch/datasource",
        "KeystoneHQ/Smart-Contract-Metadata-Registry",
        "tangtj/bsc-contract-database"
    }
    sorted_repos = sorted(
        [repo for repo in repo_paths if repo not in excluded_repos],
        key=lambda x: len(x.split('/'))  # Prioritize shorter paths
    )[:5]

    # Construct analysis prompt
    prompt = (
        "Analyze these smart contract snippets and repository data for key information:"
        "Focus areas:"
        "1. Contract functionality and inheritance patterns"
        "2. Function signatures and modifiers"
        "3. Protocol integrations and standards (ERC20, ERC721, etc)"
        "4. Repository structure and naming patterns"
        "5. Comments and documentation context"
        "Return ONLY a JSON object:"
        "OUTPUT_START"
        "{"
        '  "contract_name": "<Name from patterns or functionality>",'
        '  "associated_entity": "<Project/Organization/Developer>",'
        '  "usage_category": "<Primary function: Token/DEX/Lending/etc>"'
        "}"
        "OUTPUT_END"
        f"Code Snippets:{chr(10).join(processed_snippets)}"
        f"Repository Paths:{chr(10).join(sorted_repos)}"
    )

    try:
        # Prepare API call
        payload = {
            "model": "claude-3-haiku-20240307",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": prompt}]
        }
        print("\nAPI Payload:", json.dumps(payload, indent=2))

        # Make API call
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(**payload)

        # Extract JSON from response
        content = response.content[0].text if isinstance(response.content, list) else response.content
        start_idx = content.find("OUTPUT_START") + len("OUTPUT_START")
        end_idx = content.find("OUTPUT_END")

        if start_idx == -1 or end_idx == -1:
            print("Response markers not found")
            return None

        return json.loads(content[start_idx:end_idx].strip())

    except Exception as e:
        print(f"API Error: {str(e)}")
        return None


# -------------------------------
# Airtable API Integration
# -------------------------------

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
        """
        Fetch all unprocessed contracts for a specific origin_key.
        Only records with an empty 'repo_count' field are considered unprocessed.
        """
        all_records = []
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
                url = f"{base_url}&offset={offset}" if offset else base_url
                response = requests.get(url, headers=AIRTABLE_HEADERS)
                response.raise_for_status()
                data = response.json()
                records = data.get('records', [])
                all_records.extend(records)
                print(f"[{origin_key}] Page {page}: Fetched {len(records)} records. Total: {len(all_records)}")
                offset = data.get('offset')
                if not offset:
                    break
                page += 1
                time.sleep(0.2)  # Respect Airtable rate limits
            except requests.exceptions.RequestException as e:
                print(f"Error fetching page {page} for {origin_key}: {str(e)}")
                break
        print(f"[{origin_key}] Total unprocessed records: {len(all_records)}")
        return all_records

    @staticmethod
    def update_record(record_id: str, contract_address: str, repos_data: List[Dict],
                      ai_metadata: Optional[Dict] = None, snippets: Optional[List[str]] = None) -> bool:
        """
        Update an Airtable record with GitHub search results and (if available) AI analysis metadata.
        The following fields are updated:
          - github_found: Boolean indicating if any repositories were found.
          - repo_count: Number of repositories found.
          - repo_paths: A newline‑separated list of repository paths.
          - ai_contract_name: The contract name from AI analysis.
          - ai_associated_entity: The company/Git org/creator from AI analysis.
          - ai_usage_category: The usage category (mapped) from AI analysis.
          - ai_snippets: The unique code snippets analyzed.
        """
        url = f"{AIRTABLE_BASE_URL}/{TABLE_NAME}/{record_id}"

        # Collect unique repository names from repos_data.
        repo_paths = list({repo['repo_name'] for repo in repos_data})
        request_body = {
            "fields": {
                "github_found": bool(repos_data),
                "repo_count": len(repos_data),
                "repo_paths": '\n'.join(repo_paths) if repo_paths else ''
            },
            "typecast": True
        }
        # If AI metadata is available, include the details.
        if ai_metadata:
            request_body["fields"].update({
                "ai_contract_name": ai_metadata.get("contract_name", ""),
                "ai_associated_entity": ai_metadata.get("associated_entity", ""),
                "ai_usage_category": ai_metadata.get("usage_category", "")
            })
        # Optionally include the unique snippets.
        if snippets:
            request_body["fields"]["ai_snippets"] = '\n'.join(snippets)

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.patch(url, headers=AIRTABLE_HEADERS, json=request_body, timeout=30)
                response.raise_for_status()
                print(f"Updated record {record_id} with {len(repos_data)} repositories found")
                return True
            except requests.exceptions.RequestException as e:
                print(f"Error updating record {record_id} (Attempt {attempt}/{max_retries}): {str(e)}")
                time.sleep(2 ** attempt)  # Exponential backoff
        return False

# -------------------------------
# GitHub API Integration
# -------------------------------

class GitHubAPI:
    """Handle all GitHub API interactions."""

    @staticmethod
    def search_contract(address: str) -> Dict:
        """
        Search GitHub for a contract address.
        Returns a dictionary with two keys:
          - 'repos_data': a list of repository details (one per unique repo)
          - 'snippets': a list of code snippets (text fragments) found in the search results.
        """
        url = "https://api.github.com/search/code"
        params = {
            "q": address,
            "per_page": 100
        }
        max_retries = 3
        current_retry = 0
        all_repos = {}
        all_snippets = []
        while current_retry < max_retries:
            try:
                response = requests.get(url, headers=GITHUB_HEADERS, params=params, timeout=30)
                # Check for rate limiting; if rate-limited, wait accordingly.
                if response.status_code == 403:
                    remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                    if remaining == 0:
                        sleep_time = 10  # A default wait time; you may parse X-RateLimit-Reset header instead.
                        print(f"GitHub rate limit exceeded. Waiting {sleep_time} seconds...")
                        time.sleep(sleep_time)
                        continue
                response.raise_for_status()
                search_results = response.json()

                for item in search_results.get('items', []):
                    # Extract repository details
                    repo = item.get('repository', {})
                    repo_full_name = repo.get('full_name')
                    if repo_full_name and repo_full_name not in all_repos:
                        all_repos[repo_full_name] = {
                            'repo_name': repo_full_name,
                            'description': repo.get('description'),
                            'stars': repo.get('stargazers_count'),
                            'url': repo.get('html_url'),
                            'file_path': item.get('path'),
                            'file_url': item.get('html_url')
                        }
                    # Extract snippet(s) from text matches if available
                    for match in item.get('text_matches', []):
                        snippet = match.get('fragment')
                        if snippet:
                            all_snippets.append(snippet)
                break  # Exit loop if successful
            except requests.exceptions.RequestException as e:
                current_retry += 1
                print(f"Error searching GitHub (Attempt {current_retry}/{max_retries}): {str(e)}")
                time.sleep(2 ** current_retry)  # Exponential backoff

        # Deduplicate snippets using our helper function.
        unique_snippets = deduplicate_snippets(all_snippets)
        return {"repos_data": list(all_repos.values()), "snippets": unique_snippets}

# -------------------------------
# Main Contract Processing Function
# -------------------------------

TOP_SNIPPETS_COUNT = 10

def process_contracts(origin_key: str):
    """
    Process all unprocessed contracts for a given origin_key:
      1. Retrieve contract records from Airtable.
      2. For each contract:
          - Search GitHub for code snippets matching the contract address.
          - Deduplicate and collect unique snippets.
          - Select only the top snippets.
          - Send the top snippets and repo paths to the Anthropic API for metadata analysis.
          - Update the Airtable record with the GitHub and AI results.
    """
    try:
        records = AirtableAPI.fetch_contracts_by_origin(origin_key)
        if not records:
            print(f"No unprocessed records for origin_key: {origin_key}")
            return

        for index, record in enumerate(records, 1):
            record_id = record.get('id')
            fields = record.get('fields', {})
            contract_address = fields.get('address')
            if not contract_address:
                print(f"Skipping record {record_id} - no contract address found")
                continue

            print(f"Processing {index}/{len(records)}: {contract_address}")
            # Search GitHub for repository details and code snippets.
            search_result = GitHubAPI.search_contract(contract_address)
            repos_data = search_result.get("repos_data", [])
            snippets = search_result.get("snippets", [])

            # Extract unique repository paths (for the AI prompt)
            repo_paths = list({repo['repo_name'] for repo in repos_data})
            # 'snippets' are already deduplicated in GitHubAPI.search_contract
            unique_snippets = snippets

            # --- NEW: Limit to only the top snippets ---
            if unique_snippets:
                # Sort snippets by length (longer ones may provide more context)
                unique_snippets = sorted(unique_snippets, key=len, reverse=True)
                # Select only the top N snippets defined by TOP_SNIPPETS_COUNT
                top_snippets = unique_snippets[:TOP_SNIPPETS_COUNT]
            else:
                top_snippets = []
            # -------------------------------------------

            # Initialize AI metadata to None.
            ai_metadata = None
            # Only perform AI analysis if we have at least one top snippet.
            if top_snippets:
                ai_metadata = get_ai_metadata(top_snippets, repo_paths, target_address=contract_address)
                if ai_metadata:
                    print(f"AI Metadata: {ai_metadata}")
                else:
                    print("AI analysis failed or returned no metadata.")

            # Update the Airtable record with GitHub and (if available) AI results.
            AirtableAPI.update_record(record_id, contract_address, repos_data,
                                        ai_metadata=ai_metadata, snippets=top_snippets)
            # Short delay to help with rate limiting.
            time.sleep(2)

        print(f"Completed processing for origin_key: {origin_key}")

    except Exception as e:
        print(f"Error in process_contracts: {str(e)}")
        raise

# -------------------------------
# Main Entry Point
# -------------------------------

def main():
    """Entry point of the script."""
    try:
        # Check required environment variables.
        if not all([AIRTABLE_TOKEN, GITHUB_TOKEN, ANTHROPIC_API_KEY]):
            raise EnvironmentError("Missing one or more required environment variables.")

        # Parse command-line arguments.
        parser = argparse.ArgumentParser(description="Process contracts for a specific origin_key.")
        parser.add_argument('--origin_key', type=str, required=True, help="The origin_key to process")
        args = parser.parse_args()

        # Process contracts using the provided origin_key.
        process_contracts(args.origin_key)

    except Exception as e:
        print(f"Script failed: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
