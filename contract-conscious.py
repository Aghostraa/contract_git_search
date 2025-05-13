# %%
import requests
import os
import json
from dotenv import load_dotenv
import time
from typing import Dict, List, Optional
from datetime import datetime
import urllib.parse
import argparse
from typing import Tuple

# %%
load_dotenv()

# %%
# API Configuration
AIRTABLE_TOKEN = os.getenv('REACT_APP_AIRTABLE_TOKEN')
AIRTABLE_BASE_URL = 'https://api.airtable.com/v0/appZWDvjvDmVnOici'
TABLE_NAME = 'tblcXnFAf0IEvAQA6'
TARGET_VIEW_ID = 'viwF2Xc24CGNO7u5C'  # MEV Prefilter viwF2Xc24CGNO7u5C, Targeted: viwx6juMBenBuY6hs
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

# List of GitHub repositories to exclude from search results
EXCLUDED_REPOS = [
    "HelayLiu/utils_download",
    "KeystoneHQ/Smart-Contract-Metadata-Registry",
    "tangtj/"
]

# List of supported blockchain chains and their IDs
SUPPORTED_CHAINS = {
    "base": 8453,
    "ethereum": 1,
    "optimism": 10,
    "arbitrum": 42161,
}

# Check for missing configuration
if not all([AIRTABLE_TOKEN, GITHUB_TOKEN]):
    print("WARNING: Missing AIRTABLE_TOKEN or GITHUB_TOKEN environment variable!")

print("Configuration loaded.")

# %%
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
                    try:
                        existing_data = json.load(f)
                    except json.JSONDecodeError:
                        print(f"Warning: {filename} contains invalid JSON. Initializing as empty list.")
                        existing_data = []
            
            # Ensure existing_data is a list
            if not isinstance(existing_data, list):
                print(f"Warning: {filename} does not contain a list. Initializing as empty list.")
                existing_data = []
                
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
            response = requests.get(url, headers=AIRTABLE_HEADERS, timeout=30)
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
                'total_records_in_response': len(records),
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
        filter_formula = "NOT({repo_count})"
        base_url = (
            f"{AIRTABLE_BASE_URL}/{TABLE_NAME}"
            f"?view={TARGET_VIEW_ID}"
            f"&filterByFormula={urllib.parse.quote(filter_formula)}"
            f"&fields[]=address&fields[]=origin_key&fields[]=repo_count&fields[]=chain"
        )

        offset = None
        page = 1

        while True:
            try:
                # Add offset for pagination if it exists
                url = f"{base_url}&offset={offset}" if offset else base_url
                print(f"Fetching page {page}: {url}")
                response = requests.get(url, headers=AIRTABLE_HEADERS, timeout=30)
                
                if response.status_code != 200:
                    print(f"Airtable API Error ({response.status_code}): {response.text}")
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
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON from Airtable on page {page}: {str(e)}")
                print(f"Response text: {response.text}")
                break

        print(f"Total unprocessed records (NULL repo_count): {len(all_records)}")
        return all_records

    @staticmethod
    def update_record(record_id: str, github_found: bool, repo_count: int, 
                     transaction_data: Optional[Dict] = None) -> bool:
        """Update an Airtable record with GitHub search results and optional transaction data."""
        url = f"{AIRTABLE_BASE_URL}/{TABLE_NAME}/{record_id}"

        # Create the fields dictionary with github_found and repo_count
        fields = {
            "github_found": github_found,
            "repo_count": repo_count
        }
        
        # Add transaction data if provided
        if transaction_data:
            try:
                fields["transaction_data"] = json.dumps(transaction_data)
            except TypeError as e:
                print(f"Error serializing transaction_data for record {record_id}: {str(e)}")
                return False

        request_body = {
            "fields": fields,
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
                
                if response.status_code != 200:
                    print(f"Airtable Update Error ({response.status_code}) for record {record_id}: {response.text}")
                response.raise_for_status()
                
                status = "FOUND" if github_found else "NOT FOUND"
                print(f"Updated record {record_id} - GitHub repositories: {status} (count: {repo_count})")
                if transaction_data:
                    print(f"Added transaction data for {record_id} with {len(transaction_data.get('transactions', []))} transactions")
                return True

            except requests.exceptions.RequestException as e:
                current_retry += 1
                print(f"Error updating record {record_id} (Attempt {current_retry}/{max_retries}): {str(e)}")

                if current_retry < max_retries:
                    time.sleep(2 ** current_retry)  # Exponential backoff
                    continue

                return False

print("AirtableAPI class defined.")

# %%
class GitHubAPI:
    """Handle all GitHub API interactions."""

    @staticmethod
    def search_contract(address: str) -> Tuple[bool, int]:
        """Search GitHub for a contract address and return whether any results were found."""
        url = "https://api.github.com/search/code"
        params = {
            "q": address,
            "per_page": 10  # Use larger page size to minimize pagination
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
                    reset_time = int(response.headers.get('X-RateLimit-Reset', time.time()))
                    if remaining == 0:
                        sleep_time = max(1, reset_time - int(time.time())) + 5  # Add buffer
                        print(f"GitHub rate limit exceeded. Waiting {sleep_time} seconds...")
                        time.sleep(sleep_time)
                        continue

                if response.status_code != 200:
                    print(f"GitHub API Error ({response.status_code}): {response.text}")
                response.raise_for_status()
                search_results = response.json()
                
                # Filter out results from excluded repositories
                valid_items = []
                excluded_count = 0
                for item in search_results.get('items', []):
                    repo_full_name = item.get('repository', {}).get('full_name', '')
                    if not repo_full_name:
                        continue
                        
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
                    else:
                        excluded_count += 1
                
                valid_count = len(valid_items)
                total_api_count = search_results.get('total_count', 0)
                print(f"GitHub search for {address}: API found {total_api_count}, Valid found: {valid_count}, Excluded: {excluded_count}")
                
                return (valid_count > 0, valid_count)

            except requests.exceptions.RequestException as e:
                current_retry += 1
                print(f"Error searching GitHub (Attempt {current_retry}/{max_retries}): {str(e)}")

                if current_retry < max_retries:
                    time.sleep(2 ** current_retry)  # Exponential backoff
                    continue

                return (False, 0)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON from GitHub: {str(e)}")
                print(f"Response text: {response.text}")
                return (False, 0)

print("GitHubAPI class defined.")

# %%
# Improved BlockscoutAPI with proper auth headers and detailed response logging
import requests
import pandas as pd
import logging
import json
import time
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('BlockscoutAPI')

class BlockscoutAPI:
    def __init__(self, debug=True):
        self.debug = debug
        
        # Load API keys from environment variables
        self.api_keys = {
            1: os.getenv("BLOCKSCAPE_API_KEY_ETH"),
            10: os.getenv("BLOCKSCAPE_API_KEY_OPTIMISM"),
            324: os.getenv("BLOCKSCAPE_API_KEY_ZKSYNC"),
            8453: os.getenv("BLOCKSSCOUT_API_KEY_Base"),
            42161: os.getenv("BLOCKSCAPE_API_KEY_ARBITRUM")
        }
        
        # Official URLs for each chain - verified from official documentation
        self.chain_config = {
            1: {
                "name": "Ethereum",
                "urls": ["https://eth.blockscout.com/api/v2/"],
                "auth_type": "header"  # Blockscout uses header-based auth
            },
            10: {
                "name": "Optimism",
                "urls": ["https://optimism.blockscout.com/api/v2/"],
                "auth_type": "header"
            },
            324: {
                "name": "zkSync Era",
                "urls": ["https://zksync.blockscout.com/api/v2/"],
                "auth_type": "header"
            },
            8453: {
                "name": "Base",
                "urls": [
                    "https://base.blockscout.com/api/v2/",
                    "https://api.basescan.org/api"  # Etherscan-style API uses query param auth
                ],
                "auth_type": "header"  # Will change dynamically based on URL
            },
            42161: {
                "name": "Arbitrum One",
                "urls": ["https://arbitrum.blockscout.com/api/v2/"],
                "auth_type": "header"
            },
            1101: {
                "name": "Mode",
                "urls": ["https://explorer.mode.network/api/v2/"],
                "auth_type": "header"
            },
            59144: {
                "name": "Linea",
                "urls": ["https://explorer.linea.build/api/v2/"],
                "auth_type": "header"
            },
            
            
            
        }
        
        # Configure session with retries
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504, 524],
            allowed_methods=["GET"]
        )
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        
        # Log API key status
        for chain_id, key in self.api_keys.items():
            if key:
                logger.info(f"API key for {self.chain_config[chain_id]['name']} (ID: {chain_id}) is available")
            else:
                logger.warning(f"No API key found for {self.chain_config[chain_id]['name']} (ID: {chain_id})")
    
    def log_request(self, url, response, chain_id=None):
        """Log request and response details with proper masking of API keys"""
        if self.debug:
            # Mask API key in URL if present
            safe_url = url
            chain_name = self.chain_config[chain_id]['name'] if chain_id in self.chain_config else "Unknown"
            
            for key in self.api_keys.values():
                if key and key in safe_url:
                    safe_url = safe_url.replace(key, "API_KEY_HIDDEN")
                    
            logger.info(f"[{chain_name}] Request URL: {safe_url}")
            logger.info(f"[{chain_name}] Response Status: {response.status_code}")
            
            # Log headers (masking authorization)
            safe_headers = dict(response.request.headers)
            if 'Authorization' in safe_headers:
                safe_headers['Authorization'] = 'Bearer API_KEY_HIDDEN'
            logger.info(f"[{chain_name}] Request Headers: {json.dumps(safe_headers)}")
            
            # Log complete response (but limit very large responses)
            try:
                data = response.json()
                response_str = json.dumps(data, indent=2)
                
                # If response is very large, trim it
                if len(response_str) > 2000:
                    logger.info(f"[{chain_name}] Response (truncated): {response_str[:2000]}...[truncated]")
                else:
                    logger.info(f"[{chain_name}] Complete Response: {response_str}")
                    
            except:
                # If not JSON, log text response
                text = response.text
                if len(text) > 2000:
                    logger.info(f"[{chain_name}] Response Text (truncated): {text[:2000]}...[truncated]")
                else:
                    logger.info(f"[{chain_name}] Complete Response Text: {text}")
    
    def make_request(self, url, chain_id, timeout=10):
        """Make a request with proper authentication headers and error handling"""
        headers = {}
        
        # Determine auth type based on URL and chain config
        auth_type = self.chain_config[chain_id]['auth_type']
        
        # For Etherscan-style APIs use query param
        if "scan.org" in url:
            auth_type = "param"
        
        # Add authentication
        api_key = self.api_keys.get(chain_id)
        if api_key:
            if auth_type == "header":
                # Blockscout uses Bearer token in header
                headers['Authorization'] = f'Bearer {api_key}'
            elif auth_type == "param" and "apikey=" not in url and "api_key=" not in url:
                # Etherscan-style APIs use query param
                separator = "&" if "?" in url else "?"
                url = f"{url}{separator}apikey={api_key}"
        
        try:
            logger.info(f"Making request to {url} with auth type: {auth_type}")
            response = self.session.get(url, headers=headers, timeout=timeout)
            self.log_request(url, response, chain_id)
            return response
        except requests.exceptions.Timeout:
            logger.error(f"Request timed out: {url}")
            raise Exception(f"Request timed out for {self.chain_config[chain_id]['name']} API")
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error: {url}")
            raise Exception(f"Connection error for {self.chain_config[chain_id]['name']} API")
        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            raise
    
    def try_endpoints(self, chain_id, endpoint_template, **kwargs):
        """Try different endpoints for a chain until one works"""
        if chain_id not in self.chain_config:
            raise ValueError(f"Chain ID {chain_id} not supported")
        
        urls = self.chain_config[chain_id]["urls"]
        last_error = None
        
        for url_base in urls:
            try:
                url = endpoint_template.format(url_base=url_base, **kwargs)
                logger.info(f"Trying endpoint: {url} for {self.chain_config[chain_id]['name']}")
                response = self.make_request(url, chain_id)
                
                if response.status_code == 200:
                    return response.json()
                
            except Exception as e:
                last_error = e
                logger.warning(f"Endpoint failed: {url_base} - {str(e)}")
                continue
        
        # If we get here, all endpoints failed
        if last_error:
            raise last_error
        else:
            raise Exception(f"All endpoints failed for {self.chain_config[chain_id]['name']}")
    
    def get_transactions(self, address, chain_id, limit=5):
        """Fetch transactions for a given address on specified chain"""
        logger.info(f"Fetching transactions for {address} on {self.chain_config[chain_id]['name']}")
        
        # Try standard Blockscout endpoint first
        try:
            # Blockscout API format
            data = self.try_endpoints(
                chain_id, 
                "{url_base}addresses/{address}/transactions", 
                address=address
            )
            transactions = data.get('items', [])[:limit]
            
            # If no transactions found, try alternative formats
            if not transactions:
                logger.info("No transactions found, trying alternative Blockscout format")
                try:
                    data = self.try_endpoints(
                        chain_id, 
                        "{url_base}address/{address}/transactions", 
                        address=address
                    )
                    transactions = data.get('items', [])[:limit]
                except Exception as e:
                    logger.warning(f"Alternative Blockscout format failed: {str(e)}")
                
                # For Etherscan-style APIs
                if not transactions and any("scan.org" in url for url in self.chain_config[chain_id]["urls"]):
                    logger.info("Trying Etherscan-style API format")
                    try:
                        data = self.try_endpoints(
                            chain_id,
                            "{url_base}?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=desc",
                            address=address
                        )
                        transactions = data.get('result', [])[:limit]
                    except Exception as e:
                        logger.warning(f"Etherscan format failed: {str(e)}")
            
            logger.info(f"Found {len(transactions)} transactions on {self.chain_config[chain_id]['name']}")
            
            # Extract important fields
            parsed_txs = []
            for tx in transactions:
                # Handle different API response formats
                if 'hash' in tx:
                    # Blockscout format
                    tx_data = {
                        'hash': tx.get('hash'),
                        'status': tx.get('status'),
                        'method': tx.get('method'),
                        'timestamp': tx.get('timestamp'),
                        'value': tx.get('value'),
                        'from': tx.get('from', {}).get('hash') if isinstance(tx.get('from'), dict) else tx.get('from'),
                        'to': tx.get('to', {}).get('hash') if isinstance(tx.get('to'), dict) else tx.get('to')
                    }
                    
                    # Add decoded input if available
                    if tx.get('decoded_input'):
                        tx_data['method_call'] = tx.get('decoded_input', {}).get('method_call')
                        tx_data['parameters'] = tx.get('decoded_input', {}).get('parameters')
                else:
                    # Etherscan format
                    tx_data = {
                        'hash': tx.get('hash', tx.get('txHash', tx.get('transactionHash'))),
                        'status': '1' if tx.get('txreceipt_status') == '1' else '0',
                        'method': 'unknown',
                        'timestamp': tx.get('timeStamp'),
                        'value': tx.get('value'),
                        'from': tx.get('from'),
                        'to': tx.get('to')
                    }
                
                parsed_txs.append(tx_data)
            
            return parsed_txs
            
        except Exception as e:
            logger.error(f"Error fetching transactions: {str(e)}")
            # Return empty list instead of raising to allow partial results
            return []
    
    def get_transaction_logs(self, tx_hash, chain_id):
        """Fetch logs for a specific transaction on the specified chain"""
        logger.info(f"Fetching logs for transaction {tx_hash} on {self.chain_config[chain_id]['name']}")
        
        try:
            # Blockscout API format
            data = self.try_endpoints(
                chain_id,
                "{url_base}transactions/{tx_hash}/logs",
                tx_hash=tx_hash
            )
            logs = data.get('items', [])
            
            # If no logs found with Blockscout API, try Etherscan-style API
            if not logs and any("scan.org" in url for url in self.chain_config[chain_id]["urls"]):
                logger.info("Trying Etherscan-style API for logs")
                try:
                    data = self.try_endpoints(
                        chain_id,
                        "{url_base}?module=logs&action=getTxLogs&txhash={tx_hash}",
                        tx_hash=tx_hash
                    )
                    logs = data.get('result', [])
                except Exception as e:
                    logger.warning(f"Etherscan logs format failed: {str(e)}")
            
            logger.info(f"Found {len(logs)} logs for transaction on {self.chain_config[chain_id]['name']}")
            
            # Extract important fields
            parsed_logs = []
            for log in logs:
                # Check if this is a Transfer event (ERC20/ERC721 transfer signature)
                is_transfer = False
                topics = log.get('topics', [])
                if topics and topics[0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
                    is_transfer = True
                
                # Extract token name safely
                token_name = None
                if isinstance(log.get('address'), dict):
                    token_name = log.get('address', {}).get('name')
                
                log_data = {
                    'address': log.get('address', {}).get('hash') if isinstance(log.get('address'), dict) else log.get('address'),
                    'token_name': token_name,
                    'is_transfer': is_transfer,
                    'topics': topics,
                    'data': log.get('data')
                }
                
                # Add decoded data if available
                if log.get('decoded'):
                    log_data['method_call'] = log.get('decoded', {}).get('method_call')
                    log_data['parameters'] = log.get('decoded', {}).get('parameters')
                
                parsed_logs.append(log_data)
            
            return parsed_logs
            
        except Exception as e:
            logger.error(f"Error fetching transaction logs: {str(e)}")
            # Return empty list instead of raising
            return []
    
    def get_transactions_with_logs(self, address, chain_id, limit=5):
        """Get transactions and their associated logs in one call"""
        txs = self.get_transactions(address, chain_id, limit)
        
        for tx in txs:
            try:
                logger.info(f"Fetching logs for transaction {tx['hash']}")
                tx['logs'] = self.get_transaction_logs(tx['hash'], chain_id)
                # Add small delay between requests to avoid rate limiting
                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"Could not fetch logs for tx {tx['hash']}: {str(e)}")
                tx['logs'] = []
        
        return txs
    
    def to_dataframe(self, data):
        """Convert transaction data to pandas DataFrame for easier analysis"""
        return pd.DataFrame(data)


def test_blockscout_api():
    # Initialize API with debug mode
    api = BlockscoutAPI(debug=True)
    
    # Contract address to test
    contract_address = "0xD251c1325c5d7b29C6219912D8648a3149cDF57B"
    base_chain_id = 8453  # Base
    
    try:
        print(f"Testing BlockscoutAPI for contract {contract_address} on {api.chain_config[base_chain_id]['name']}")
        print("-" * 80)
        
        # Check Base API key status only
        print("API key status:")
        key = api.api_keys.get(base_chain_id)
        if key:
            masked_key = f"{key[:4]}...{key[-4:]}" if len(key) > 8 else "****"
            print(f"✅ {api.chain_config[base_chain_id]['name']} (ID: {base_chain_id}): API key available ({masked_key})")
        else:
            print(f"❌ {api.chain_config[base_chain_id]['name']} (ID: {base_chain_id}): No API key found")
        
        # Test connecting to Base endpoints only
        print("\nTesting connection to Base API endpoints...")
        for url in api.chain_config[base_chain_id]["urls"]:
            if "blockscout" in url:
                # Blockscout health check endpoint
                ping_url = f"{url}health"
                auth_type = "header"
            else:
                # For Etherscan-style APIs
                ping_url = f"{url}?module=block&action=getblocknobytime&timestamp=1609459200&closest=before"
                auth_type = "param"
            
            headers = {}
            if auth_type == "header" and api.api_keys.get(base_chain_id):
                headers['Authorization'] = f'Bearer {api.api_keys.get(base_chain_id)}'
            elif auth_type == "param" and api.api_keys.get(base_chain_id):
                ping_url += f"&apikey={api.api_keys.get(base_chain_id)}"
            
            try:
                response = requests.get(ping_url, headers=headers, timeout=5)
                print(f"✅ Base API ({url}): Status {response.status_code}")
                print(f"   Response: {response.text[:100]}..." if len(response.text) > 100 else f"   Response: {response.text}")
            except Exception as e:
                print(f"❌ Base API ({url}): Error - {str(e)}")
        
        # Test 1: Get transactions
        print("\nTest 1: Fetching transactions...")
        txs = api.get_transactions(contract_address, base_chain_id)
        print(f"✅ Found {len(txs)} transactions on Base\n")
        
        # Display transaction summary
        if txs:
            tx_df = api.to_dataframe(txs)
            display_cols = ['hash', 'method', 'timestamp', 'status']
            print("Transaction summary:")
            display(tx_df[display_cols])
            
            # Test 2: Get logs for the first transaction (if available)
            if txs:
                print("\nTest 2: Fetching logs for first transaction...")
                tx_hash = txs[0]['hash']
                logs = api.get_transaction_logs(tx_hash, base_chain_id)
                print(f"✅ Found {len(logs)} logs for transaction {tx_hash[:10]}...\n")
                
                # Display logs summary if available
                if logs:
                    logs_df = api.to_dataframe(logs)
                    display_cols = ['token_name', 'is_transfer']
                    if 'address' in logs_df.columns:
                        display_cols.insert(0, 'address')
                    print("Logs summary:")
                    display(logs_df[display_cols])
                else:
                    print("No logs found or API unavailable")
            
            # Test 3: Get transactions with logs (limited to reduce API load)
            print("\nTest 3: Fetching transactions with logs...")
            txs_with_logs = api.get_transactions_with_logs(contract_address, base_chain_id, limit=1)
            print(f"✅ Found {len(txs_with_logs)} transactions with logs\n")
            
            # Print sample of combined data
            if txs_with_logs:
                print("Sample transaction with logs:")
                sample_tx = txs_with_logs[0]
                print(f"Transaction: {sample_tx['hash']}")
                print(f"Method: {sample_tx.get('method_call', sample_tx.get('method', 'Unknown'))}")
                print(f"Logs count: {len(sample_tx.get('logs', []))}")
                
                # Show token transfers if any
                transfers = [log for log in sample_tx.get('logs', []) if log.get('is_transfer')]
                if transfers:
                    print(f"Token transfers: {len(transfers)}")
                    for transfer in transfers[:2]:  # Show max 2 transfers
                        print(f"  - Token: {transfer.get('token_name', 'Unknown')}")
                        print(f"  - Address: {transfer.get('address', 'Unknown')}")
            else:
                print("No transactions with logs found or API unavailable")
        
        return "All tests completed successfully!"
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return None

# Run the test
test_blockscout_api()

# %%
def process_all_contracts():
    """Process all unprocessed contracts from the target view."""
    processed_count = 0
    updated_count = 0
    failed_update_count = 0
    github_not_found_count = 0
    additional_api_checked_count = 0
    
    try:
        # Fetch all unprocessed records directly from the view
        print(f"Fetching unprocessed records from Airtable view {TARGET_VIEW_ID}...")
        records = AirtableAPI.fetch_all_unprocessed_contracts()

        if not records:
            print(f"No unprocessed records found in view {TARGET_VIEW_ID}.")
            return
            
        total_records = len(records)
        print(f"Found {total_records} unprocessed records to process.")

        # Process each record
        for index, record in enumerate(records, 1):
            processed_count += 1
            record_id = record.get('id')
            fields = record.get('fields', {})
            contract_address = fields.get('address')
            origin_key = fields.get('origin_key')
            # Default to 'base' chain if missing
            chain = fields.get('chain') or 'base'

            if not record_id:
                print(f"Skipping record {index}/{total_records} - missing record ID")
                continue
                
            if not contract_address:
                print(f"Skipping record {record_id} ({index}/{total_records}) - no contract address")
                continue

            print(f"--- Processing {index}/{total_records}: Record ID {record_id}, Address {contract_address}, Chain {chain} ---")

            # Search GitHub
            github_found, repo_count = GitHubAPI.search_contract(contract_address)

            # If no GitHub repositories found, try the additional API
            transaction_data = None
            if not github_found:
                github_not_found_count += 1
                print(f"No GitHub repositories found for {contract_address}, checking Contract API...")
                additional_api_checked_count += 1
                transaction_data = ContractAPI.process_contract_additional_data(contract_address, chain)
                
                # Save to our tracking file regardless of transaction data success
                AirtableAPI.save_no_github_contracts(contract_address, record_id, origin_key)
            
            # Update Airtable with github_found, repo_count, and transaction data if available
            update_successful = AirtableAPI.update_record(record_id, github_found, repo_count, transaction_data)
            if update_successful:
                updated_count += 1
            else:
                failed_update_count += 1
                print(f"Failed to update Airtable record {record_id}")

            # Rate limiting delay between processing records
            print(f"--- Finished processing record {record_id}. Sleeping... ---")
            time.sleep(2)

        print(f"\n=== Processing Summary ===")
        print(f"Total records processed: {processed_count}")
        print(f"Airtable records updated successfully: {updated_count}")
        print(f"Airtable records failed to update: {failed_update_count}")
        print(f"Contracts without GitHub presence: {github_not_found_count}")
        print(f"Contracts checked via Contract API: {additional_api_checked_count}")
        print(f"=========================")

    except Exception as e:
        print(f"An unexpected error occurred during the process: {str(e)}")

print("process_all_contracts function defined.")

# %%
# Test 1: Check Airtable View Structure
if AIRTABLE_TOKEN:
    print(f"Checking structure of view: {TARGET_VIEW_ID}")
    view_info = AirtableAPI.get_view_structure()
    print(json.dumps(view_info, indent=2))
else:
    print("Skipping Airtable view structure check (missing AIRTABLE_TOKEN).")

# %%
# Test 2: Fetch Unprocessed Contracts from Airtable
if AIRTABLE_TOKEN:
    print("Fetching a batch of unprocessed contracts...")
    unprocessed_records = AirtableAPI.fetch_all_unprocessed_contracts()
    if unprocessed_records:
        print(f"Fetched {len(unprocessed_records)} records. First record:")
        print(json.dumps(unprocessed_records[0], indent=2))
    else:
        print("No unprocessed records found or error fetching.")
else:
    print("Skipping Airtable fetch test (missing AIRTABLE_TOKEN).")

# %%
# Test 3: Search GitHub for a Specific Contract Address
test_address = "0x347cee5cC8C6FB4872123B40B799A8750f0E7EA2" # Example address
if GITHUB_TOKEN:
    print(f"Searching GitHub for address: {test_address}")
    found, count = GitHubAPI.search_contract(test_address)
    print(f"GitHub Search Result: Found={found}, Valid Count={count}")
else:
    print("Skipping GitHub search test (missing GITHUB_TOKEN).")

# %%
# Run the full process
# Warning: This will modify your Airtable data for unprocessed records.

run_full = False # Set to True to execute

if run_full:
    if AIRTABLE_TOKEN and GITHUB_TOKEN and CONTRACT_API_BASE_URL != 'https://api.example.com':
        print("Starting full contract processing...")
        process_all_contracts()
        print("Full contract processing finished.")
    else:
        print("Cannot run full process. Check:")
        if not AIRTABLE_TOKEN: print("- Missing AIRTABLE_TOKEN")
        if not GITHUB_TOKEN: print("- Missing GITHUB_TOKEN")
        if CONTRACT_API_BASE_URL == 'https://api.example.com': print("- CONTRACT_API_BASE_URL is not set")
else:
    print("Full process run skipped (run_full is False). Set run_full = True in the cell above to execute.")


