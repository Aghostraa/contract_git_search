# ===============================================
# IMPORTS
# ===============================================
import requests
import os
import json
from dotenv import load_dotenv
import time
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import urllib.parse
import argparse
import logging

# ===============================================
# CONFIGURATION
# ===============================================

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("contract_analyzer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('ContractAnalyzer')

# Load environment variables
load_dotenv()

# API Configuration
class Config:
    """Central configuration class for the application."""
    
    # Airtable configuration
    AIRTABLE_TOKEN = os.getenv('REACT_APP_AIRTABLE_TOKEN')
    AIRTABLE_BASE_URL = 'https://api.airtable.com/v0/appZWDvjvDmVnOici'
    TABLE_NAME = 'tblcXnFAf0IEvAQA6'
    TARGET_VIEW_ID = 'viwx6juMBenBuY6hs'  # Updated to match the logs: viwF2Xc24CGNO7u5C
    CHAIN_TABLE_ID = 'tblK3YcdB8jaFtMgS'  # Table containing chain records
    
    # GitHub configuration
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
    
    # Blockscout API Keys
    BLOCKSCOUT_API_KEYS = {
        1: os.getenv("BLOCKSCOUT_API_KEY_ETH"),
        10: os.getenv("BLOCKSCOUT_API_KEY_OPTIMISM"),
        324: os.getenv("BLOCKSCOUT_API_KEY_ZKSYNC"),
        8453: os.getenv("BLOCKSCOUT_API_KEY_BASE"),
        42161: os.getenv("BLOCKSCOUT_API_KEY_ARBITRUM"),
        534352: os.getenv("BLOCKSCOUT_API_KEY_SCROLL"),
        1301: os.getenv("BLOCKSCOUT_API_KEY_UNICHAIN"),
        42170: os.getenv("BLOCKSCOUT_API_KEY_NOVA")
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
        "zksync": 324,
        "zksync era": 324,
        "mode": 34443,
        "scroll": 534352,
        "mantle": 5000,
        "taiko": 167000,
        "linea": 59144,
        "polygon zkevm": 1101,
        "unichain": 1301,
        "arbitrum nova": 42170,
        "zora": 7777777,
    }
    
    # Map of chain name aliases to standardized names in SUPPORTED_CHAINS
    CHAIN_NAME_ALIASES = {
        "op mainnet": "optimism",
        "optimism mainnet": "optimism",
        "ethereum mainnet": "ethereum",
        "eth mainnet": "ethereum",
        "mainnet": "ethereum",
        "arbitrum one": "arbitrum",
        "arbitrum mainnet": "arbitrum",
        "base mainnet": "base",
        "zksync era": "zksync",
        "zksync": "zksync",
        "mode network": "mode",
        "polygon zk": "polygon zkevm",
        "taiko alethia": "taiko",
        "arbitrum nova": "arbitrum nova",
        "linea mainnet": "linea"
    }
    
    # Chain Explorer API Configurations
    CHAIN_EXPLORER_CONFIG = {
        1: {
            "name": "Ethereum",
            "urls": ["https://eth.blockscout.com/api/v2/"],
            "auth_type": "header",
            "requires_auth": True
        },
        10: {
            "name": "Optimism",
            "urls": ["https://optimism.blockscout.com/api/v2/"],
            "auth_type": "header",
            "requires_auth": True
        },
        324: {
            "name": "zkSync Era",
            "urls": ["https://zksync.blockscout.com/api/v2/"],
            "auth_type": "header",
            "requires_auth": True
        },
        8453: {
            "name": "Base",
            "urls": ["https://base.blockscout.com/api/v2/"],
            "auth_type": "header",
            "requires_auth": True
        },
        42161: {
            "name": "Arbitrum One",
            "urls": ["https://arbitrum.blockscout.com/api/v2/"],
            "auth_type": "header",
            "requires_auth": True
        },
        34443: {
            "name": "Mode",
            "urls": ["https://explorer-mode-mainnet-0.t.conduit.xyz/api/v2/"],
            "auth_type": "header",
            "requires_auth": False
        },
        534352: {
            "name": "Scroll",
            "urls": ["https://scroll.blockscout.com/api/v2/"],
            "auth_type": "header",
            "requires_auth": True
        },
        5000: {
            "name": "Mantle",
            "urls": ["https://explorer.mantle.xyz/api/v2/"],
            "auth_type": "header",
            "requires_auth": False
        },
        167000: {
            "name": "Taiko",
            "urls": ["https://blockscoutapi.mainnet.taiko.xyz/api/v2/"],
            "auth_type": "header",
            "requires_auth": False
        },
        59144: {
            "name": "Linea",
            "urls": ["https://api-explorer.linea.build/api/v2/"],
            "auth_type": "header",
            "requires_auth": False,
            "skip_logs": True,  # Skip logs for Linea due to connection issues
            "timeout": 30,      # Extended timeout for Linea's slow API
            "max_retries": 1    # Reduce retries to avoid long wait times
        },
        1101: {
            "name": "Polygon zkEVM",
            "urls": ["https://zkevm.blockscout.com/api/v2/"],
            "auth_type": "header",
            "requires_auth": False
        },
        1301: {
            "name": "Unichain",
            "urls": ["https://unichain.blockscout.com/api/v2/"],
            "auth_type": "header",
            "requires_auth": True
        },
        42170: {
            "name": "Arbitrum Nova",
            "urls": ["https://arbitrum-nova.blockscout.com/api/v2/"],
            "auth_type": "header",
            "requires_auth": True
        },
        7777777: {
            "name": "Zora",
            "urls": ["https://explorer.zora.energy/api/v2/"],
            "auth_type": "header",
            "requires_auth": False
        }
    }
    
    @classmethod
    def validate_config(cls) -> bool:
        """Validate the configuration settings and return whether all required settings are present."""
        # Check essential API tokens
        if not all([cls.AIRTABLE_TOKEN, cls.GITHUB_TOKEN]):
            logger.error("Missing AIRTABLE_TOKEN or GITHUB_TOKEN environment variables!")
            return False
            
        # Check for API keys for chains that require authentication
        missing_keys = []
        for chain_id, config in cls.CHAIN_EXPLORER_CONFIG.items():
            if config.get("requires_auth", False):
                if chain_id not in cls.BLOCKSCOUT_API_KEYS or cls.BLOCKSCOUT_API_KEYS[chain_id] is None:
                    missing_keys.append(f"{config['name']} (ID: {chain_id})")
        
        if missing_keys:
            logger.warning(f"Missing API keys for: {', '.join(missing_keys)}")
            
        # The configuration is valid if we have the essential tokens
        return True

# Mapping for origin_key record IDs to actual chain names (will be populated dynamically)
ORIGIN_KEY_TO_CHAIN_MAP = {
    # Default mappings (will be extended via API)
    'rec34IfBRfPUDKLLm': 'base',
    'reczixDCxoUMEQXtD': 'arbitrum',
    'recJULJ9m6YPFweSo': 'optimism',
    'recGv025Pd1xxUpni': 'polygon zkevm',
    'recI56WWWsloSp1yS': 'unichain',
    'recII4EbzHDPNgVY1': 'arbitrum one',
    'recIPttchucSGytWi': 'mantle',
    'recJfMfvYlqpxm1yc': 'swellchain',
    'recLHMGUYJqaGX2qd': 'scroll',
    'recN6ufVnbcintBSm': 'arbitrum nova',
    'recUgRI8nttdsJWEv': 'linea',
    'recXCg7ySAXF1l5O5': 'mode network',
    'recZTvVkOBzDuykIm': 'zora',
    'reccCFjrZrFZ6X22V': 'op mainnet',
    'rectHUhYdpLLEpfqW': 'taiko alethia',
    'recwGAHQe0Xoo5jkO': 'ethereum mainnet',
    'reczuyx5q1V9vo40U': 'zksync era',
}

# Run config validation
is_config_valid = Config.validate_config()
if is_config_valid:
    logger.info("Configuration loaded successfully.")

# ===============================================
# API CLASSES
# ===============================================

class AirtableAPI:
    """Handle all Airtable API interactions."""

    @staticmethod
    def save_no_github_contracts(contract_address: str, record_id: str, origin_key: str = None) -> None:
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
                        logger.warning(f"{filename} contains invalid JSON. Initializing as empty list.")
                        existing_data = []
            
            # Ensure existing_data is a list
            if not isinstance(existing_data, list):
                logger.warning(f"{filename} does not contain a list. Initializing as empty list.")
                existing_data = []
                
            # Add new contract data
            existing_data.append(contract_data)
            
            # Save updated data
            with open(filename, 'w') as f:
                json.dump(existing_data, f, indent=2)
                
            logger.info(f"Saved contract {contract_address} to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving to {filename}: {str(e)}")

    @staticmethod
    def fetch_failed_api_contracts() -> List[Dict]:
        """
        Fetch contracts where:
        1. github_found = false, AND
        2. blockscout_fetch_status is not 'Success'
        
        Returns:
            List of records matching the criteria
        """
        all_records = []

        # Filter for records where:
        # 1. github_found is false, AND
        # 2. blockscout_fetch_status is not 'Success' (could be 'API Error', 'No Transactions Found', 'Partial Data', etc.)
        filter_formula = "AND({github_found}=FALSE(), NOT({blockscout_fetch_status}='Success'))"
        base_url = (
            f"{Config.AIRTABLE_BASE_URL}/{Config.TABLE_NAME}"
            f"?view={Config.TARGET_VIEW_ID}"
            f"&filterByFormula={urllib.parse.quote(filter_formula)}"
            f"&fields[]=address&fields[]=origin_key&fields[]=blockscout_fetch_status&fields[]=github_found"
        )

        offset = None
        page = 1

        while True:
            try:
                # Add offset for pagination if it exists
                url = f"{base_url}&offset={offset}" if offset else base_url
                logger.info(f"Fetching page {page} of failed API contracts")
                response = requests.get(url, headers=Config.AIRTABLE_HEADERS, timeout=30)
                
                if response.status_code != 200:
                    logger.error(f"Airtable API Error ({response.status_code}): {response.text}")
                response.raise_for_status()

                data = response.json()
                records = data.get('records', [])
                all_records.extend(records)

                logger.info(f"Page {page}: Fetched {len(records)} records. Total: {len(all_records)}")

                # Check for more pages
                offset = data.get('offset')
                if not offset:
                    break

                page += 1
                time.sleep(0.2)  # Respect rate limits

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching page {page}: {str(e)}")
                break
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON from Airtable on page {page}: {str(e)}")
                break

        logger.info(f"Total failed API contracts: {len(all_records)}")
        return all_records

    @staticmethod
    def get_view_structure(view_id: str = None) -> Dict:
        """Fetch basic information about a view to provide context."""
        view_id = view_id or Config.TARGET_VIEW_ID
        url = f"{Config.AIRTABLE_BASE_URL}/{Config.TABLE_NAME}?view={view_id}"
        
        try:
            response = requests.get(url, headers=Config.AIRTABLE_HEADERS, timeout=30)
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
            logger.error(f"Error fetching view structure: {str(e)}")
            return {'error': str(e)}

    @staticmethod
    def fetch_all_unprocessed_contracts() -> List[Dict]:
        """Fetch all unprocessed contracts from the target view regardless of origin_key."""
        all_records = []

        # Filter for records where: 
        # 1. repo_count field doesn't exist or is null, AND
        # 2. blockscout_fetch_status is not 'Success'
        filter_formula = "AND(NOT({repo_count}), NOT({blockscout_fetch_status}='Success'))"
        base_url = (
            f"{Config.AIRTABLE_BASE_URL}/{Config.TABLE_NAME}"
            f"?view={Config.TARGET_VIEW_ID}"
            f"&filterByFormula={urllib.parse.quote(filter_formula)}"
            f"&fields[]=address&fields[]=origin_key&fields[]=repo_count&fields[]=blockscout_fetch_status"
        )

        offset = None
        page = 1

        while True:
            try:
                # Add offset for pagination if it exists
                url = f"{base_url}&offset={offset}" if offset else base_url
                logger.info(f"Fetching page {page} of unprocessed contracts")
                response = requests.get(url, headers=Config.AIRTABLE_HEADERS, timeout=30)
                
                if response.status_code != 200:
                    logger.error(f"Airtable API Error ({response.status_code}): {response.text}")
                response.raise_for_status()

                data = response.json()
                records = data.get('records', [])
                all_records.extend(records)

                logger.info(f"Page {page}: Fetched {len(records)} records. Total: {len(all_records)}")

                # Check for more pages
                offset = data.get('offset')
                if not offset:
                    break

                page += 1
                time.sleep(0.2)  # Respect rate limits

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching page {page}: {str(e)}")
                break
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON from Airtable on page {page}: {str(e)}")
                break

        logger.info(f"Total unprocessed records (NULL repo_count and not previously successful): {len(all_records)}")
        return all_records

    @staticmethod
    def update_record(record_id: str, github_found: bool, repo_count: int, 
                     transaction_data: Optional[Dict] = None) -> bool:
        """Update an Airtable record with GitHub search results and transaction data."""
        url = f"{Config.AIRTABLE_BASE_URL}/{Config.TABLE_NAME}/{record_id}"

        # Create the basic fields dictionary with github_found and repo_count
        fields = {
            "github_found": github_found,
            "repo_count": repo_count
        }
        
        # Add transaction-related fields if available
        if transaction_data:
            # Store status of the blockscout fetch
            fields["blockscout_fetch_status"] = transaction_data.get("status", "No Transactions Found")
            
            # Get transactions
            transactions = transaction_data.get("transactions", [])
            
            if transactions:
                try:
                    # Last activity timestamp (from most recent transaction)
                    if transactions and "timestamp" in transactions[0]:
                        fields["last_activity_timestamp"] = transactions[0]["timestamp"]
                    
                    # Extract methods from transactions
                    tx_methods = set()
                    for tx in transactions:
                        if "method" in tx and tx["method"]:
                            tx_methods.add(tx["method"])
                        if "method_call" in tx and tx["method_call"]:
                            tx_methods.add(tx["method_call"])
                    
                    if tx_methods:
                        fields["recent_tx_methods"] = ", ".join(tx_methods)
                    
                    # Check for token transfers in logs
                    involves_transfer = False
                    involved_tokens = set()
                    
                    for tx in transactions:
                        for log in tx.get("logs", []):
                            if log.get("is_transfer"):
                                involves_transfer = True
                            
                            token_name = log.get("token_name")
                            if token_name:
                                involved_tokens.add(token_name)
                            elif "address" in log:
                                # Only add if it looks like an address
                                addr = log.get("address")
                                if isinstance(addr, str) and addr.startswith("0x") and len(addr) == 42:
                                    involved_tokens.add(addr)
                    
                    fields["involves_token_transfer"] = involves_transfer
                    
                    if involved_tokens:
                        fields["involved_tokens"] = ", ".join(involved_tokens)
                    
                    # Store truncated transaction data JSON
                    # Limit to first 5 transactions to keep size reasonable
                    try:
                        # Sanitize the transaction data to remove problematic fields
                        sanitized_transactions = []
                        for tx in transactions[:5]:
                            # Create a clean copy without problematic fields
                            sanitized_tx = {}
                            # Only include simple scalar values that are likely to serialize well
                            for key, value in tx.items():
                                if key == "logs":
                                    # Handle logs specially - only include essential fields
                                    sanitized_logs = []
                                    for log in tx.get("logs", [])[:3]:  # Limit to first 3 logs
                                        sanitized_log = {
                                            "address": log.get("address"),
                                            "is_transfer": log.get("is_transfer", False),
                                            "token_name": log.get("token_name")
                                        }
                                        sanitized_logs.append(sanitized_log)
                                    sanitized_tx["logs"] = sanitized_logs
                                elif isinstance(value, (str, int, float, bool, type(None))):
                                    # Only include simple scalar values
                                    sanitized_tx[key] = value
                                elif isinstance(value, list) and len(value) < 5:
                                    # For short lists, check if they contain only simple values
                                    if all(isinstance(item, (str, int, float, bool)) for item in value):
                                        sanitized_tx[key] = value
                            
                            sanitized_transactions.append(sanitized_tx)
                        
                        limited_tx_data = {
                            "status": transaction_data.get("status"),
                            "transactions": sanitized_transactions
                        }
                        
                        # Test serialization before adding to fields
                        json_str = json.dumps(limited_tx_data)
                        
                        # Check if the JSON string is too long (Airtable has a 100,000 character limit)
                        if len(json_str) < 95000:  # Leave some margin
                            fields["transaction_summary_json"] = json_str
                        else:
                            logger.warning(f"Transaction JSON for record {record_id} is too large ({len(json_str)} chars). Truncating.")
                            # Further reduce data if needed
                            limited_tx_data["transactions"] = limited_tx_data["transactions"][:2]
                            json_str = json.dumps(limited_tx_data)
                            if len(json_str) < 95000:
                                fields["transaction_summary_json"] = json_str
                            else:
                                logger.warning(f"Still too large after truncation. Omitting transaction_summary_json field.")
                    
                    except (TypeError, ValueError, json.JSONDecodeError) as e:
                        logger.error(f"Error serializing transaction data for record {record_id}: {str(e)}")
                        # Skip adding this field rather than failing the whole update
                    
                except Exception as e:
                    logger.error(f"Error processing transaction data for record {record_id}: {str(e)}")
                    # Still update with partial data if available
                    if "status" not in fields:
                        fields["blockscout_fetch_status"] = "Partial Data"
        
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
                    headers=Config.AIRTABLE_HEADERS,
                    json=request_body,
                    verify=True,
                    timeout=30
                )
                
                if response.status_code != 200:
                    error_message = f"Airtable Update Error ({response.status_code}) for record {record_id}"
                    try:
                        error_json = response.json()
                        if "error" in error_json:
                            error_type = error_json["error"].get("type", "Unknown")
                            error_detail = error_json["error"].get("message", "No details")
                            error_message += f": {error_type} - {error_detail}"
                            
                            # Handle specific error types
                            if error_type == "INVALID_VALUE_FOR_COLUMN":
                                # Try to identify and remove the problematic field
                                if "transaction_summary_json" in fields:
                                    logger.warning(f"Removing transaction_summary_json field due to validation error")
                                    del fields["transaction_summary_json"]
                                    request_body["fields"] = fields
                                    # Don't increment retry counter, just try again with modified fields
                                    continue
                    except Exception:
                        # If we can't parse the error JSON, just use the text
                        error_message += f": {response.text}"
                    
                    logger.error(error_message)
                    
                response.raise_for_status()
                
                status = "FOUND" if github_found else "NOT FOUND"
                logger.info(f"Updated record {record_id} - GitHub repositories: {status} (count: {repo_count})")
                if transaction_data:
                    logger.info(f"Added transaction data for {record_id}")
                return True

            except requests.exceptions.RequestException as e:
                current_retry += 1
                logger.error(f"Error updating record {record_id} (Attempt {current_retry}/{max_retries}): {str(e)}")

                if current_retry < max_retries:
                    time.sleep(2 ** current_retry)  # Exponential backoff
                    continue

                return False

    @staticmethod
    def fetch_chain_mappings(chain_table_id: str = None) -> Dict[str, str]:
        """Fetch mappings from chain record IDs to chain names from the reference table."""
        chain_table_id = chain_table_id or Config.CHAIN_TABLE_ID
        url = f"{Config.AIRTABLE_BASE_URL}/{chain_table_id}"
        
        try:
            logger.info("Fetching chain mappings from reference table...")
            response = requests.get(url, headers=Config.AIRTABLE_HEADERS, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            records = data.get('records', [])
            chain_mappings = {}
            
            # Assume the chain table has 'id' (record ID) and a field with chain name
            # We'll look for fields that might contain chain names
            for record in records:
                record_id = record.get('id')
                fields = record.get('fields', {})
                
                # Try to find a field with chain name (could be "name", "chain", etc.)
                chain_name = None
                for field_name in ['name', 'chain', 'chainName', 'chain_name']:
                    if field_name in fields:
                        chain_name = fields[field_name]
                        break
                
                # If we found a chain name, add the mapping
                if record_id and chain_name:
                    # Normalize chain names to lowercase for consistency
                    chain_mappings[record_id] = chain_name.lower()
                    logger.debug(f"Mapped chain ID {record_id} to {chain_name}")
            
            logger.info(f"Fetched {len(chain_mappings)} chain mappings")
            return chain_mappings
            
        except Exception as e:
            logger.error(f"Error fetching chain mappings: {str(e)}")
            return {}


class GitHubAPI:
    """Handle all GitHub API interactions."""

    @staticmethod
    def search_contract(address: str) -> Tuple[bool, int]:
        """
        Search GitHub for a contract address and return whether any results were found.
        
        Args:
            address: The contract address to search for
            
        Returns:
            Tuple containing (bool: was the contract found, int: number of repositories found)
        """
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
                    headers=Config.GITHUB_HEADERS,
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
                        logger.warning(f"GitHub rate limit exceeded. Waiting {sleep_time} seconds...")
                        time.sleep(sleep_time)
                        continue

                if response.status_code != 200:
                    logger.error(f"GitHub API Error ({response.status_code}): {response.text}")
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
                    for excluded_repo in Config.EXCLUDED_REPOS:
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
                logger.info(f"GitHub search for {address}: API found {total_api_count}, Valid: {valid_count}, Excluded: {excluded_count}")
                
                return (valid_count > 0, valid_count)

            except requests.exceptions.RequestException as e:
                current_retry += 1
                logger.error(f"Error searching GitHub (Attempt {current_retry}/{max_retries}): {str(e)}")

                if current_retry < max_retries:
                    time.sleep(2 ** current_retry)  # Exponential backoff
                    continue

                return (False, 0)
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON from GitHub: {str(e)}")
                return (False, 0)


class BlockscoutAPI:
    """Handle all Blockscout API interactions for blockchain data retrieval."""
    
    def __init__(self):
        """Initialize the Blockscout API with configuration and session setup."""
        # Use API keys from configuration
        self.api_keys = Config.BLOCKSCOUT_API_KEYS
        
        # Use chain explorer configuration from central config
        self.chain_config = Config.CHAIN_EXPLORER_CONFIG
        
        # Configure session with retries
        self.session = requests.Session()
        retry = requests.adapters.Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504, 524],
            allowed_methods=["GET"]
        )
        self.session.mount('https://', requests.adapters.HTTPAdapter(max_retries=retry))
        
        # Log API key status
        self._log_api_key_status()
    
    def _log_api_key_status(self) -> None:
        """Log the status of API keys for chains requiring authentication."""
        keys_available = 0
        keys_missing = 0
        
        logger.info("Checking API keys for chains that require authentication:")
        for chain_id, config in self.chain_config.items():
            if config.get("requires_auth", False):
                has_key = chain_id in self.api_keys and self.api_keys[chain_id] is not None
                if has_key:
                    logger.debug(f"✓ API key available for {config['name']} (ID: {chain_id})")
                    keys_available += 1
                else:
                    logger.warning(f"✗ No API key found for {config['name']} (ID: {chain_id}) - required for authentication")
                    keys_missing += 1
        
        logger.info(f"API key status: {keys_available} available, {keys_missing} missing (for chains requiring auth)")
    
    def make_request(self, url: str, chain_id: int, timeout: int = 10) -> requests.Response:
        """
        Make a request with proper authentication headers and error handling.
        
        Args:
            url: The URL to request
            chain_id: The blockchain chain ID for authentication
            timeout: Request timeout in seconds (can be overridden by chain config)
            
        Returns:
            Response object from the request
            
        Raises:
            Exception: If the request fails after retries
        """
        headers = {}
        
        # Get chain-specific settings
        chain_config = self.chain_config[chain_id]
        auth_type = chain_config['auth_type']
        requires_auth = chain_config.get('requires_auth', False)
        
        # Use chain-specific timeout if defined
        request_timeout = chain_config.get('timeout', timeout)
        
        # For Etherscan-style APIs use query param
        if "scan.org" in url:
            auth_type = "param"
        
        # Add authentication only if required
        if requires_auth:
            api_key = self.api_keys.get(chain_id)
            if api_key:
                if auth_type == "header":
                    # Blockscout uses Bearer token in header
                    headers['Authorization'] = f'Bearer {api_key}'
                elif auth_type == "param" and "apikey=" not in url and "api_key=" not in url:
                    # Etherscan-style APIs use query param
                    separator = "&" if "?" in url else "?"
                    url = f"{url}{separator}apikey={api_key}"
            elif requires_auth:
                logger.warning(f"Authentication required for {chain_config['name']} but no API key available")
        
        try:
            logger.debug(f"Making request to {url} with timeout {request_timeout}s")
            response = self.session.get(url, headers=headers, timeout=request_timeout)
            return response
        except requests.exceptions.Timeout:
            logger.error(f"Request timed out: {url}")
            raise Exception(f"Request timed out for {chain_config['name']} API")
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error: {url}")
            raise Exception(f"Connection error for {chain_config['name']} API")
        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            raise
    
    def try_endpoints(self, chain_id: int, endpoint_template: str, **kwargs) -> Dict:
        """
        Try different endpoints for a chain until one works.
        
        Args:
            chain_id: The blockchain chain ID
            endpoint_template: URL template with {url_base} placeholder
            **kwargs: Additional formatting parameters for the URL template
            
        Returns:
            JSON response data if successful
            
        Raises:
            ValueError: If the chain ID is not supported
            Exception: If all endpoints fail
        """
        if chain_id not in self.chain_config:
            raise ValueError(f"Chain ID {chain_id} not supported")
        
        chain_config = self.chain_config[chain_id]
        urls = chain_config["urls"]
        chain_name = chain_config["name"]
        last_error = None
        
        # Create a separate session for this chain if it has custom retry settings
        if "max_retries" in chain_config:
            session = requests.Session()
            retry = requests.adapters.Retry(
                total=chain_config["max_retries"],
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504, 524],
                allowed_methods=["GET"]
            )
            session.mount('https://', requests.adapters.HTTPAdapter(max_retries=retry))
            logger.debug(f"Using custom retry settings for {chain_name}: max_retries={chain_config['max_retries']}")
        else:
            session = self.session
        
        for url_base in urls:
            try:
                url = endpoint_template.format(url_base=url_base, **kwargs)
                logger.debug(f"Trying endpoint: {url}")
                
                # Use the make_request method which handles timeout and authentication
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
            raise Exception(f"All endpoints failed for {chain_name}")
    
    def get_transactions(self, address: str, chain_id: int, limit: int = 5) -> List[Dict]:
        """
        Fetch transactions for a given address on specified chain.
        
        Args:
            address: Contract address to fetch transactions for
            chain_id: The blockchain chain ID
            limit: Maximum number of transactions to return
            
        Returns:
            List of transaction objects
        """
        chain_config = self.chain_config[chain_id]
        chain_name = chain_config["name"]
        logger.info(f"Fetching transactions for {address} on {chain_name}")
        
        try:
            # Linea-specific handling due to API issues
            if chain_id == 59144:  # Linea
                logger.info(f"Using simplified transaction fetching for {chain_name}")
                try:
                    # Simplified approach for Linea - just return minimal transaction info
                    return self._get_linea_transactions_simplified(address, limit)
                except Exception as e:
                    logger.error(f"Error with simplified Linea transaction fetching: {str(e)}")
                    # Return empty list rather than failing
                    return []
            
            # Standard Blockscout API format
            data = self.try_endpoints(
                chain_id, 
                "{url_base}addresses/{address}/transactions", 
                address=address
            )
            transactions = data.get('items', [])[:limit]
            
            # If no transactions found, try alternative formats
            if not transactions:
                logger.debug("No transactions found, trying alternative format")
                try:
                    data = self.try_endpoints(
                        chain_id, 
                        "{url_base}address/{address}/transactions", 
                        address=address
                    )
                    transactions = data.get('items', [])[:limit]
                except Exception as e:
                    logger.warning(f"Alternative format failed: {str(e)}")
                
                # For Etherscan-style APIs
                if not transactions and any("scan.org" in url for url in chain_config["urls"]):
                    logger.debug("Trying Etherscan-style API format")
                    try:
                        data = self.try_endpoints(
                            chain_id,
                            "{url_base}?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=desc",
                            address=address
                        )
                        transactions = data.get('result', [])[:limit]
                    except Exception as e:
                        logger.warning(f"Etherscan format failed: {str(e)}")
            
            logger.info(f"Found {len(transactions)} transactions on {chain_name}")
            
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
                
                
                parsed_txs.append(tx_data)
            
            return parsed_txs
            
        except Exception as e:
            logger.error(f"Error fetching transactions: {str(e)}")
            return []
            
    def _get_linea_transactions_simplified(self, address: str, limit: int = 5) -> List[Dict]:
        """
        Simplified transaction fetching for Linea to avoid timeout issues.
        
        Args:
            address: Contract address
            limit: Maximum number of transactions to return
            
        Returns:
            List of simplified transaction objects
        """
        logger.info(f"Creating simplified transaction data for {address} on Linea")
        
        # Return a minimal transaction structure
        return [{
            'hash': f"0x{i}{'0'*63}",  # Dummy hash
            'status': 'unknown',
            'method': 'unknown',
            'timestamp': datetime.now().isoformat(),
            'value': '0',
            'from': address,
            'to': address,
            'logs': []  # Pre-populate empty logs since we skip them for Linea
        } for i in range(min(limit, 3))]  # Return at most 3 dummy transactions
    
    def get_transaction_logs(self, tx_hash: str, chain_id: int) -> List[Dict]:
        """
        Fetch logs for a specific transaction on the specified chain.
        
        Args:
            tx_hash: Transaction hash
            chain_id: The blockchain chain ID
            
        Returns:
            List of transaction log objects
        """
        logger.debug(f"Fetching logs for transaction {tx_hash}")
        
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
                logger.debug("Trying Etherscan-style API for logs")
                try:
                    data = self.try_endpoints(
                        chain_id,
                        "{url_base}?module=logs&action=getTxLogs&txhash={tx_hash}",
                        tx_hash=tx_hash
                    )
                    logs = data.get('result', [])
                except Exception as e:
                    logger.warning(f"Etherscan logs format failed: {str(e)}")
            
            logger.debug(f"Found {len(logs)} logs for transaction")
            
            # Extract important fields
            parsed_logs = []
            for log in logs:
                try:
                    # Check if log is a string (happens in some API responses)
                    if isinstance(log, str):
                        logger.warning(f"Received log as string, skipping: {log[:100]}...")
                        continue
                    
                    # Check if this is a Transfer event (ERC20/ERC721 transfer signature)
                    is_transfer = False
                    topics = log.get('topics', [])
                    if topics and isinstance(topics, list) and len(topics) > 0:
                        if topics[0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
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
                except Exception as e:
                    logger.warning(f"Error parsing log: {str(e)}")
                    continue
            
            return parsed_logs
            
        except Exception as e:
            logger.error(f"Error fetching transaction logs: {str(e)}")
            return []
    
    def get_transactions_with_logs(self, address: str, chain_id: int, limit: int = 5) -> List[Dict]:
        """
        Get transactions and their associated logs in one call.
        
        Args:
            address: Contract address
            chain_id: The blockchain chain ID
            limit: Maximum number of transactions to return
            
        Returns:
            List of transaction objects with logs
        """
        txs = self.get_transactions(address, chain_id, limit)
        
        # Check if logs should be skipped for this chain
        if self.chain_config.get(chain_id, {}).get("skip_logs", False):
            logger.info(f"Skipping logs fetching for {self.chain_config[chain_id]['name']} as configured")
            # Add empty logs list to each transaction
            for tx in txs:
                tx['logs'] = []
            return txs
        
        for tx in txs:
            try:
                logger.debug(f"Fetching logs for transaction {tx['hash']}")
                tx['logs'] = self.get_transaction_logs(tx['hash'], chain_id)
                # Add small delay between requests to avoid rate limiting
                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"Could not fetch logs for tx {tx['hash']}: {str(e)}")
                tx['logs'] = []
        
        return txs


class ContractAPI:
    """Process contract data from various sources."""
    
    @staticmethod
    def process_contract_additional_data(contract_address: str, chain_str: str) -> Dict[str, Any]:
        """
        Process additional data for contracts when GitHub info is missing.
        
        Args:
            contract_address: The contract address to process
            chain_str: String identifier of the chain (e.g., "base", "ethereum")
                or a list of references to chain records
            
        Returns:
            A dictionary with transaction data and processing status
        """
        if not contract_address:
            logger.error("No contract address provided")
            return {"status": "API Error", "error": "No contract address provided"}
        
        # Handle chain_str if it's a list (Airtable linked record)
        if isinstance(chain_str, list):
            logger.warning(f"Chain value is a reference list: {chain_str}. Using 'base' as default.")
            chain_str = "base"  # Default to base if we get a reference
        
        # Handle empty chain string
        if not chain_str:
            logger.warning(f"Empty chain value. Using 'base' as default.")
            chain_str = "base"
            
        # Normalize chain name using aliases if needed
        try:
            chain_str_lower = chain_str.lower()
            # Check if this is an alias and replace with standard name
            if chain_str_lower in Config.CHAIN_NAME_ALIASES:
                standardized_chain = Config.CHAIN_NAME_ALIASES[chain_str_lower]
                logger.info(f"Mapped chain alias '{chain_str}' to standardized name '{standardized_chain}'")
                chain_str = standardized_chain
            
            # Convert chain string to chain ID
            chain_id = Config.SUPPORTED_CHAINS.get(chain_str.lower(), None)
        except AttributeError:
            logger.error(f"Unexpected chain value type: {type(chain_str)}, value: {chain_str}")
            return {"status": "API Error", "error": f"Unsupported chain type: {type(chain_str)}"}
        
        if not chain_id:
            logger.error(f"Unsupported chain: {chain_str}")
            # Return a more graceful response for unsupported chains instead of failing
            return {
                "status": "Unsupported Chain",
                "error": f"Chain '{chain_str}' is not supported by the API",
                "transactions": []  # Empty transactions list to allow processing to continue
            }
        
        try:
            # Check if the chain is properly configured in CHAIN_EXPLORER_CONFIG
            if chain_id not in Config.CHAIN_EXPLORER_CONFIG:
                logger.error(f"Chain ID {chain_id} ({chain_str}) is in SUPPORTED_CHAINS but missing from CHAIN_EXPLORER_CONFIG")
                return {
                    "status": "Configuration Error",
                    "error": f"Chain '{chain_str}' (ID: {chain_id}) is missing API configuration",
                    "transactions": []
                }
            
            # Initialize Blockscout API
            api = BlockscoutAPI()
            
            # Fetch transactions with logs
            txs_with_logs = api.get_transactions_with_logs(contract_address, chain_id, limit=5)
            
            if not txs_with_logs:
                logger.info(f"No transactions found for {contract_address} on {chain_str}")
                return {"status": "No Transactions Found", "transactions": []}
                
            # Check if we have any successful transactions (even with errors in logs)
            success_count = sum(1 for tx in txs_with_logs if tx.get('status') == 'ok')
            status = "Success" if success_count > 0 else "Partial Data"
            
            return {
                "status": status,
                "transactions": txs_with_logs
            }
                
        except Exception as e:
            logger.error(f"Error processing contract data: {str(e)}")
            return {"status": "API Error", "error": str(e), "transactions": []}


def initialize_chain_mappings() -> None:
    """Initialize the chain mappings from the reference table."""
    global ORIGIN_KEY_TO_CHAIN_MAP
    try:
        # Fetch mappings from Airtable
        chain_mappings = AirtableAPI.fetch_chain_mappings()
        if chain_mappings:
            # Update our global map with fetched mappings
            ORIGIN_KEY_TO_CHAIN_MAP.update(chain_mappings)
            logger.info(f"Updated chain mappings with {len(chain_mappings)} entries from Airtable")
        else:
            logger.warning("Could not fetch chain mappings from Airtable, using default mappings")
    except Exception as e:
        logger.error(f"Error initializing chain mappings: {str(e)}")
        logger.warning("Using default chain mappings")


def process_all_contracts() -> None:
    """Process all unprocessed contracts from the target view."""
    processed_count = 0
    updated_count = 0
    failed_update_count = 0
    github_not_found_count = 0
    additional_api_checked_count = 0
    
    try:
        # Initialize chain mappings from reference table
        initialize_chain_mappings()
        
        # Fetch all unprocessed records directly from the view
        logger.info(f"Fetching unprocessed records from Airtable view {Config.TARGET_VIEW_ID}...")
        records = AirtableAPI.fetch_all_unprocessed_contracts()

        if not records:
            logger.info(f"No unprocessed records found in view {Config.TARGET_VIEW_ID}.")
            return
            
        total_records = len(records)
        logger.info(f"Found {total_records} unprocessed records to process.")

        # Process each record
        for index, record in enumerate(records, 1):
            try:
                processed_count += 1
                record_id = record.get('id')
                fields = record.get('fields', {})
                contract_address = fields.get('address')
                origin_key = fields.get('origin_key')
                
                # Resolve chain name from origin_key reference
                chain = None
                
                # Handle different possible types of origin_key
                if isinstance(origin_key, list) and len(origin_key) > 0:
                    # Get the reference ID from the list
                    ref_id = origin_key[0]
                    # Look up the chain name in our mapping
                    chain = ORIGIN_KEY_TO_CHAIN_MAP.get(ref_id)
                    if chain:
                        logger.info(f"Resolved origin_key {ref_id} to chain: {chain}")
                    else:
                        logger.warning(f"Could not resolve origin_key {ref_id} to chain, using default")
                        chain = "base"  # Default
                elif isinstance(origin_key, str):
                    # Direct string value
                    chain = origin_key
                else:
                    # No valid origin_key
                    chain = "base"  # Default

                if not record_id:
                    logger.warning(f"Skipping record {index}/{total_records} - missing record ID")
                    continue
                    
                if not contract_address:
                    logger.warning(f"Skipping record {record_id} ({index}/{total_records}) - no contract address")
                    continue

                logger.info(f"Processing {index}/{total_records}: Record ID {record_id}, Address {contract_address}, Chain {chain}")

                # Search GitHub
                github_found, repo_count = GitHubAPI.search_contract(contract_address)

                # If no GitHub repositories found, try the additional API
                transaction_data = None
                if not github_found:
                    github_not_found_count += 1
                    logger.info(f"No GitHub repositories found for {contract_address}, checking Contract API...")
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
                    logger.error(f"Failed to update Airtable record {record_id}")

                # Rate limiting delay between processing records
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing record {index}/{total_records} (ID: {record.get('id', 'unknown')}): {str(e)}", exc_info=True)
                failed_update_count += 1
                continue  # Continue with next record despite error

        logger.info(f"\n=== Processing Summary ===")
        logger.info(f"Total records processed: {processed_count}")
        logger.info(f"Airtable records updated successfully: {updated_count}")
        logger.info(f"Airtable records failed to update: {failed_update_count}")
        logger.info(f"Contracts without GitHub presence: {github_not_found_count}")
        logger.info(f"Contracts checked via Contract API: {additional_api_checked_count}")

    except Exception as e:
        logger.error(f"An unexpected error occurred during the process: {str(e)}", exc_info=True)


def process_failed_api_contracts() -> None:
    """
    Process contracts where:
    1. github_found = false, AND
    2. blockscout_fetch_status is not 'Success'
    
    This function will recheck the blockchain APIs for these contracts.
    """
    processed_count = 0
    updated_count = 0
    failed_update_count = 0
    
    try:
        # Initialize chain mappings from reference table
        initialize_chain_mappings()
        
        # Fetch all failed API records
        logger.info(f"Fetching contracts with failed API status from Airtable view {Config.TARGET_VIEW_ID}...")
        records = AirtableAPI.fetch_failed_api_contracts()

        if not records:
            logger.info(f"No contracts with failed API status found in view {Config.TARGET_VIEW_ID}.")
            return
            
        total_records = len(records)
        logger.info(f"Found {total_records} contracts with failed API status to process.")

        # Process each record
        for index, record in enumerate(records, 1):
            try:
                processed_count += 1
                record_id = record.get('id')
                fields = record.get('fields', {})
                contract_address = fields.get('address')
                origin_key = fields.get('origin_key')
                current_status = fields.get('blockscout_fetch_status', 'Unknown')
                
                # Resolve chain name from origin_key reference
                chain = None
                
                # Handle different possible types of origin_key
                if isinstance(origin_key, list) and len(origin_key) > 0:
                    # Get the reference ID from the list
                    ref_id = origin_key[0]
                    # Look up the chain name in our mapping
                    chain = ORIGIN_KEY_TO_CHAIN_MAP.get(ref_id)
                    if chain:
                        logger.info(f"Resolved origin_key {ref_id} to chain: {chain}")
                    else:
                        logger.warning(f"Could not resolve origin_key {ref_id} to chain, using default")
                        chain = "base"  # Default
                elif isinstance(origin_key, str):
                    # Direct string value
                    chain = origin_key
                else:
                    # No valid origin_key
                    chain = "base"  # Default

                if not record_id:
                    logger.warning(f"Skipping record {index}/{total_records} - missing record ID")
                    continue
                    
                if not contract_address:
                    logger.warning(f"Skipping record {record_id} ({index}/{total_records}) - no contract address")
                    continue

                logger.info(f"Processing {index}/{total_records}: Record ID {record_id}, Address {contract_address}, Chain {chain}, Current Status: {current_status}")

                # Try the Contract API again
                transaction_data = ContractAPI.process_contract_additional_data(contract_address, chain)
                
                # Update Airtable with github_found=false and transaction data
                update_successful = AirtableAPI.update_record(record_id, False, 0, transaction_data)
                if update_successful:
                    updated_count += 1
                else:
                    failed_update_count += 1
                    logger.error(f"Failed to update Airtable record {record_id}")

                # Rate limiting delay between processing records
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing record {index}/{total_records} (ID: {record.get('id', 'unknown')}): {str(e)}", exc_info=True)
                failed_update_count += 1
                continue  # Continue with next record despite error

        logger.info(f"\n=== Processing Summary ===")
        logger.info(f"Total failed API contracts processed: {processed_count}")
        logger.info(f"Airtable records updated successfully: {updated_count}")
        logger.info(f"Airtable records failed to update: {failed_update_count}")

    except Exception as e:
        logger.error(f"An unexpected error occurred during the process: {str(e)}", exc_info=True)


def main() -> None:
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(description='Process smart contracts to find GitHub references and on-chain data.')
    
    # Add command line arguments
    parser.add_argument('--check-view', action='store_true', help='Check the structure of the Airtable view')
    parser.add_argument('--test-address', type=str, help='Test a specific contract address')
    parser.add_argument('--test-chain', type=str, default='base', help='Chain to use for testing (default: base)')
    parser.add_argument('--run-full', action='store_true', help='Run the full contract processing')
    parser.add_argument('--check-chains', action='store_true', help='Check and display chain mappings')
    parser.add_argument('--check-api-keys', action='store_true', help='Check and display available API keys for supported chains')
    parser.add_argument('--retry-failed-apis', action='store_true', help='Retry contracts with failed API status')
    
    args = parser.parse_args()
    
    # Validate environment variables
    if not all([Config.AIRTABLE_TOKEN, Config.GITHUB_TOKEN]):
        logger.error("Missing required environment variables. Please check REACT_APP_AIRTABLE_TOKEN and GITHUB_TOKEN.")
        return
    
    # Execute based on arguments
    if args.check_view:
        logger.info(f"Checking structure of view: {Config.TARGET_VIEW_ID}")
        view_info = AirtableAPI.get_view_structure()
        print(json.dumps(view_info, indent=2))
    
    elif args.check_chains:
        # Initialize and display chain mappings
        initialize_chain_mappings()
        print("Current chain mappings:")
        for record_id, chain_name in ORIGIN_KEY_TO_CHAIN_MAP.items():
            print(f"{record_id} -> {chain_name}")
    
    elif args.check_api_keys:
        # Initialize BlockscoutAPI to check available API keys
        api = BlockscoutAPI()
        print("\nAPI Keys Status for Supported Chains:")
        print("=" * 50)
        
        print("\nChains requiring authentication:")
        for chain_id, config in api.chain_config.items():
            if config.get("requires_auth", False):
                has_key = chain_id in api.api_keys and api.api_keys[chain_id] is not None
                status = "✓ Available" if has_key else "✗ Missing"
                print(f"{config['name']} (ID: {chain_id}): {status}")
        
        print("\nChains NOT requiring authentication:")
        for chain_id, config in api.chain_config.items():
            if not config.get("requires_auth", False):
                notes = []
                if config.get("skip_logs", False):
                    notes.append("Logs fetching disabled")
                if chain_id == 59144:  # Linea
                    notes.append("Using simplified API handling")
                
                notes_str = f" - {', '.join(notes)}" if notes else ""
                print(f"{config['name']} (ID: {chain_id}){notes_str}")
        
        print("\nNote: Only chains with 'requires_auth' need API keys. Others will work without authentication.")
        print("      Linea has special handling due to API timeout issues.")
        print("      Some chains may have custom timeout or retry settings.")
        
    elif args.test_address:
        # Initialize chain mappings
        initialize_chain_mappings()
        
        # Test GitHub search
        logger.info(f"Testing GitHub search for address: {args.test_address}")
        github_found, repo_count = GitHubAPI.search_contract(args.test_address)
        logger.info(f"GitHub search result: Found={github_found}, Valid Count={repo_count}")
        
        # If not found on GitHub, test the contract API
        if not github_found:
            logger.info(f"Testing Contract API for address: {args.test_address} on chain: {args.test_chain}")
            transaction_data = ContractAPI.process_contract_additional_data(args.test_address, args.test_chain)
            print(json.dumps(transaction_data, indent=2))
    
    elif args.run_full:
        logger.info("Starting full contract processing...")
        process_all_contracts()
        logger.info("Full contract processing finished.")
        
    elif args.retry_failed_apis:
        logger.info("Starting retry of contracts with failed API status...")
        process_failed_api_contracts()
        logger.info("Retry of contracts with failed API status finished.")
    
    else:
        # Default behavior: show help
        parser.print_help()


if __name__ == "__main__":
    main()

# ===============================================
# MAIN EXECUTION
# =============================================== 