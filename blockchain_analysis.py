import requests
import json
import os
import time
from typing import Dict, List, Optional, Set, Tuple
import urllib.parse
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("blockchain_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Chain URLs configuration
CHAIN_BLOCKSPACE_URLS = {
    "arbitrum": "https://api.growthepie.xyz/v1/chains/blockspace/arbitrum.json",
    "polygon_zkevm": "https://api.growthepie.xyz/v1/chains/blockspace/polygon_zkevm.json",
    "optimism": "https://api.growthepie.xyz/v1/chains/blockspace/optimism.json",
    "zksync_era": "https://api.growthepie.xyz/v1/chains/blockspace/zksync_era.json",
    "base": "https://api.growthepie.xyz/v1/chains/blockspace/base.json",
    "zora": "https://api.growthepie.xyz/v1/chains/blockspace/zora.json",
    "linea": "https://api.growthepie.xyz/v1/chains/blockspace/linea.json",
    "scroll": "https://api.growthepie.xyz/v1/chains/blockspace/scroll.json",
    "mantle": "https://api.growthepie.xyz/v1/chains/blockspace/mantle.json",
    "mode": "https://api.growthepie.xyz/v1/chains/blockspace/mode.json",
    "taiko": "https://api.growthepie.xyz/v1/chains/blockspace/taiko.json",
    "swell": "https://api.growthepie.xyz/v1/chains/blockspace/swell.json"
}

# Airtable configuration
AIRTABLE_BASE_URL = 'https://api.airtable.com/v0/appZWDvjvDmVnOici'
CONTRACTS_TABLE = 'tblcXnFAf0IEvAQA6'
ORIGIN_KEYS_TABLE = 'tblK3YcdB8jaFtMgS'
AIRTABLE_TOKEN = os.getenv('REACT_APP_AIRTABLE_TOKEN')  # Set as environment variable

# Airtable headers
AIRTABLE_HEADERS = {
    'Authorization': f'Bearer {AIRTABLE_TOKEN}',
    'Content-Type': 'application/json'
}

# Threshold for unlabeled shares (percentage)
THRESHOLD = 30

# Time ranges to check
TIME_RANGES = ['7d', '30d']

# Map from time range to day value for day_range field
TIME_RANGE_TO_DAY = {
    '7d': 7,
    '30d': 30
}


def get_origin_key_record_id(chain: str) -> Optional[str]:
    """Get the record ID for a specific origin_key from the origin_keys table."""

    
    # URL encode the filter formula
    filter_formula = f"{{origin_key}} = '{chain}'"
    encoded_filter = urllib.parse.quote(filter_formula)
    
    url = f"{AIRTABLE_BASE_URL}/{ORIGIN_KEYS_TABLE}?filterByFormula={encoded_filter}"
    
    try:
        response = requests.get(url, headers=AIRTABLE_HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'records' in data and len(data['records']) > 0:
            record_id = data['records'][0]['id']
            return record_id
        
        logger.warning(f"No record found for origin_key: {chain}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching origin_key record ID for {chain}: {str(e)}")
        return None


def fetch_chain_data(chain: str) -> Dict:
    """Fetch data for a specific chain from the API."""
    url = CHAIN_BLOCKSPACE_URLS.get(chain)
    if not url:
        logger.warning(f"No URL found for chain {chain}")
        return {}
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data for {chain}: {str(e)}")
        return {}


def analyze_chain_data(chain_data: Dict, chain: str) -> Dict:
    """
    Analyze chain data to check if unlabeled share exceeds threshold.
    Returns details if threshold is exceeded, empty dict otherwise.
    """
    result = {
        'chain': chain,
        'exceeded': False,
        'exceeded_ranges': [],
        'unlabeled_contracts': []
    }
    
    # Check if the data structure is as expected
    if not chain_data or 'overview' not in chain_data:
        return result
    
    overview = chain_data.get('overview', {})
    
    for time_range in TIME_RANGES:
        if time_range not in overview:
            continue
        
        range_data = overview[time_range]
        if 'unlabeled' not in range_data:
            continue
        
        unlabeled = range_data['unlabeled']
        types = overview.get('types', [])
        
        # Find indexes for txcount_share and gas_fees_share
        tx_share_idx = -1
        fees_share_eth_idx = -1
        fees_share_usd_idx = -1
        
        for i, t in enumerate(types):
            if t == 'txcount_share':
                tx_share_idx = i
            elif t == 'gas_fees_share_eth':
                fees_share_eth_idx = i
            elif t == 'gas_fees_share_usd':
                fees_share_usd_idx = i
        
        if tx_share_idx == -1 and fees_share_eth_idx == -1 and fees_share_usd_idx == -1:
            continue
        
        data = unlabeled.get('data', [])
        contracts = unlabeled.get('contracts', {}).get('data', [])
        contract_types = unlabeled.get('contracts', {}).get('types', [])
        
        # Extract shares and convert to percentages
        tx_share = data[tx_share_idx] * 100 if 0 <= tx_share_idx < len(data) else 0
        fees_share_eth = data[fees_share_eth_idx] * 100 if 0 <= fees_share_eth_idx < len(data) else 0
        fees_share_usd = data[fees_share_usd_idx] * 100 if 0 <= fees_share_usd_idx < len(data) else 0
        
        # Check if any share exceeds threshold
        if tx_share > THRESHOLD or fees_share_eth > THRESHOLD or fees_share_usd > THRESHOLD:
            result['exceeded'] = True
            result['exceeded_ranges'].append({
                'time_range': time_range,
                'day_range': TIME_RANGE_TO_DAY[time_range],
                'tx_share': tx_share,
                'fees_share_eth': fees_share_eth,
                'fees_share_usd': fees_share_usd
            })
            
            # Extract top unlabeled contracts if available
            if contracts and contract_types:
                # Find indices for relevant contract fields
                addr_idx = contract_types.index('address') if 'address' in contract_types else -1
                tx_count_idx = contract_types.index('txcount_absolute') if 'txcount_absolute' in contract_types else -1
                gas_fees_eth_idx = contract_types.index('gas_fees_absolute_eth') if 'gas_fees_absolute_eth' in contract_types else -1
                
                # Extract contract details
                for contract in contracts:
                    if addr_idx >= 0 and tx_count_idx >= 0 and gas_fees_eth_idx >= 0:
                        result['unlabeled_contracts'].append({
                            'address': contract[addr_idx],
                            'tx_count': contract[tx_count_idx],
                            'gas_fees_eth': contract[gas_fees_eth_idx],
                            'time_range': time_range,
                            'day_range': TIME_RANGE_TO_DAY[time_range]
                        })
    
    # Sort contracts by transaction count (descending)
    if result['unlabeled_contracts']:
        result['unlabeled_contracts'] = sorted(
            result['unlabeled_contracts'], 
            key=lambda x: x['tx_count'], 
            reverse=True
        )
    
    return result


def get_existing_addresses_and_record_ids_from_airtable(origin_key: str, origin_key_record_id: str) -> Dict[str, Dict]:
    """
    Fetch all addresses already in Airtable for a given origin_key along with their record IDs.
    Returns a dictionary mapping lowercase addresses to their record details.
    """
    address_to_details = {}
    
    # Build filter formula for linked record field
    filter_formula = f"SEARCH('{origin_key_record_id}', ARRAYJOIN(origin_key))"
    encoded_filter = urllib.parse.quote(filter_formula)
    
    base_url = (
        f"{AIRTABLE_BASE_URL}/{CONTRACTS_TABLE}"
        f"?filterByFormula={encoded_filter}"
        f"&fields[]=address&fields[]=origin_key&fields[]=day_range&fields[]=asap"
    )
    
    offset = None
    page = 1
    while True:
        try:
            url = f"{base_url}&offset={offset}" if offset else base_url
            response = requests.get(url, headers=AIRTABLE_HEADERS, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            records_count = len(data.get('records', []))
            logger.info(f"[{origin_key}] Page {page}: Fetched {records_count} records. Total: {len(address_to_details) + records_count}")
            
            for record in data.get('records', []):
                record_id = record.get('id')
                fields = record.get('fields', {})
                address = fields.get('address', '')
                day_range = fields.get('day_range')
                asap = fields.get('asap', False)
                
                if address and record_id:
                    address_to_details[address.lower()] = {
                        'record_id': record_id, 
                        'day_range': day_range,
                        'asap': asap
                    }
            
            offset = data.get('offset')
            if not offset:
                break
                
            page += 1
            time.sleep(0.2)  # Respect Airtable rate limits
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching records for {origin_key}: {str(e)}")
            break
    
    logger.info(f"Found {len(address_to_details)} existing records for {origin_key} in Airtable")
    return address_to_details


def add_new_record_to_airtable(chain: str, contract: Dict, origin_key_record_id: str) -> bool:
    """
    Add a new record to Airtable and set the 'asap' field to checked.
    First checks if the contract already exists to avoid duplicates.
    """
    address = contract['address'].lower()
    
    # First check if the contract already exists
    filter_formula = f"AND(address = '{address}', SEARCH('{origin_key_record_id}', ARRAYJOIN(origin_key)))"
    encoded_filter = urllib.parse.quote(filter_formula)
    check_url = f"{AIRTABLE_BASE_URL}/{CONTRACTS_TABLE}?filterByFormula={encoded_filter}"
    
    try:
        response = requests.get(check_url, headers=AIRTABLE_HEADERS, timeout=30)
        response.raise_for_status()
        existing_records = response.json().get('records', [])
        
        if existing_records:
            logger.info(f"Contract {address} already exists in Airtable for this chain. Skipping creation.")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Error checking for existing contract {address}: {str(e)}")
        return False
    
    # If we get here, the contract doesn't exist, so create it
    url = f"{AIRTABLE_BASE_URL}/{CONTRACTS_TABLE}"
    
    # Make sure numeric values are proper numbers, not strings
    tx_count = int(contract['tx_count']) if isinstance(contract['tx_count'], str) else contract['tx_count']
    gas_fees_eth = float(contract['gas_fees_eth']) if isinstance(contract['gas_fees_eth'], str) else contract['gas_fees_eth']
    day_range = contract['day_range']  # This should be 7 or 30
    
    # Format the request with the linked record ID
    request_body = {
        "fields": {
            "address": address,
            "origin_key": [origin_key_record_id],  # Format as array for linked record
            "asap": True,  # Set the checkmark
            "txcount": tx_count,
            "gas_eth": gas_fees_eth,
            "day_range": day_range
        }
    }
    
    logger.debug(f"Add request body: {json.dumps(request_body)}")
    
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(url, headers=AIRTABLE_HEADERS, json=request_body, timeout=30)
            response.raise_for_status()
            logger.info(f"Added new record for {address} with origin_key {chain}")
            return True
        except requests.exceptions.RequestException as e:
            error_msg = f"Error adding record for {address} (Attempt {attempt}/{max_retries}): {str(e)}"
            if attempt == max_retries:
                logger.error(error_msg)
                # Try to extract the specific error from the response
                try:
                    error_detail = response.json().get('error', {})
                    logger.error(f"Error details: {json.dumps(error_detail)}")
                except Exception:
                    pass
            else:
                logger.warning(error_msg)
                time.sleep(2 ** attempt)  # Exponential backoff
    
    return False


def update_asap_field(record_id: str, address: str) -> bool:
    """
    Update the 'asap' field to checked for an existing record.
    """
    url = f"{AIRTABLE_BASE_URL}/{CONTRACTS_TABLE}/{record_id}"
    
    request_body = {
        "fields": {
            "asap": True  # Set the checkmark
        }
    }
    
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.patch(url, headers=AIRTABLE_HEADERS, json=request_body, timeout=30)
            response.raise_for_status()
            logger.info(f"Updated asap field for record {record_id} (address: {address})")
            return True
        except requests.exceptions.RequestException as e:
            error_msg = f"Error updating asap field for record {record_id} (address: {address}) (Attempt {attempt}/{max_retries}): {str(e)}"
            if attempt == max_retries:
                logger.error(error_msg)
            else:
                logger.warning(error_msg)
                time.sleep(2 ** attempt)  # Exponential backoff
    
    return False


def should_update_record(existing_details: Dict, new_day_range: int) -> bool:
    """
    Determine if we should update an existing record based on day_range and asap status.
    
    Rules:
    1. If asap is already checked (True), no need to update
    2. If existing day_range matches the new one, we should update asap
    3. For different day_ranges, update if the new one is more "urgent" (e.g., 7d instead of 30d)
    """
    if existing_details.get('asap', False):
        # Already marked as asap, no need to update
        return False
    
    existing_day_range = existing_details.get('day_range')
    
    if existing_day_range == new_day_range:
        # Same day_range, update to set asap
        return True
    
    # For different day_ranges, 7 is more urgent than 30
    if new_day_range < existing_day_range:
        return True
    
    return False


def main():
    """Main function to run the script."""
    logger.info("Starting blockchain unlabeled share analysis...")
    
    # Ensure AIRTABLE_TOKEN is set
    if not AIRTABLE_TOKEN:
        logger.error("AIRTABLE_TOKEN environment variable not set. Please set it before running this script.")
        return
    
    # Track counts for summary
    chains_checked = 0
    chains_exceeded = 0
    total_updated = 0
    total_added = 0
    
    # Cache for origin key record IDs
    origin_key_id_cache = {}
    
    for chain in CHAIN_BLOCKSPACE_URLS:
        chains_checked += 1
        logger.info(f"\nAnalyzing chain: {chain}")
        
        # Get the origin_key record ID (and cache it)
        if chain not in origin_key_id_cache:
            origin_key_id = get_origin_key_record_id(chain)
            if not origin_key_id:
                logger.error(f"Could not find record ID for origin_key '{chain}'. Skipping this chain.")
                continue
            origin_key_id_cache[chain] = origin_key_id
        else:
            origin_key_id = origin_key_id_cache[chain]
            logger.info(f"Using cached record ID for {chain}: {origin_key_id}")
        
        # Fetch and analyze chain data
        chain_data = fetch_chain_data(chain)
        analysis = analyze_chain_data(chain_data, chain)
        
        if not analysis['exceeded']:
            logger.info(f"Chain {chain} is below the {THRESHOLD}% threshold for unlabeled shares.")
            continue
        
        chains_exceeded += 1
        logger.info(f"Chain {chain} exceeded the {THRESHOLD}% threshold for unlabeled shares!")
        
        for range_info in analysis['exceeded_ranges']:
            time_range = range_info['time_range']
            logger.info(f"  - Time range: {time_range} (day_range: {range_info['day_range']})")
            logger.info(f"    TX share: {range_info['tx_share']:.2f}%")
            logger.info(f"    Fees share (ETH): {range_info['fees_share_eth']:.2f}%")
            logger.info(f"    Fees share (USD): {range_info['fees_share_usd']:.2f}%")
        
        # Get existing addresses with their record IDs for this chain from Airtable
        address_to_details = get_existing_addresses_and_record_ids_from_airtable(chain, origin_key_id)
        
        # Track changes for this chain
        chain_updated = 0
        chain_added = 0
        
        # Process top contracts
        processed_addresses = set()
        for idx, contract in enumerate(analysis['unlabeled_contracts']):
            address = contract['address'].lower()
            
            # Skip if we've already processed this address
            if address in processed_addresses:
                continue
            
            processed_addresses.add(address)
            
            # Only process up to 50 contracts per chain to avoid hitting API limits
            if idx >= 50:
                logger.info(f"Reached limit of 50 contracts for {chain}. Skipping remaining contracts.")
                break
            
            if address in address_to_details:
                # Address already exists, check if we need to update the asap field
                existing_details = address_to_details[address]
                record_id = existing_details.get('record_id')
                
                if record_id and should_update_record(existing_details, contract['day_range']):
                    if update_asap_field(record_id, address):
                        chain_updated += 1
                        total_updated += 1
                        logger.info(f"Updated asap for {address} with day_range {contract['day_range']}")
                else:
                    logger.info(f"Skipped updating {address} (already asap or lower priority day_range)")
            else:
                # Check if we've reached the limit of new contracts
                if chain_added >= 10:
                    logger.info(f"Reached limit of 10 new contracts for {chain}. Skipping remaining new contracts.")
                    continue
                    
                # Add new record
                if add_new_record_to_airtable(chain, contract, origin_key_id):
                    chain_added += 1
                    total_added += 1
                    logger.info(f"Added new contract {chain_added}/10 for {chain}")
            
            # Add a small delay between requests to avoid rate limiting
            time.sleep(0.1)
        
        logger.info(f"Summary for {chain}: Updated {chain_updated} existing records, added {chain_added} new records")
    
    # Log final summary
    logger.info("\n=== Analysis Summary ===")
    logger.info(f"Total chains analyzed: {chains_checked}")
    logger.info(f"Chains exceeding {THRESHOLD}% threshold: {chains_exceeded}")
    logger.info(f"Total records updated with 'asap' flag: {total_updated}")
    logger.info(f"Total new records added: {total_added}")
    logger.info("Analysis complete!")


if __name__ == "__main__":
    main()