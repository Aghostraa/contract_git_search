#!/usr/bin/env python3

import os
import json
import warnings
from web3 import Web3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Suppress SSL warnings 
warnings.filterwarnings("ignore", category=Warning)

# ERC20 Token Interface - minimal ABI for 'approve' function
ERC20_ABI = json.loads('''
[
    {
        "constant": false,
        "inputs": [
            {
                "name": "_spender",
                "type": "address"
            },
            {
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "approve",
        "outputs": [
            {
                "name": "",
                "type": "bool"
            }
        ],
        "payable": false,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [
            {
                "name": "_owner",
                "type": "address"
            },
            {
                "name": "_spender",
                "type": "address"
            }
        ],
        "name": "allowance",
        "outputs": [
            {
                "name": "",
                "type": "uint256"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    }
]
''')

def approve_token(token_address, spender_address, amount, private_key):
    """
    Approve a spender to use a specific amount of tokens on behalf of the token holder
    
    Args:
        token_address (str): Address of the ERC20 token contract
        spender_address (str): Address of the spender (contract that will use the tokens)
        amount (int): Amount of tokens to approve (in wei)
        private_key (str): Private key of the token holder (your wallet)
        
    Returns:
        str: Transaction hash
    """
    try:
        # Connect to an Ethereum/Taiko node
        eth_provider_url = os.getenv("ETH_PROVIDER_URL", "")
        taiko_api_key = os.getenv("TAIKO_API_KEY", "")
        
        # Check if we have a Taiko API key
        if taiko_api_key and ("taiko" in eth_provider_url.lower() or "taikoscan" in eth_provider_url.lower()):
            # If we're using a Taiko provider, add the API key
            if "?" in eth_provider_url:
                provider_url = f"{eth_provider_url}&apikey={taiko_api_key}"
            else:
                provider_url = f"{eth_provider_url}?apikey={taiko_api_key}"
            w3 = Web3(Web3.HTTPProvider(provider_url))
            print(f"Using Taiko provider with API key")
        elif eth_provider_url:
            # Use the provider URL from the .env file
            w3 = Web3(Web3.HTTPProvider(eth_provider_url))
            print(f"Using network provider: {eth_provider_url}")
        else:
            # Use Taiko provider as fallback
            default_taiko_url = "https://rpc.taiko.xyz"
            provider_url = f"{default_taiko_url}?apikey={taiko_api_key}" if taiko_api_key else default_taiko_url
            w3 = Web3(Web3.HTTPProvider(provider_url))
            print("Using default Taiko endpoint")
            
        # Check connection
        if not w3.is_connected():
            raise Exception("Failed to connect to blockchain node")
            
        # Get the wallet address from the private key
        account = w3.eth.account.from_key(private_key)
        wallet_address = account.address
        
        print(f"Connected to blockchain node. Account: {wallet_address}")
        print(f"Current network chain ID: {w3.eth.chain_id}")
        
        # Create contract instance
        token_contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
        
        # Check current allowance
        current_allowance = token_contract.functions.allowance(
            wallet_address, 
            Web3.to_checksum_address(spender_address)
        ).call()
        
        print(f"Current allowance: {current_allowance}")
        
        if current_allowance >= amount:
            print(f"You already have sufficient allowance ({current_allowance}). No need to approve more.")
            return None
        
        # Check Web3.py version by inspecting the transaction object's structure
        # Web3.py v7 uses a different transaction handling approach
        try:
            # Build the approve transaction
            approve_txn = token_contract.functions.approve(
                Web3.to_checksum_address(spender_address),
                amount
            ).build_transaction({
                'from': wallet_address,
                'nonce': w3.eth.get_transaction_count(wallet_address),
                'gas': 100000,  # gas limit
                'gasPrice': w3.eth.gas_price,
                'chainId': w3.eth.chain_id
            })
            
            # Sign and send using Web3.py Account object
            signed_txn = w3.eth.account.sign_transaction(approve_txn, private_key)
            
            # Get the raw transaction based on the Web3.py version
            if hasattr(signed_txn, 'rawTransaction'):
                # Web3.py v6 style
                raw_tx = signed_txn.rawTransaction
            elif hasattr(signed_txn, 'raw_transaction'):
                # Web3.py v7 style
                raw_tx = signed_txn.raw_transaction
            else:
                # Try direct approach for v7
                raw_tx = signed_txn
                
            # Send the transaction
            tx_hash = w3.eth.send_raw_transaction(raw_tx)
            print(f"Approval transaction submitted: {tx_hash.hex()}")
            
            # If we're on a Taiko network, use taikoscan.io regardless of chain ID
            explorer_url = "https://taikoscan.io"
            print(f"Track your transaction at: {explorer_url}/tx/{tx_hash.hex()}")
            
            # Wait for receipt (optional)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"Transaction status: {'Success' if receipt.status == 1 else 'Failed'}")
            
            return tx_hash.hex()
            
        except Exception as e:
            print(f"Transaction error: {str(e)}")
            
            # Try alternative approach for Web3.py v7
            print("Trying alternative Web3.py v7 approach...")
            
            # Use the contract's functions directly with the account object
            tx = token_contract.functions.approve(
                Web3.to_checksum_address(spender_address),
                amount
            ).transact({'from': wallet_address, 'gas': 100000})
            
            print(f"Approval transaction submitted: {tx.hex()}")
            explorer_url = "https://taikoscan.io"
            print(f"Track your transaction at: {explorer_url}/tx/{tx.hex()}")
            
            receipt = w3.eth.wait_for_transaction_receipt(tx)
            print(f"Transaction status: {'Success' if receipt.status == 1 else 'Failed'}")
            
            return tx.hex()
            
    except Exception as e:
        print(f"Error: {str(e)}")
        print("Debug info:")
        print(f"Token address: {token_address}")
        print(f"Spender address: {spender_address}")
        print(f"Amount: {amount}")
        return None

def main():
    # These values should be provided by the user
    print("ERC20 Token Approval Script")
    print("---------------------------")
    
    # Extract data from the error message
    spender_contract = "0xA35f53a71FA6cd7AC9Df7f7814ecBc49dF255A38"  # Contract you're interacting with
    wallet_address = "0x00148E2eD9C1F1b1E6444F6eD50646DEa7F80867"    # Your wallet address
    
    # Get the token address from user
    token_address = input("Enter the ERC20 token address: ")
    
    # Amount options
    print("\nAmount Options:")
    print("1. Use amount from transaction data (0x38d7ea4c68000 = 1000000000000000 wei)")
    print("2. Enter custom amount")
    amount_choice = input("Choose option (1/2): ")
    
    if amount_choice == "1":
        # Transaction data amount: 0x38d7ea4c68000
        amount = int("0x38d7ea4c68000", 16)  # Convert hex to decimal
        print(f"Using amount: {amount} wei")
    else:
        amount = int(input("Enter the amount to approve (in wei, as a decimal number): "))
    
    # Get network selection for Taiko only
    print("\nTaiko Network Options:")
    print("1. Taiko Mainnet")
    print("2. Taiko Katla Testnet")
    print("3. Taiko Jolnir Testnet")
    network_choice = input("Choose Taiko network (1-3): ")
    
    network_urls = {
        "1": "https://rpc.taiko.xyz",
        "2": "https://rpc.katla.taiko.xyz",
        "3": "https://rpc.jolnir.taiko.xyz"
    }
    
    # Update the .env file with the selected network
    network_url = network_urls.get(network_choice, network_urls["1"])
    
    # Get API key from .env or ask user
    taiko_api_key = os.getenv("TAIKO_API_KEY", "")
    if not taiko_api_key:
        taiko_api_key = input("Enter your Taiko API key: ")
        with open(".env", "a") as env_file:
            env_file.write(f"\nTAIKO_API_KEY={taiko_api_key}\n")
    
    # Update the .env file with the network URL
    with open(".env", "a") as env_file:
        env_file.write(f"\nETH_PROVIDER_URL={network_url}\n")
    
    # Get private key
    private_key = input("\nEnter your private key (will not be stored): ")
    
    # Execute the approval
    print(f"\nApproving {amount} tokens from contract {token_address}")
    print(f"Spender: {spender_contract}")
    print(f"Network: {network_url}")
    
    tx_hash = approve_token(token_address, spender_contract, amount, private_key)
    
    if tx_hash:
        print(f"\nApproval successful! Transaction hash: {tx_hash}")
        print(f"View transaction on Taiko Explorer: https://taikoscan.io/tx/{tx_hash}")
    else:
        print("\nApproval failed or was not needed.")

if __name__ == "__main__":
    main() 