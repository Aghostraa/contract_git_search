import json
import pandas as pd
import yaml
import os
import anthropic
from typing import List, Dict
import re

class ContractClassifierMCP:
    def __init__(self, categories_path="category_definitions.yml"):
        # Initialize Anthropic client
        self.client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
        self.categories = self._load_categories(categories_path)
        
    def _load_categories(self, path):
        with open(path, "r") as f:
            return yaml.safe_load(f)
    
    def create_context(self, contracts: List[Dict]) -> str:
        """Create a structured context for Claude"""
        # Format category definitions in a condensed, easy-to-reference format
        category_context = ""
        for main_cat, details in self.categories.items():
            if main_cat == "version":
                continue
                
            category_context += f"## {details['main_category_name']}\n"
            for category in details.get('categories', []):
                category_context += f"- {category['category_id']}: {category['name']} - {category['description']}\n"
        
        # Format contract information
        contracts_context = json.dumps(contracts, indent=2)
        
        # Complete context with task description
        context = f"""
# Contract Classification Task

## Categories Reference
{category_context}

## Contracts to Classify
{contracts_context}

Your task is to classify each contract into the most appropriate category based on its name and address.
For each contract, provide the category_id that best matches its likely purpose.
"""
        return context
    
    def classify_contracts(self, chain_id: str, contracts: List[Dict]) -> List[Dict]:
        """Classify contracts using structured prompting with Claude"""
        # Create MCP context
        context = self.create_context(contracts)
        
        # Format the system message
        system_message = """
You are a blockchain contract classifier. Your job is to categorize smart contracts based on their names and purposes.
Respond with a JSON object where keys are contract addresses and values are the appropriate category_id.
Only return the JSON object, no additional explanation.
"""
        
        # Call Claude with context
        response = self.client.messages.create(
            model="claude-3-sonnet-20240229",
            system=system_message,
            max_tokens=2000,
            messages=[
                {"role": "user", "content": context}
            ]
        )
        
        # Extract the JSON response
        try:
            # Look for JSON in the response
            content = response.content[0].text
            # Extract JSON using simple heuristic (between { and })
            json_start = content.find('{')
            json_end = content.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                json_text = content[json_start:json_end]
                classifications = json.loads(json_text)
                
                # Apply classifications to the original contracts
                for contract in contracts:
                    contract_address = contract["Address"]
                    if contract_address in classifications:
                        contract["Usage Category"] = classifications[contract_address]
            else:
                print("Error: Could not find JSON in Claude's response")
            
            return contracts
            
        except json.JSONDecodeError:
            print("Error: Claude didn't return valid JSON")
            return contracts
    
    def process_batch(self, chain_id: str, addresses: List[str], project_name: str, 
                     contract_names: List[str] = None) -> pd.DataFrame:
        """Process a batch of contracts from start to finish"""
        # Create initial contract records
        contracts = []
        for i, address in enumerate(addresses):
            name = contract_names[i] if contract_names and i < len(contract_names) else f"Contract-{address[:8]}"
            contracts.append({
                "Chain": chain_id,
                "Address": address,
                "Contract Name": name,
                "Owner Project": project_name,
                "Usage Category": "unknown",
                "Is Contract": "true"
            })
        
        # Classify the contracts
        classified_contracts = self.classify_contracts(chain_id, contracts)
        
        # Convert to DataFrame
        return pd.DataFrame(classified_contracts)
    
    def save_to_csv(self, df: pd.DataFrame, output_path: str = "classified_contracts.csv"):
        """Save classification results to CSV"""
        df.to_csv(output_path, index=False)
        print(f"Results saved to {output_path}")

    def preprocess_input(self, input_data, format_type="list"):
        """Normalize different input formats to consistent address/name lists"""
        addresses = []
        names = []
        
        if format_type == "list":
            # Already in expected format
            return input_data.get("addresses", []), input_data.get("names", [])
        
        elif format_type == "csv":
            # Parse from CSV file
            df = pd.read_csv(input_data["file_path"])
            addresses = df[input_data.get("address_column", "Address")].tolist()
            names = df[input_data.get("name_column", "Contract Name")].tolist() if "name_column" in input_data else []
            
        elif format_type == "text":
            # Parse from text list (one address per line)
            lines = input_data["text"].strip().split("\n")
            for line in lines:
                # Try to extract name if in format "Name: 0x..."
                if ":" in line:
                    parts = line.split(":", 1)
                    names.append(parts[0].strip())
                    addresses.append(parts[1].strip())
                else:
                    addresses.append(line.strip())
                    # Generate placeholder name
                    names.append(f"Contract-{line.strip()[:8]}")
                
        elif format_type == "js_addresses":
            # Process JavaScript/TypeScript address exports
            js_text = input_data["text"]
            
            # Extract addresses from JS object
            # This is a simplified approach - for complex JS objects, consider using a JS parser
            address_pattern = r"'(0x[a-fA-F0-9]{40})'"
            extracted_addresses = re.findall(address_pattern, js_text)
            
            # Remove duplicates while preserving order
            unique_addresses = []
            for addr in extracted_addresses:
                if addr not in unique_addresses:
                    unique_addresses.append(addr)
            
            # Generate names based on context
            for addr in unique_addresses:
                # Find context for this address
                # Look for nearby identifiers in the text
                addr_pos = js_text.find(addr)
                context_start = max(0, addr_pos - 100)
                context_end = min(len(js_text), addr_pos + 100)
                context = js_text[context_start:context_end]
                
                # Extract key identifiers
                keys = re.findall(r'(\w+):', context)
                if keys:
                    name = f"{keys[-1]}-{addr[:6]}"
                else:
                    name = f"Contract-{addr[:8]}"
                
                addresses.append(addr)
                names.append(name)
                
        elif format_type == "explorer_api":
            # Placeholder for getting data from explorer APIs
            # Would implement API calls to etherscan, etc.
            pass
        
        return addresses, names

    def preprocess_with_ai(self, input_data):
        """Use AI to extract contract addresses and names from any input format"""
        text = input_data.get("text", "")
        if not text and "file_path" in input_data:
            with open(input_data["file_path"], "r") as f:
                text = f.read()
        
        # Use Claude to extract contract addresses and names
        prompt = f"""
Extract all Ethereum contract addresses (0x...) from the following text, along with appropriate names for each contract.

For each address:
1. Look for contextual clues to assign a meaningful name
2. If multiple instances of the same address exist, only include it once
3. Return the results as a JSON array of objects with "address" and "name" fields

Input:
{text}

Output only valid JSON without any explanation:
"""
        
        system_message = """
You are a specialized extraction tool that processes blockchain-related text and extracts contract addresses with their names.
Always return a valid JSON array, even if empty. Format: [{"address": "0x123...", "name": "ContractName"}, ...]
Never include explanatory text outside the JSON.
"""
        
        try:
            response = self.client.messages.create(
                model="claude-3-sonnet-20240229",
                system=system_message,
                max_tokens=2000,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Extract the JSON response
            content = response.content[0].text
            result = json.loads(content)
            
            # Extract addresses and names
            addresses = [item["address"] for item in result]
            names = [item["name"] for item in result]
            
            return addresses, names
            
        except Exception as e:
            print(f"Error extracting addresses: {e}")
            # Fallback to regex extraction if AI extraction fails
            address_pattern = r"0x[a-fA-F0-9]{40}"
            addresses = list(set(re.findall(address_pattern, text)))
            names = [f"Contract-{addr[:8]}" for addr in addresses]
            return addresses, names

    def process_input(self, input_data):
        """Process any input format and classify contracts"""
        # Extract contract addresses and names using AI
        addresses, names = self.preprocess_with_ai(input_data)
        
        # Determine chain ID from context or use default
        chain_id = input_data.get("chain_id", "eip155:1")  # Default to Ethereum
        project_name = input_data.get("project", "unknown")
        
        # Process the batch
        results_df = self.process_batch(chain_id, addresses, project_name, names)
        
        # Save to CSV if output path is provided
        if "output_path" in input_data:
            self.save_to_csv(results_df, input_data["output_path"])
        
        return results_df

# Usage example
if __name__ == "__main__":
    # Initialize the MCP classifier
    classifier = ContractClassifierMCP()
    
    # Example batch of contracts
    chain_id = "eip155:8453"  # Base chain
    project = "moonwell-fi"
    addresses = [
        "0xfBb21d0380beE3312B33c4353c8936a0F13EF26C",
        "0x8b621804a7637b781e2BbD58e256a591F2dF7d51",
    ]
    names = [
        "Comptroller",
        "Temporal Governor"
    ]
    
    # Process the batch
    results_df = classifier.process_batch(chain_id, addresses, project, names)
    classifier.save_to_csv(results_df)
