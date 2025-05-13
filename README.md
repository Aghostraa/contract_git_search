# GitHub Contract Lookup

This script searches GitHub for contract addresses from a specific Airtable view and updates the `github_found` and `repo_count` fields based on the search results.

## Current Logic

The script:
1. Only looks at records from the view ID: `viwF2Xc24CGNO7u5C`
2. Fetches all contracts where the `repo_count` field is NULL (doesn't exist)
   - Important: Records with `repo_count` = 0 are considered already processed
3. Searches GitHub for each contract address
4. Updates the following fields for all processed records:
   - `github_found`: Set to `true` if GitHub results were found
   - `repo_count`: The total count of repositories found
5. Does not update the `repo_paths` field

## Setup

1. Make sure you have Python 3.6+ installed
2. Install required dependencies:
   ```
   python3 -m pip install requests python-dotenv
   ```
3. Set up environment variables in a `.env` file:
   ```
   REACT_APP_AIRTABLE_TOKEN=your_airtable_token
   GITHUB_TOKEN=your_github_token
   ```

## Usage

The script can be run in two ways:

### Process All Contracts

This will process all records with NULL `repo_count` in the target view:

```
python3 Script.py
```

or explicitly:

```
python3 Script.py --all
```

### Check View Structure

To check the structure of the target view:

```
python3 Script.py --check_view
```

## How It Works

1. The script fetches all records with NULL `repo_count` field from the target view
   - Records where `repo_count` is set to any value (including 0) are skipped
2. For each contract address, it searches GitHub for references
3. Every processed contract gets its `github_found` and `repo_count` fields updated
4. The `repo_count` field serves as a check to identify which contracts have already been queried

## Features

- **Contract Processing**: Fetches all unprocessed contracts (with NULL repo_count) in one batch
- **GitHub Search**: Finds repositories containing specific contract addresses
- **Airtable Updates**: Updates records with GitHub search results
- **View Info**: Provides simple context about an Airtable view's structure
- **Error Handling**: Includes retries and rate limit handling 