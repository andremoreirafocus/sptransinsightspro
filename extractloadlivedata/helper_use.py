"""
Example usage of processed_requests_helper functions.

This module demonstrates how to use the helper functions to:
1. Fetch unprocessed requests
2. Mark a request as processed
"""

from src.config import get_config
from datetime import datetime
from src.services.processed_requests_helper import (
    get_unprocessed_requests,
    mark_request_as_processed,
    mark_request_as_processed_by_filename,
)


def example_workflow(config):
    """
    Example workflow showing how to use the processed_requests_helper functions.

    Args:
        config: Configuration dictionary with environment variables
    """
    # Step 1: Get all unprocessed requests
    print("\n=== Step 1: Fetching unprocessed requests ===")
    unprocessed = get_unprocessed_requests(config)

    if unprocessed:
        print(f"Found {len(unprocessed)} unprocessed request(s):")
        for request in unprocessed:
            print(f"  - ID: {request['id']}")
            print(f"    Filename: {request['filename']}")
            print(f"    Logical Date: {request['logical_date']}")
            print(f"    Processed: {request['processed']}")
            print(f"    Created At: {request['created_at']}")
            print()
    else:
        print("No unprocessed requests found.")
    # Step 2: Process a specific request and mark it as processed
    if unprocessed:
        print("\n=== Step 2: Marking a request as processed ===")
        # Get the logical_date of the first unprocessed request
        first_request = unprocessed[0]
        logical_date = first_request["logical_date"]

        print(f"Marking request with logical_date={logical_date} as processed...")
        success = mark_request_as_processed(config, logical_date)

        if success:
            print("✓ Successfully marked request as processed")
        else:
            print("✗ Failed to mark request as processed")

    # Step 3: Verify the update by fetching unprocessed requests again
    print("\n=== Step 3: Verifying the update ===")
    unprocessed_after = get_unprocessed_requests(config)
    print(f"Unprocessed requests after update: {len(unprocessed_after)}")
    if len(unprocessed_after) < len(unprocessed):
        print("✓ Request successfully marked as processed!")
    else:
        print("✗ No change in unprocessed requests count")


def example_workflow_by_filename(config):
    """
    Example workflow showing how to use the processed_requests_helper functions with filename.

    Args:
        config: Configuration dictionary with environment variables
    """
    # Step 1: Get all unprocessed requests
    print("\n=== Step 1: Fetching unprocessed requests (by filename workflow) ===")
    unprocessed = get_unprocessed_requests(config)

    if unprocessed:
        print(f"Found {len(unprocessed)} unprocessed request(s):")
        for request in unprocessed:
            print(f"  - ID: {request['id']}")
            print(f"    Filename: {request['filename']}")
            print(f"    Logical Date: {request['logical_date']}")
            print(f"    Processed: {request['processed']}")
            print(f"    Created At: {request['created_at']}")
            print()
    else:
        print("No unprocessed requests found.")

    # Step 2: Process a specific request and mark it as processed by filename
    if unprocessed:
        print("\n=== Step 2: Marking a request as processed by filename ===")
        # Get the filename of the first unprocessed request
        first_request = unprocessed[0]
        filename = first_request["filename"]

        print(f"Marking request with filename={filename} as processed...")
        success = mark_request_as_processed_by_filename(config, filename)

        if success:
            print("✓ Successfully marked request as processed")
        else:
            print("✗ Failed to mark request as processed")

    # Step 3: Verify the update by fetching unprocessed requests again
    print("\n=== Step 3: Verifying the update ===")
    unprocessed_after = get_unprocessed_requests(config)
    print(f"Unprocessed requests after update: {len(unprocessed_after)}")
    if len(unprocessed_after) < len(unprocessed):
        print("✓ Request successfully marked as processed!")
    else:
        print("✗ No change in unprocessed requests count")


def example_single_mark_operation(config, logical_date_str):
    """
    Example of marking a single request as processed.

    Args:
        config: Configuration dictionary with environment variables
        logical_date_str: ISO format datetime string (e.g., "2026-02-25T15:30:00+00:00")
    """
    print(f"\nMarking request as processed for logical_date: {logical_date_str}")

    # Parse the logical_date string to datetime if needed
    try:
        logical_date = datetime.fromisoformat(logical_date_str)
    except ValueError:
        print(f"Invalid datetime format: {logical_date_str}")
        return

    # Mark the request as processed
    success = mark_request_as_processed(config, logical_date)

    if success:
        print("✓ Request marked as processed")
    else:
        print("✗ Failed to mark request as processed")


if __name__ == "__main__":
    config = get_config()
    # Example 1: Run the full workflow using logical_date
    # example_workflow(config)

    # Example 2: Run the full workflow using filename (uncomment to use)
    example_workflow_by_filename(config)

    # Example 3: Mark a specific request by logical_date (uncomment to use)
    example_single_mark_operation(config, "2026-02-25T23:14:00+00:00")
