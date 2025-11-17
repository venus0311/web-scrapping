from typing import Dict, Any
from datetime import datetime, timedelta
import asyncio

# Global in-memory storage for processed data
process_data_cache: Dict[str, Dict[str, Any]] = {}


def store_processed_data(entry_id: str, data: dict):
    """
    Store processed data in cache
    """
    process_data_cache[entry_id] = {
        **data,
        "timestamp": datetime.now().isoformat()
    }
    print(f"📊 Processed data stored for frontend - Entry: {entry_id}")


def get_processed_data(entry_id: str) -> Dict[str, Any]:
    """
    Get processed data from cache
    """
    return process_data_cache.get(entry_id)


def delete_processed_data(entry_id: str):
    """
    Delete processed data from cache
    """
    if entry_id in process_data_cache:
        del process_data_cache[entry_id]
        print(f"🧹 Cleaned up cache for entry: {entry_id}")


async def cleanup_old_cache_entries():
    """
    Clean up cache entries older than 30 minutes
    """
    while True:
        await asyncio.sleep(1800)  # Run every 30 minutes
        current_time = datetime.now()
        keys_to_delete = []
        
        for entry_id, data in process_data_cache.items():
            timestamp = datetime.fromisoformat(data["timestamp"])
            if current_time - timestamp > timedelta(minutes=30):
                keys_to_delete.append(entry_id)
        
        for key in keys_to_delete:
            del process_data_cache[key]
            print(f"🧹 Cleaned up old cache entry: {key}")


def get_entry_name(entry_id: str) -> str:
    """
    Get entry name from cache
    """
    data = process_data_cache.get(entry_id)
    return data.get("entry_name", "") if data else ""


def get_cache_debug_info():
    """
    Get debug information about the cache
    """
    return {
        "cache_size": len(process_data_cache),
        "cache_keys": list(process_data_cache.keys()),
        "cache_data": process_data_cache
    }