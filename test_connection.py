#!/usr/bin/env python3

import sys
import json
from mysqldb import MySQLDB

def test_connection():
    """Test the SSH tunnel and MySQL connection."""
    
    # Load config
    with open('config.json', 'r') as f:
        config_data = json.load(f)
    
    # Get database info
    db_info = config_data['databases']['pm-v3-api-service']
    print(f"Testing connection to {db_info['db_name']} at {db_info['ssh_host']}...")
    
    # Create MySQL connection
    database = MySQLDB(db_info)
    
    try:
        # Test connection
        print("Attempting to connect via SSH tunnel...")
        database.connect()
        print("✅ SSH tunnel and MySQL connection successful!")
        
        # Test basic query
        print("Testing basic query...")
        result = database.run_query("SHOW TABLES")
        print(f"✅ Found {len(result)} total tables:")
        
        for row in result.values:
            table_name = row[0]
            print(f"  - {table_name}")
            
        # Look for PM-related tables specifically
        pm_tables = []
        for row in result.values:
            table_name = row[0]
            if 'pum_' in table_name.lower() or 'optin' in table_name.lower() or 'review' in table_name.lower():
                pm_tables.append(table_name)
        
        if pm_tables:
            print(f"\n📍 Found {len(pm_tables)} PM-related tables:")
            for table in pm_tables:
                print(f"  - {table}")
        else:
            print("\n⚠️  No tables containing 'pum_', 'optin', or 'review' found")
            
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False
        
    finally:
        try:
            database.disconnect()
            print("Connection closed.")
        except:
            pass

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)