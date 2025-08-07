#!/usr/bin/env python3

import json
import os
from datetime import datetime

def add_pm_tables():
    """Add PM-related tables to the config for data export."""
    
    # Read current config
    config_path = 'config.json'
    with open(config_path, 'r') as f:
        config_data = json.load(f)
    
    db_name = 'pm-v3-api-service'
    
    # Ensure the database config exists
    if db_name not in config_data['databases']:
        print(f"❌ Database {db_name} not found in config")
        return False
    
    # Initialize tables array if it doesn't exist
    if 'tables' not in config_data['databases'][db_name]:
        config_data['databases'][db_name]['tables'] = []
    
    # Define the output directory
    output_dir = "./output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Tables to add for PM data export
    tables_to_add = [
        {
            "name": "pmapi_pum_optins",
            "columns": [],  # Empty means all columns
            "where": "datetime >= '2025-01-01 00:00:00'",  # Filter for 2025 data
            "primary_key": "id",
            "incremental": True,
            "incremental_column": "id",
            "last_id": 0,
            "output": f"{output_dir}/pm-v3/pmapi_pum_optins_2025.csv"
        },
        {
            "name": "pmapi_pum_reviews",
            "columns": [],  # Empty means all columns
            "where": "time >= '2025-01-01 00:00:00'",  # Filter for 2025 data
            "primary_key": "id", 
            "incremental": True,
            "incremental_column": "id",
            "last_id": 0,
            "output": f"{output_dir}/pm-v3/pmapi_pum_reviews_2025.csv"
        }
    ]
    
    # Check if tables already exist in config
    existing_table_names = []
    if config_data['databases'][db_name]['tables']:
        existing_table_names = [table['name'] for table in config_data['databases'][db_name]['tables']]
    
    # Add new tables
    tables_added = 0
    for table_config in tables_to_add:
        table_name = table_config['name']
        
        if table_name in existing_table_names:
            print(f"⚠️  Table {table_name} already exists in config, skipping...")
            continue
        
        config_data['databases'][db_name]['tables'].append(table_config)
        tables_added += 1
        print(f"✅ Added table {table_name} to config")
    
    # Save updated config
    if tables_added > 0:
        # Create backup
        backup_path = f"{config_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        with open(backup_path, 'w') as f:
            json.dump(config_data, f)
        print(f"📋 Created config backup: {backup_path}")
        
        # Save updated config
        with open(config_path, 'w') as f:
            json.dump(config_data, f, indent=4)
        print(f"💾 Updated {config_path} with {tables_added} new tables")
    else:
        print("ℹ️  No new tables to add")
    
    # Display final config summary
    total_tables = len(config_data['databases'][db_name]['tables'])
    print(f"\n📊 Configuration Summary:")
    print(f"   Database: {db_name}")
    print(f"   Total tables: {total_tables}")
    
    if config_data['databases'][db_name]['tables']:
        print("   Configured tables:")
        for table in config_data['databases'][db_name]['tables']:
            print(f"     - {table['name']} → {table['output']}")
            if table.get('where'):
                print(f"       Filter: {table['where']}")
    
    return True

if __name__ == "__main__":
    success = add_pm_tables()
    if success:
        print("\n🎉 Table configuration completed successfully!")
    else:
        print("\n❌ Table configuration failed!")