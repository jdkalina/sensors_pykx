import os
import shutil
from pathlib import Path
import pykx

def get_unique_tables(source_dir="/home/ubuntu/tipy/kxdb"):
    """Get list of all unique table names across all date directories."""
    tables = set()
    for date_dir in os.listdir(source_dir):
        date_path = Path(source_dir) / date_dir
        if date_path.is_dir():
            for table in os.listdir(date_path):
                if Path(date_path / table).is_dir():
                    tables.add(table)
    return sorted(list(tables))

def move_table(table_name, source_dir='/home/ubuntu/tipy/kxdb', target_dir='/home/ubuntu/tipy/database', ending='.1'):
    """Move one table from all date directories to target database."""
    for date_dir in os.listdir(source_dir):
        source_date_path = Path(source_dir) / date_dir
        if not source_date_path.is_dir():
            continue
        source_table = source_date_path / f"{table_name}{ending}"
        if not source_table.exists():
            continue
        target_date_path = Path(target_dir) / date_dir
        target_table = target_date_path / table_name
        
        target_date_path.mkdir(parents=True, exist_ok=True)
        if target_table.exists():
            shutil.rmtree(target_table)
        shutil.copytree(source_table, target_table)

def log_migration(table_name, database_dir='/home/ubuntu/tipy/database', log_file="migration.log"):
    """Log the table name and current database table count."""
    try:
        db = pykx.DB(path=database_dir)
        table_count = len(db.tables)
    except:
        table_count = "ERROR"
    
    with open(log_file, "a") as f:
        f.write(f"{table_name}: {table_count}\n")
    print(f"Logged {table_name}: {table_count}")


tables = get_unique_tables()

if __name__=='__main__':
    move_table(table_name='ees_run', source_dir='/home/ubuntu/pydb/hdb/data', target_dir='/home/ubuntu/tipy/database', ending='.1')
    move_table(table_name='ees_run', source_dir='/home/ubuntu/pydb/hdb/data', target_dir='/home/ubuntu/tipy/database', ending='.0')
    move_table(table_name='ees_sensor_data', source_dir='/home/ubuntu/pydb/hdb/data', target_dir='/home/ubuntu/tipy/database', ending='.1')
    move_table(table_name='ees_sensor_data', source_dir='/home/ubuntu/pydb/hdb/data', target_dir='/home/ubuntu/tipy/database', ending='.0')
    move_table(table_name='ees_run_context', source_dir='/home/ubuntu/pydb/hdb/data', target_dir='/home/ubuntu/tipy/database', ending='.1')
    move_table(table_name='ees_run_context', source_dir='/home/ubuntu/pydb/hdb/data', target_dir='/home/ubuntu/tipy/database', ending='.0')

