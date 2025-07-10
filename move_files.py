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


import os

count=0
def stepper():
    os.chdir('/home/ubuntu/tipy')
    global count
    print(count)
    tbl = tables[count]
    move_table(tbl)
    print(f"moved {tbl}")
    log_migration(tbl)
    print(pykx.DB(path='/home/ubuntu/tipy/database').tables)
    print('success\n...........\n\n')
    count+=1

stepper()


move_table(table_name='ees_run', source_dir='/home/ubuntu/pydb/hdb/data', target_dir='/home/ubuntu/tipy/database', ending='.1')
move_table(table_name='ees_run', source_dir='/home/ubuntu/pydb/hdb/data', target_dir='/home/ubuntu/tipy/database', ending='.0')
move_table(table_name='ees_sensor_data', source_dir='/home/ubuntu/pydb/hdb/data', target_dir='/home/ubuntu/tipy/database', ending='.1')
move_table(table_name='ees_sensor_data', source_dir='/home/ubuntu/pydb/hdb/data', target_dir='/home/ubuntu/tipy/database', ending='.0')
move_table(table_name='ees_run_context', source_dir='/home/ubuntu/pydb/hdb/data', target_dir='/home/ubuntu/tipy/database', ending='.1')
move_table(table_name='ees_run_context', source_dir='/home/ubuntu/pydb/hdb/data', target_dir='/home/ubuntu/tipy/database', ending='.0')



import pykx
import os
db = pykx.DB(path='database')
os.chdir('../')
db.tables




import os
import fnmatch

def tree_directories(path=".", target_name=None, prefix="", level=-1):
    """
    Print a tree of directories starting from the given path, optionally showing
    parent directories leading to a file or contents of a directory matching target_name.
    
    Args:
        path (str): Directory path to start from (default: current directory).
        target_name (str): Partial name of file or directory to search for (default: None, lists all directories).
        prefix (str): Prefix for formatting tree structure (used in recursion).
        level (int): Depth limit (-1 for no limit).
    """
    # Skip if level is 0 (depth limit reached)
    if level == 0:
        return []

    found_paths = []
    try:
        # Get list of all entries
        entries = os.listdir(path)
        entries.sort()  # Sort entries alphabetically
        dirs = [d for d in entries if os.path.isdir(os.path.join(path, d))]
        
        # Check if the current directory matches the target_name
        if target_name and fnmatch.fnmatch(os.path.basename(path).lower(), f"*{target_name.lower()}*"):
            # If current path is a directory match, print it and all its subdirectory contents
            print(f"{prefix}└── {os.path.basename(path)}")
            sub_dirs = [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]
            sub_dirs.sort()
            for i, sub_dir in enumerate(sub_dirs):
                is_last = i == len(sub_dirs) - 1
                print(f"{prefix}{'    ' if is_last else '│   '}{'└── ' if is_last else '├── '}{sub_dir}")
                # Recursively print subdirectories
                tree_directories(
                    path=os.path.join(path, sub_dir),
                    target_name=None,  # List all subdirectories
                    prefix=prefix + ("    " if is_last else "│   "),
                    level=level - 1 if level > 0 else -1
                )
            return [os.path.abspath(path)]  # Mark this directory as found
        
        # Check for files matching target_name
        if target_name:
            matching_files = [f for f in entries if os.path.isfile(os.path.join(path, f)) and 
                            fnmatch.fnmatch(f.lower(), f"*{target_name.lower()}*")]
            if matching_files:
                found_paths.append(os.path.abspath(path))  # Store parent directory of matching file
        
        # Process subdirectories
        for i, dir_name in enumerate(dirs):
            dir_path = os.path.join(path, dir_name)
            # Recursively search subdirectories
            sub_paths = tree_directories(
                path=dir_path,
                target_name=target_name,
                prefix=prefix + ("│   " if i < len(dirs) - 1 else "    "),
                level=level - 1 if level > 0 else -1
            )
            found_paths.extend(sub_paths)
        
        # If target_name is specified, only print directories leading to found paths
        if target_name:
            if any(os.path.abspath(path).startswith(os.path.abspath(p)) or 
                   os.path.abspath(p).startswith(os.path.abspath(path)) for p in found_paths):
                is_last = i == len(dirs) - 1 if 'i' in locals() else True
                print(f"{prefix}{'└── ' if is_last else '├── '}{os.path.basename(path)}")
        else:
            # If no target name, print all directories
            is_last = i == len(dirs) - 1 if 'i' in locals() else True
            print(f"{prefix}{'└── ' if is_last else '├── '}{os.path.basename(path)}")
        
        return found_paths
    
    except PermissionError:
        print(f"{prefix}└── [Permission Denied]")
        return []
    except OSError as e:
        print(f"{prefix}└── [Error: {e}]")
        return []

def treeByFile(target_name = "ees_run", directory = '/home/ubuntu/pydb/'):
    if not os.path.exists(directory):
        directory = "."  # Fallback to current directory if path doesn't exist
        print(f"Directory {directory} not found, using current directory.")
    print(f"Directory tree for {directory} searching for {target_name}:")
    found_paths = tree_directories(directory, target_name=target_name)
    if not found_paths:
        print(f"No files or directories matching {target_name} found in {directory}.")

t = tree_directories('/home/ubuntu/pydb/hdb/data')

treeByFile(target_name = "ees_tool_lookup", directory = '/home/ubuntu/pydb')


import pykx as kx
abc = kx.DB(path='test')
abc.tables

os.getcwd()
os.chdir('ubuntu/tipy/')


import pykx as kx
import os
db = pykx.DB(path='database')
os.chdir('../')






import pykx as kx

def schema_generate(df):
    import pykx
    schema = { v[0]:v[1] for k,v in data.dtypes.pd().iterrows()}
    transformed = {}
    for key, value in schema.items():
        try:
            attr_name = value.decode('utf-8').split('kx.')[1]
            if attr_name == 'CharVector': attr_name = 'List'
            transformed[key] = getattr(kx, attr_name)
        except (AttributeError, IndexError, UnicodeDecodeError) as e:
            transformed[key] = value
    return transformed

path2 = "/home/ubuntu/pydb/idb/data/2025.06.02/9/ees_sensor_lookup.ss"

db = kx.DB(path='database')
path = path2
data = kx.q(f'get `:{path2}')

schema_dict = schema_generate(data)
schema = kx.schema.builder(schema_dict)

if len(path.split('.')) > 1: tablename = path.split('/')[-1].split('.')[0]
else: tablename = path.split('/')[-1]

gzip = kx.Compress(algo=kx.CompressionAlgorithm.gzip, level=8)
db.create(schema.insert(data, inplace=False),table_name= tablename, format= 'splayed', compress=gzip)

db.tables



# EES TOOL LOOKUP

path1 = "/home/ubuntu/pydb/idb/data/2025.06.02/9/ees_tool_lookup.ss"
import pykx as kx

def schema_generate(df):
    import pykx
    schema = { v[0]:v[1] for k,v in data.dtypes.pd().iterrows()}
    transformed = {}
    for key, value in schema.items():
        try:
            attr_name = value.decode('utf-8').split('kx.')[1]
            if attr_name == 'CharVector': attr_name = 'List'
            transformed[key] = getattr(kx, attr_name)
        except (AttributeError, IndexError, UnicodeDecodeError) as e:
            transformed[key] = value
    return transformed

# db = kx.DB(path='database')
path = path1
data = kx.q(f'get `:{path1}')

schema_dict = schema_generate(data)
schema = kx.schema.builder(schema_dict)

if len(path.split('.')) > 1: tablename = path.split('/')[-1].split('.')[0]
else: tablename = path.split('/')[-1]

gzip = kx.Compress(algo=kx.CompressionAlgorithm.gzip, level=8)
db.create(schema.insert(data, inplace=False),table_name= tablename, format= 'splayed', compress=gzip)

db.tables
