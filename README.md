# Sensors PyKX

A Python project that migrates KX Sensors HDB (Historical Database) files into a separate PyKX database and provides analytics capabilities for sensor data analysis.

## Overview

This project is designed to:
- Move HDB files from KX Sensors into a dedicated PyKX database
- Provide query functions for sensor data analysis
- Enable basic analytics on sensor runs, contexts, and time-series data

## Project Structure

```
sensors_pykx/
├── kxqueries.py         # Main query functions and analytics
├── move_files.py        # Database migration utilities
├── notebook.ipynb      # Jupyter notebook for interactive analysis
├── .gitignore          # Git ignore file
├── venv/               # Python virtual environment
└── database/           # Target PyKX database directory (excluded from git)
```

## Key Components

### Database Migration (`move_files.py`)
- **`get_unique_tables()`**: Discovers all unique table names across date directories
- **`move_table()`**: Migrates individual tables from source to target database
- **`log_migration()`**: Logs migration progress and database table counts
- **`schema_generate()`**: Generates PyKX schemas from existing data structures

### Query Interface (`kxqueries.py`)
- **`get_runs()`**: Retrieves run information for specified tools and time ranges
- **`get_contexts()`**: Fetches context data for runs and tools
- **`get_sensor_data()`**: Extracts sensor time-series data (float, integer, string types)
- **`kx_dt()`**: Utility function for KX date conversion
- **`samples` class**: Provides sample data for testing queries

## Database Schema

The project works with several key tables:
- **`ees_run`**: Tool run information with timestamps
- **`ees_run_context`**: Context variables for runs
- **`ees_sensor_data`**: Time-series sensor measurements
- **`ees_sensor_lookup`**: Sensor metadata and configurations
- **`ees_tool_lookup`**: Tool definitions and process types

## Installation

### Prerequisites
- Python 3.12+
- PyKX library
- KX Sensors data access

### Setup
1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install pykx pandas numpy
   ```

## Usage

### Database Migration
```python
from move_files import move_table, get_unique_tables

# Get all available tables
tables = get_unique_tables(source_dir="/path/to/kxdb")

# Move specific table
move_table(
    table_name='ees_sensor_data',
    source_dir='/path/to/source/kxdb',
    target_dir='/path/to/target/database'
)
```

### Analytics Queries
```python
import kxqueries as kxq

# Initialize sample data
data = kxq.samples()

# Get run information
runs = kxq.get_runs(
    tool_ids=data.tool_id,
    start_time=data.start_time,
    end_time=data.end_time
)

# Get sensor data
sensor_data = kxq.get_sensor_data(
    tool_id=data.tool_id,
    run_id=data.run_id,
    start_time=data.start_time,
    end_time=data.end_time
)

# Get context information
contexts = kxq.get_contexts(
    tool_ids=data.tool_id,
    context_ids=data.context_ids,
    start_time=data.start_time,
    end_time=data.end_time
)
```

## Data Types and Formats

### Sensor Data Types
- **Float/Double**: Continuous sensor measurements
- **Integer**: Discrete sensor values
- **String**: Categorical or text-based sensor data

### Time Series Structure
- **`time_stamps`**: Timestamp arrays for sensor readings
- **`data_float`**: Float sensor values
- **`data_long`**: Integer sensor values  
- **`data_str`**: String sensor values (null-terminated, processed)

## Configuration

### Database Paths
Default paths can be configured in the respective modules:
- Source: `/home/ubuntu/tipy/kxdb`
- Target: `/home/ubuntu/tipy/database`

### Compression
Data is stored with gzip compression (level 8) for efficient storage.

## Interactive Analysis

Use the included Jupyter notebook (`notebook.ipynb`) for interactive data exploration and visualization.

## Development

### Adding New Queries
1. Add function to `kxqueries.py`
2. Follow the existing pattern of using PyKX column selections
3. Use appropriate joins and key indexing for performance

### Database Schema Updates
1. Update `schema_generate()` in `move_files.py` for new data types
2. Add new table migration logic as needed

## Troubleshooting

### Common Issues
- **Permission Errors**: Ensure proper file system permissions for database directories
- **PyKX License**: Check for valid PyKX license if encountering license warnings
- **Memory Issues**: Monitor memory usage during large table migrations

### Logging
Migration progress is logged to `migration.log` with table counts and status information.

## License

This project is designed for internal use with KX Sensors data. Ensure compliance with KX licensing requirements.

## Support

For issues or questions:
1. Check the migration logs for database-related issues
2. Verify PyKX installation and licensing
3. Ensure proper access to source KX Sensors data
