# CSV Database Processor

A professional tool for batch processing database operations (UPDATE/DELETE) using CSV files.

## Features

- 🔄 Support for batch DELETE and UPDATE operations
- 📊 CSV-driven operation commands
- 🔒 Data backup before operations
- 👀 Preview of affected data before execution
- ✅ User confirmation for each operation
- 🎨 Rich CLI interface with colored output
- 🏢 Multi-environment support (prod/test/dev)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/iyuangang/csv-processor.git
cd csv-processor
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Create a `config.json` file with your database configurations:

```json
{
    "databases": {
        "prod": {
            "username": "prod_user",
            "password": "prod_password",
            "host": "prod.example.com",
            "port": "1521",
            "service_name": "PROD"
        },
        "test": {
            "username": "test_user",
            "password": "test_password",
            "host": "test.example.com",
            "port": "1521",
            "service_name": "TEST"
        }
    }
}
```

## CSV Format

Your CSV file should follow this format:

### For DELETE operations:
```csv
column1,column2,command
value1,value2,delete
```

### For UPDATE operations:
```csv
column1,column2,command,new_column1,new_column2
value1,value2,update,new_value1,new_value2
```

- Columns before `command` are used as conditions
- Columns after `command` (prefixed with `new_`) specify the values to update

### Example CSV:
```csv
id,name,status,department,command,new_salary,new_status
1,,,IT,delete,,
5,,,Sales,update,8000,inactive
```

## Usage

1. Run with specific environment and CSV file:
```bash
python main.py --env test --csv-file your_commands.csv
```

2. Optional configuration file path:
```bash
python main.py --env test --csv-file your_commands.csv --config-file path/to/config.json
```

## Testing

1. Generate test data:
```bash
python tests/generate_test_data.py
```

2. Initialize test database:
```bash
python tests/init_database.py
```

3. Run test commands:
```bash
python main.py --env test --csv-file tests/test_commands.csv
```

## Features in Detail

### Batch DELETE Operations
- Combines multiple delete conditions using IN clauses
- Shows affected rows before execution
- Displays unmatched conditions

### UPDATE Operations
- Supports multiple column updates
- Handles date formats automatically
- Shows before/after data preview

### Safety Features
- Automatic data backup to `test_table_bak`
- Transaction management
- User confirmation required
- Clear error messages

## Project Structure

```
csv_processor/
├── src/
│   ├── __init__.py
│   ├── config.py        # Configuration management
│   ├── database.py      # Database operations
│   ├── processor.py     # Data processing
│   └── models.py        # Data models
├── tests/
│   ├── generate_test_data.py
│   ├── init_database.py
│   └── create_test_tables.sql
├── requirements.txt
└── main.py
```

## Dependencies

- pandas>=1.3.0
- cx-Oracle>=8.3.0
- click>=8.0.0
- rich>=10.0.0

## Error Handling

The tool provides clear error messages for common issues:
- Database connection errors
- Invalid CSV format
- SQL execution errors
- Data validation errors

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Author

@iyuangang

## Acknowledgments

- Oracle Database
- Python community
- All contributors
