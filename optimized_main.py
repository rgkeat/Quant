import os
import pandas as pd
from sqlglot import parse_one, expressions as exp
from biportal_connect.get_BIPORTAL import getBIPORTAL
import re
from typing import Dict, Set, Tuple, List, Optional


def read_sql_query(file_path: str) -> str:
    """Read SQL query from file with proper error handling."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            content = file.read().strip()
            if not content:
                raise ValueError(f"File is empty: {file_path}")
            return content
    except UnicodeDecodeError:
        raise ValueError(f"Cannot decode file as UTF-8: {file_path}")


def extract_table_set(parsed) -> Set[Tuple[Optional[str], str]]:
    """Extract table references from parsed SQL."""
    table_set = set()
    for table_expr in parsed.find_all(exp.Table):
        db_expr = table_expr.args.get("db")
        table = table_expr.name.lower()
        if db_expr is not None:
            db_name = db_expr.name.lower() if hasattr(db_expr, "name") else str(db_expr).lower()
            table_set.add((db_name, table))
        else:
            table_set.add((None, table))
    return table_set


def infer_db_schema(table: str, df_columns: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    """Infer database and schema from metadata DataFrame."""
    db, schema = None, None
    if not df_columns.empty:
        matches = df_columns[df_columns["table.name"] == table]
        if not matches.empty:
            if matches["database.name"].nunique() == 1:
                db = matches.iloc[0]["database.name"].lower()
            if matches["schema.name"].nunique() == 1:
                schema = matches.iloc[0]["schema.name"].lower()
    return db, schema


def is_temp_table(table_name: str) -> bool:
    """Check if a table name is a temporary table (#temp or ##global_temp)."""
    return table_name.startswith('#')


def clean_sql_for_parsing(sql_query: str) -> str:
    """Clean SQL query by removing comments."""
    # Remove single line comments
    sql_clean = re.sub(r'--.*?\n', '\n', sql_query)
    # Remove block comments
    sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)
    return sql_clean


def extract_columns_from_create_table(column_definitions: str) -> List[str]:
    """Extract column names from CREATE TABLE definition."""
    columns = []
    lines = [line.strip() for line in column_definitions.split(',')]
    
    for line in lines:
        if not line or line.upper().startswith(('PRIMARY', 'FOREIGN', 'UNIQUE', 'CHECK', 'INDEX')):
            continue
            
        col_parts = line.split()
        if col_parts:
            col_name = col_parts[0].strip('[]"\'').lower()
            if col_name and col_name not in ['constraint', 'primary', 'foreign', 'unique', 'check']:
                columns.append(col_name)
    
    return columns


def extract_columns_from_select_list(select_list: str) -> List[str]:
    """Extract column names from SELECT list."""
    columns = []
    if select_list.strip() == '*':
        return columns
        
    col_parts = [col.strip() for col in select_list.split(',')]
    for col in col_parts:
        if not col:
            continue
            
        # Handle aliases with AS
        if ' as ' in col.lower():
            col_name = col.lower().split(' as ')[-1].strip('[]"\'')
        else:
            # Take last part after dot for qualified names
            col_name = col.split('.')[-1].strip('[]"\'')
        
        # Clean up and validate column name
        col_name = re.sub(r'[^\w]+', '', col_name)
        if col_name:
            columns.append(col_name.lower())
    
    return columns


def extract_temp_table_info(sql_query: str) -> Dict[str, Dict]:
    """Enhanced temp table extraction with better column detection."""
    temp_tables = {}
    sql_clean = clean_sql_for_parsing(sql_query)
    
    # Pattern for CREATE TABLE #temp statements - fixed regex
    create_patterns = [
        r'CREATE\s+TABLE\s+(#\w+)\s*\(\s*(.*?)\s*\)',
        r'CREATE\s+TABLE\s+(#\w+)\s+\(\s*(.*?)\s*\)'
    ]
    
    for pattern in create_patterns:
        for match in re.finditer(pattern, sql_clean, re.IGNORECASE | re.DOTALL):
            temp_table_name = match.group(1).lower()
            column_definitions = match.group(2)
            
            columns = extract_columns_from_create_table(column_definitions)
            
            temp_tables[temp_table_name] = {
                'type': 'create_table',
                'columns': columns,
                'database': 'tempdb',
                'schema': 'dbo'
            }
    
    # Pattern for SELECT INTO #temp statements
    select_into_patterns = [
        r'SELECT\s+(.*?)\s+INTO\s+(#\w+)(?:\s+FROM|\s*$)',
        r'SELECT\s+(.*?)\s+INTO\s+(#\w+)\s+'
    ]
    
    for pattern in select_into_patterns:
        for match in re.finditer(pattern, sql_clean, re.IGNORECASE | re.DOTALL):
            temp_table_name = match.group(2).lower()
            if temp_table_name not in temp_tables:
                select_list = match.group(1).strip()
                columns = extract_columns_from_select_list(select_list)
                
                temp_tables[temp_table_name] = {
                    'type': 'select_into',
                    'columns': columns,
                    'database': 'tempdb',
                    'schema': 'dbo'
                }
    
    # Find all temp table references even if not defined
    temp_refs = re.findall(r'#\w+', sql_clean, re.IGNORECASE)
    for temp_ref in set(temp_refs):
        temp_name = temp_ref.lower()
        if temp_name not in temp_tables:
            temp_tables[temp_name] = {
                'type': 'referenced',
                'columns': [],
                'database': 'tempdb',
                'schema': 'dbo'
            }
    
    return temp_tables


def extract_all_table_references(sql_query: str) -> Set[str]:
    """Use regex to find all table references when sqlglot fails."""
    tables = set()
    # Fixed regex patterns - removed invalid syntax
    patterns = [
        r'FROM\s+(\[?[\w.#]+\]?)',
        r'JOIN\s+(\[?[\w.#]+\]?)',
        r'INTO\s+(\[?[\w.#]+\]?)',
        r'UPDATE\s+(\[?[\w.#]+\]?)',
        r'INSERT\s+(?:INTO\s+)?(\[?[\w.#]+\]?)'
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, sql_query, re.IGNORECASE)
        for match in matches:
            table_name = match.strip('[]\'').lower()
            if table_name and not table_name.startswith('select'):
                tables.add(table_name)
    
    return tables


def build_comprehensive_dataframe(sql_query: str, df_columns: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """Build a comprehensive DataFrame focusing on returning results even when parsing fails."""
    records = []
    
    # Extract temp tables
    temp_tables = extract_temp_table_info(sql_query)
    print(f"Found {len(temp_tables)} temporary tables")
    
    # Add temp table records only if they have actual columns
    for temp_name, temp_info in temp_tables.items():
        for col in temp_info['columns']:
            records.append({
                'Database': temp_info['database'],
                'Schema': temp_info['schema'],
                'Table': temp_name,
                'Column': col
            })
    
    # Use metadata from BI Portal as the primary source
    if not df_columns.empty:
        print(f"Using BI Portal metadata for {len(df_columns)} column records")
        for _, row in df_columns.iterrows():
            records.append({
                'Database': row.get('database.name', ''),
                'Schema': row.get('schema.name', ''),
                'Table': row.get('table.name', ''),
                'Column': row.get('column.name', '')
            })
    else:
        print("No BI Portal metadata available, attempting SQL parsing...")
        # Only fall back to parsing if no metadata is available
        try:
            parsed = parse_one(sql_query, dialect="tsql")  # Fixed dialect name
            # Extract column references from parsed SQL
            for col_expr in parsed.find_all(exp.Column):
                col_name = col_expr.name.lower()
                table_alias = col_expr.table.lower() if col_expr.table else None
                
                if table_alias and not is_temp_table(table_alias):
                    # Try to get schema info from parsing
                    db_name = ''
                    schema_name = ''
                    
                    # Look for the table definition to get schema info
                    for table_expr in parsed.find_all(exp.Table):
                        if table_expr.alias_or_name.lower() == table_alias:
                            if table_expr.args.get("catalog"):
                                db_name = table_expr.args["catalog"].name.lower()
                            if table_expr.args.get("db"):
                                schema_name = table_expr.args["db"].name.lower()
                            break
                    
                    records.append({
                        'Database': db_name,
                        'Schema': schema_name,
                        'Table': table_alias,
                        'Column': col_name
                    })
        except Exception as e:
            print(f"SQL parsing failed: {e}")
            # Last resort - basic regex extraction
            column_pattern = r'(\w+)\.(\w+)'
            matches = re.findall(column_pattern, sql_query, re.IGNORECASE)
            for table, column in matches:
                if not is_temp_table(table.lower()):
                    records.append({
                        'Database': '',
                        'Schema': '',
                        'Table': table.lower(),
                        'Column': column.lower()
                    })
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # Remove duplicates and sort
    if not df.empty:
        df = df.drop_duplicates().sort_values(['Database', 'Schema', 'Table', 'Column']).reset_index(drop=True)
    
    return df, temp_tables


def get_metadata_from_biportal(sql_query: str, metadata_query_path: str) -> pd.DataFrame:
    """Get metadata from BI Portal."""
    try:
        # Read the metadata query
        metadata_query = read_sql_query(metadata_query_path)
        print("Loaded metadata query from ssms_metadata.txt")
        
        # Extract table references
        table_set = set()
        all_table_refs = extract_all_table_references(sql_query)
        
        for table_ref in all_table_refs:
            if not is_temp_table(table_ref):
                parts = table_ref.split('.')
                if len(parts) >= 2:
                    table_set.add((parts[-2].lower(), parts[-1].lower()))
                else:
                    table_set.add((None, table_ref.lower()))
        
        if table_set:
            print(f"Attempting to get metadata for {len(table_set)} tables...")
            df_columns = getBIPORTAL(metadata_query, table_set)
            if df_columns is not None and not df_columns.empty:
                df_columns = df_columns.astype(str).apply(lambda x: x.str.strip().str.lower())
                print(f"Retrieved metadata for {len(df_columns)} columns")
                return df_columns
            else:
                print("No metadata retrieved from BI Portal")
        
        return pd.DataFrame(columns=["database.name", "schema.name", "table.name", "column.name"])
        
    except Exception as e:
        print(f"Warning: Could not retrieve metadata from BI Portal: {e}")
        return pd.DataFrame(columns=["database.name", "schema.name", "table.name", "column.name"])


def print_results(df_result: pd.DataFrame, temp_tables: Dict) -> None:
    """Print analysis results in a formatted way."""
    print(f"\n=== ANALYSIS RESULTS ===")
    print(f"Total records found: {len(df_result)}")
    print(f"Temporary tables found: {len(temp_tables)}")
    
    if not df_result.empty:
        print(f"\n=== COLUMN REFERENCES ===")
        print(df_result.to_string(index=False))
    else:
        print("No data found. Check if the SQL file contains valid T-SQL code.")
    
    # Print temp table details
    if temp_tables:
        print(f"\n=== TEMPORARY TABLES DETAILS ===")
        for temp_name, temp_info in temp_tables.items():
            print(f"Table: {temp_name}")
            print(f" Type: {temp_info['type']}")
            print(f" Columns ({len(temp_info['columns'])}): {temp_info['columns']}")
            print()


def main() -> pd.DataFrame:
    """Main function to process SQL query and extract metadata."""
    # Prompt the user to enter the SQL query file path
    query_path = input("Please enter the SQL query file path: ").strip()
    
    # Metadata query file path (for BI Portal connection)
    metadata_query_path = r'C:\JPMC\DEV\TMP\ds\DS_ENV\Desktop_ENV\data_dictionary\biportal\scripts\ssms_metadata.txt'
    
    try:
        # Read the SQL query
        sql_query = read_sql_query(query_path)
        print(f"SQL query loaded successfully. Length: {len(sql_query)} characters")
        
        # Get metadata from BI Portal
        df_columns = get_metadata_from_biportal(sql_query, metadata_query_path)
        
        # Build comprehensive DataFrame
        df_result, temp_tables = build_comprehensive_dataframe(sql_query, df_columns)
        
        # Print results
        print_results(df_result, temp_tables)
        
        return df_result

    except FileNotFoundError as e:
        print(f"Error: {e}")
        return pd.DataFrame()
    except ValueError as e:
        print(f"Error: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


if __name__ == "__main__":
    result_df = main()
