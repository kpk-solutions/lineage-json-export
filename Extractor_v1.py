# extract_metadata.py
"""
Script to extract source and target datasets, tables, files from scripts.
Works for .py, .sh, .ksh, .bteq, .sql files.
"""

import os
import re
import pandas as pd
import sqlparse
from datetime import datetime

# ---------------- Install commands ----------------
# pip install pandas sqlparse

# ---------------- Configuration ----------------
# Folder where script files are located
SCRIPT_DIR = "path_to_scripts_folder"  # Change this
# File extensions to process
FILE_EXTENSIONS = ['.py', '.sh', '.ksh', '.bteq', '.sql']
# Output CSV file
OUTPUT_FILE = "metadata_output.csv"

# ---------------- Patterns ----------------
SOURCE_TABLE_PATTERN = re.compile(r"\b(?:from|join|with)\s+([a-zA-Z0-9_\$]+)\.([a-zA-Z0-9_\$]+)", re.IGNORECASE)
TARGET_TABLE_PATTERN = re.compile(r"\b(?:insert\s+into|update|delete\s+from|truncate\s+table)\s+([a-zA-Z0-9_\$]+)\.([a-zA-Z0-9_\$]+)", re.IGNORECASE)
SOURCE_FILE_PATTERN = re.compile(r"\b[\w\/\.-]*\.(dat|csv)\b", re.IGNORECASE)
TARGET_FILE_PATTERN = re.compile(r">\s*([\w\/\.-]*\.(dat|csv))", re.IGNORECASE)
PARAM_PATTERN = re.compile(r"\$({)?(\w+)(})?")

# ---------------- Helper Functions ----------------

def extract_sql_statements(content):
    """Extract SQL statements from the script content."""
    statements = []
    try:
        parsed = sqlparse.parse(content)
        for stmt in parsed:
            statements.append(str(stmt))
    except Exception as e:
        pass
    return statements

def extract_tables_from_sql(sql):
    """Extract source and target tables from a SQL query."""
    sources = []
    targets = []
    try:
        sources += SOURCE_TABLE_PATTERN.findall(sql)
        targets += TARGET_TABLE_PATTERN.findall(sql)
    except Exception as e:
        pass
    return sources, targets

def extract_files(line, pattern):
    """Extract file names from a line using a given regex pattern."""
    return pattern.findall(line)

def extract_parameters(word):
    """Extract parameterized dataset/table names."""
    params = []
    matches = PARAM_PATTERN.findall(word)
    for match in matches:
        params.append(match[1])
    return params

def process_file(file_path):
    """Process a single file and extract metadata."""
    script_name = os.path.basename(file_path)
    source_tables = set()
    target_tables = set()
    source_files = set()
    target_files = set()

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            lines = content.splitlines()

            # Extract SQL statements
            sqls = extract_sql_statements(content)
            for sql in sqls:
                sources, targets = extract_tables_from_sql(sql)
                for db, tbl in sources:
                    name = f"{db}.{tbl}"
                    name = replace_parameters(name)
                    source_tables.add(name)
                for db, tbl in targets:
                    name = f"{db}.{tbl}"
                    name = replace_parameters(name)
                    target_tables.add(name)

            # Extract files and parameters from each line
            for line in lines:
                # Source files
                for file_match in extract_files(line, SOURCE_FILE_PATTERN):
                    file_name = file_match[0]
                    file_name = replace_parameters(file_name)
                    source_files.add(file_name)

                # Target files
                for file_match in extract_files(line, TARGET_FILE_PATTERN):
                    file_name = file_match[0]
                    file_name = replace_parameters(file_name)
                    target_files.add(file_name)

    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")

    return {
        "script_file_name": script_name,
        "source_tables": list(source_tables) if source_tables else ["NA"],
        "target_tables": list(target_tables) if target_tables else ["NA"],
        "source_files": list(source_files) if source_files else ["NA"],
        "target_files": list(target_files) if target_files else ["NA"],
        "insert_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def replace_parameters(text):
    """Replace parameter patterns with placeholders."""
    def replacer(match):
        return f"${{{match.group(2)}}}" if match.group(1) else f"${match.group(2)}"
    return PARAM_PATTERN.sub(replacer, text)

# ---------------- Main Execution ----------------

def main():
    all_data = []
    for root, dirs, files in os.walk(SCRIPT_DIR):
        for file in files:
            if any(file.endswith(ext) for ext in FILE_EXTENSIONS):
                full_path = os.path.join(root, file)
                print(f"Processing: {full_path}")
                data = process_file(full_path)
                all_data.append(data)

    # Create DataFrame
    df = pd.DataFrame(all_data)

    # Normalize lists to comma-separated strings
    df['source_tables'] = df['source_tables'].apply(lambda x: ','.join(sorted(set(x))))
    df['target_tables'] = df['target_tables'].apply(lambda x: ','.join(sorted(set(x))))
    df['source_files'] = df['source_files'].apply(lambda x: ','.join(sorted(set(x))))
    df['target_files'] = df['target_files'].apply(lambda x: ','.join(sorted(set(x))))

    # Drop duplicates and reset index
    df = df.drop_duplicates().reset_index(drop=True)

    # Save to CSV
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nMetadata extraction completed. Output saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
