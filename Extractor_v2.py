import re
from itertools import product
from datetime import datetime

def extract_standard_table_pairs(content, script_file_name, source_files=None, target_files=None):
    """
    Extract source-target table pairs from SQL/script content for only standard project.dataset.table format.

    Parameters:
        content (str): SQL/script content
        script_file_name (str): Name of the script file
        source_files (list): List of source files (optional)
        target_files (list): List of target files (optional)

    Returns:
        list of dict: Each dict contains:
            script_file_name, source_file, target_file,
            source_database, source_table,
            target_database, target_table, insert_time
    """
    source_files = source_files or [script_file_name]
    target_files = target_files or [script_file_name]

    # Clean comments
    content = re.sub(r'--.*', '', content)
    content = re.sub(r'#.*', '', content)
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)

    # Pattern to match standard project.dataset.table or ${project}.${dataset}.${table}
    table_pattern = r"(?:[a-zA-Z0-9_\$]+\.[a-zA-Z0-9_\$]+\.[a-zA-Z0-9_\$]+|\$\{[a-zA-Z0-9_]+\}\.\$\{[a-zA-Z0-9_]+\}\.\$\{[a-zA-Z0-9_]+\})"

    # Source tables pattern
    source_pattern = re.compile(
        r"\b(?:from|join|with)\s+(" + table_pattern + r")",
        re.IGNORECASE
    )

    # Target tables pattern
    target_pattern = re.compile(
        r"\b(?:insert\s+into|update|delete\s+from|truncate\s+table)\s+(" + table_pattern + r")",
        re.IGNORECASE
    )

    # Find matches
    source_tables = list(set(source_pattern.findall(content)))
    target_tables = list(set(target_pattern.findall(content)))

    all_records = []

    # Generate all combinations: source_table × target_table × source_file × target_file
    for src_table, tgt_table, src_file, tgt_file in product(source_tables, target_tables, source_files, target_files):
        try:
            src_db, src_tbl = src_table.split('.')[-2:]
            tgt_db, tgt_tbl = tgt_table.split('.')[-2:]
        except ValueError:
            # Skip if it does not match standard format
            continue

        all_records.append({
            "script_file_name": script_file_name,
            "source_file": src_file,
            "target_file": tgt_file,
            "source_database": src_db,
            "source_table": src_tbl,
            "target_database": tgt_db,
            "target_table": tgt_tbl,
            "insert_time": datetime.now()
        })

    return all_records
