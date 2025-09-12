"""
Smart Table Extractor for Skipped SQL Files
This script helps extract source/target tables from skipped SQL files based on examples from already processed files.
It is designed to work locally without sending data externally, suitable for privacy-sensitive environments.

Usage:
1. Install dependencies before running:
   pip install spacy
   python -m spacy download en_core_web_sm

2. Import and call process_skipped_file() wherever needed.
"""

import spacy
from spacy.matcher import PhraseMatcher

def create_matcher(nlp, table_names):
    """
    Create a PhraseMatcher using example table names.
    """
    matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
    patterns = [nlp.make_doc(name) for name in table_names]
    matcher.add("TABLE_NAME", patterns)
    return matcher

def extract_tables_from_text(content, matcher, nlp):
    """
    Extract tables from the text content using spaCy matcher.
    Returns a list of unique table names found in the text.
    """
    doc = nlp(content)
    matches = matcher(doc)
    found_tables = set()
    for match_id, start, end in matches:
        span = doc[start:end]
        found_tables.add(span.text)
    return list(found_tables)

def process_skipped_file(content, known_tables):
    """
    Process a skipped file's content using known table names.
    Returns a dictionary with tables found and confidence score.
    """
    nlp = spacy.load("en_core_web_sm")
    matcher = create_matcher(nlp, known_tables)
    found_tables = extract_tables_from_text(content, matcher, nlp)

    if not found_tables:
        return {"tables": [], "confidence": 0.0}
    else:
        confidence = min(len(found_tables) / len(known_tables), 1.0)
        return {"tables": found_tables, "confidence": confidence}

# Example usage when running this file directly
if __name__ == "__main__":
    # Example known tables from already processed files
    known_tables = [
        "project1.dataset1.table1",
        "project1.dataset2.table2",
        "project2.dataset3.table3"
    ]

    # Example skipped file content
    content = """
    INSERT INTO project1.dataset1.table1
    SELECT * FROM project1.dataset2.table2
    JOIN project2.dataset3.table3 ON ...
    WHERE project1.dataset1.table1.id > 100
    """

    result = process_skipped_file(content, known_tables)

    print("Extracted tables:", result["tables"])
    print("Confidence score:", result["confidence"])

from smart_table_extractor import process_skipped_file

known_tables = ["project1.dataset1.table1", "project1.dataset2.table2", "project2.dataset3.table3"]
content = open("path_to_skipped_file.sql").read()

result = process_skipped_file(content, known_tables)

print("Tables found:", result["tables"])
print("Confidence:", result["confidence"])
