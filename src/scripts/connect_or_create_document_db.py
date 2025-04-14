#src/scripts/connect_or_create_document_db.py
import os

def connect_or_create_doc_store(path="../data/pdfs")->None:
    """
    Ensure that the document storage directory exists.
    """
    os.makedirs(path, exist_ok=True)
    print(f"Document store is ready at: {path}")