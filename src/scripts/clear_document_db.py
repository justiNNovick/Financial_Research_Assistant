#src/scripts/clear_document_db.py
def clear_doc_store(path="../data/pdfs", should_clear: bool = False)-> None:
    """
    Clears all files from the document store directory if should_clear is True.
    """
    if not should_clear:
        print("Document store clear skipped (should_clear is False).")
        return

    if os.path.isdir(path):
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"Error deleting file {file_path}: {e}")
        print(f"Cleared all files in document store: {path}")
    else:
        print(f"Document store directory does not exist: {path}")
