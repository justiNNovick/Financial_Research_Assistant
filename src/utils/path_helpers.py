#src/utils/path_helpers.py

import os

def project_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
