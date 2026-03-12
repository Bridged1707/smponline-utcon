import os
from dotenv import load_dotenv

load_dotenv()

UTDB_API_URL = os.getenv("UTDB_API_URL", "http://10.1.0.91:9000")