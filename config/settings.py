import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from config/.env
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()

BASE_DIR = Path(__file__).parent.parent.resolve()

# Set JAVA_HOME and HADOOP_HOME with robust ASCII paths
import ctypes, shutil
if os.path.exists(r"C:\Program Files\Java\jdk-17.0.10+7") and not os.path.exists(r"C:\jdk17"):
    try:
        import subprocess
        subprocess.run(["powershell", "-Command", "New-Item -ItemType Junction -Path 'C:\\jdk17' -Target 'C:\\Program Files\\Java\\jdk-17.0.10+7' -Force"], capture_output=True)
    except Exception:
        pass

DEFAULT_JDK = r"C:\jdk17" if os.path.exists(r"C:\jdk17") else r"C:\Program Files\Java\jdk-17.0.10+7"
if os.path.exists(os.path.join(DEFAULT_JDK, "bin", "java.exe")):
    os.environ["JAVA_HOME"] = DEFAULT_JDK
    os.environ["PATH"] = os.path.join(DEFAULT_JDK, "bin") + os.pathsep + os.environ.get("PATH", "")

user_hadoop = Path.home() / ".hadoop"
user_hadoop_bin = user_hadoop / "bin"
user_hadoop_bin.mkdir(parents=True, exist_ok=True)
local_winutils = BASE_DIR / ".hadoop" / "bin" / "winutils.exe"
target_winutils = user_hadoop_bin / "winutils.exe"
if local_winutils.exists() and not target_winutils.exists():
    try:
        shutil.copy2(local_winutils, target_winutils)
    except Exception:
        pass

os.environ["HADOOP_HOME"] = str(user_hadoop)
os.environ["hadoop.home.dir"] = str(user_hadoop)

try:
    import pyspark
    if hasattr(pyspark, "__file__") and pyspark.__file__:
        pyspark_dir = os.path.dirname(pyspark.__file__)
        if any(ord(c) > 127 for c in pyspark_dir):
            if not os.path.exists(r"C:\pyspark_home"):
                try:
                    import subprocess
                    subprocess.run(["powershell", "-Command", f"New-Item -ItemType Junction -Path 'C:\\pyspark_home' -Target '{pyspark_dir}' -Force"], capture_output=True)
                except Exception:
                    pass
            if os.path.exists(r"C:\pyspark_home"):
                pyspark_dir = r"C:\pyspark_home"
        os.environ["SPARK_HOME"] = pyspark_dir
except Exception:
    pass

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")

MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "ecommerce_store")

SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", "200.0"))

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "3000"))

CLASSIFICATION_WORKERS = int(os.getenv("CLASSIFICATION_WORKERS", "4"))
CLASSIFICATION_CHUNK_SIZE = int(os.getenv("CLASSIFICATION_CHUNK_SIZE", "4000"))
MONGO_WRITE_BATCH_SIZE = int(os.getenv("MONGO_WRITE_BATCH_SIZE", "4000"))

DATA_DIR = BASE_DIR / os.getenv("DATA_DIRECTORY", "data")
REPORT_DIR = BASE_DIR / os.getenv("REPORT_DIRECTORY", "reports")

SCHEMAS_DIR = BASE_DIR / "schemas"
TESTS_DIR = BASE_DIR / "tests"
DOCS_DIR = BASE_DIR / "docs"

# Collections

COLLECTION_RAW = "orders_raw"
COLLECTION_VALIDATED = "orders_validated"
COLLECTION_QUARANTINE = "quarantine_orders"
COLLECTION_META_STATE = "meta_state"
COLLECTION_PROCESSED_EVENTS = "processed_events"

LOG_FILE = REPORT_DIR / "pipeline.log"

# Ensure required directories exist
for d in [DATA_DIR, REPORT_DIR, SCHEMAS_DIR]:
    d.mkdir(parents=True, exist_ok=True)


def setup_logging():
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

