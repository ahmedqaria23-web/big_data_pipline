import sys
from pathlib import Path

# Add project root directory to sys.path so pytest can locate 'src' and 'config'
root_dir = Path(__file__).parent.resolve()
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))
