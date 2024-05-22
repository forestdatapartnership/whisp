import shutil
from pathlib import Path

__all__ = [
    "BASE_DIR",
    "RESULTS_DIR",
    "BACKUP_CSVS_DIR",
]

BASE_DIR = Path("~").expanduser()

RESULTS_DIR = BASE_DIR / "module_results/whisp"

BACKUP_CSVS_DIR = RESULTS_DIR / "backup_csvs"
"Path: location for storing backup csvs - typically created during geo-id registration process"

RESULTS_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_CSVS_DIR.mkdir(parents=True, exist_ok=True)

### if needed
# # copy param.LC_CLASSES to CLASS_DIR and make it read only if it doesn't exist
# LOCAL_README_BACKUP_CSV = BACKUP_CSVS_DIR.joinpath("readme_backup_csv.txt")
# if not LOCAL_README_BACKUP_CSV.exists():
#     shutil.copy(param.README_BACKUP_CSV, LOCAL_README_BACKUP_CSV)
#     LOCAL_README_BACKUP_CSV.chmod(0o444)
