import subprocess
from datetime import datetime

from database.config import config
from database.utils import get_db

DATABASE_URL = config(return_url=True)

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
backup_path = f"/tmp/backup_{timestamp}.dump"

try:
    conn = get_db()
    conn.close()

    subprocess.run(
        [
            "/usr/lib/postgresql/17/bin/pg_dump",
            "-F", "c",
            DATABASE_URL,
            "-f", backup_path,
        ],
        check=True,
    )

    subprocess.run(
        [
            "rclone",
            "move",
            backup_path,
            "gdrive:pg-backups",
        ],
        check=True,
    )

    print("Backup created and uploaded.")
except Exception as e:
    print(f'[backup failure]: {e}')
    raise e
