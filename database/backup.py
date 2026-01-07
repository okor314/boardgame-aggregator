import subprocess
from config import config
from datetime import datetime

DATABASE_URL = config(return_url=True)

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
backup_path = f"/tmp/backup_{timestamp}.dump"

subprocess.run(
    [
        "pg_dump",
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