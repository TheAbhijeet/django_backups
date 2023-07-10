import os
import zipfile

from django.conf import settings
from django.utils.timezone import now


class MediaBackupManager:
    def __init__(self):
        self.media_root = settings.MEDIA_ROOT
        self.backup_root = settings.BACKUP_ROOT

    def get_backup_path(self):
        timestamp = now().strftime("%d-%m-%Y-%H::%M")
        backup_filename = f"media_backup_{timestamp}.zip"
        return self.backup_root / backup_filename

    def backup(self):
        # Create a backup directory if it doesn't exist
        os.makedirs(self.backup_root, exist_ok=True)

        # Generate a unique backup file name
        backup_file = self.get_backup_path()

        # Create a zip file to store the backup
        with zipfile.ZipFile(
            backup_file, "w", compression=zipfile.ZIP_DEFLATED
        ) as zipf:
            # Traverse the media root directory and add each file to the zip
            for root, dirs, files in os.walk(self.media_root):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, self.media_root)
                    zipf.write(file_path, arcname)

            # Calculate the relative backup path to the media root
            relative_backup_path = os.path.relpath(backup_file, self.media_root)

            return relative_backup_path

    def restore(self, zip_file):
        # Extract the contents of the backup zip file to a temporary directory
        with zipfile.ZipFile(zip_file, "r") as zipf:
            try:
                zipf.extractall(self.media_root)
            except zipfile.BadZipFile:
                raise Exception("Invalid or corrupted backup file.")
