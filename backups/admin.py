from django.contrib import admin
from django.utils.html import format_html

from backups.db_connectors import get_db_connector
from backups.media_manager import MediaBackupManager
from backups.models import Backup, Restore


@admin.register(Backup)
class BackupBackupAdmin(admin.ModelAdmin):
    list_display = ["type", "file_link", "created_at", "created_by"]
    list_filter = ["created_at"]
    readonly_fields = ["file", "created_at", "created_by"]

    def file_link(self, obj):
        if obj.file:
            return format_html("<a href='%s'>download backup</a>" % (obj.file.url,))
        else:
            return "No attachment"

    def save_model(self, request, obj, form, change):
        if obj.type == "database":
            connector = get_db_connector()
            #  Create the backup file
            backup_file_path = connector.create_backup()
            # Associate the backup with the saved model instance
            obj.file.name = backup_file_path
        elif obj.type == "media":
            backup_file_path = MediaBackupManager().backup()
            # Associate the backup with the saved model instance
            obj.file.name = backup_file_path

        obj.created_by = request.user
        obj.save()

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(Restore)
class RestoreBackupAdmin(admin.ModelAdmin):
    list_display = ["type", "restored_at", "restored_by"]
    list_filter = ["restored_at"]
    readonly_fields = ["restored_at", "restored_by"]

    def save_model(self, request, obj, form, change):
        # Associate the backup with the saved model instance
        obj.restored_by = request.user
        obj.save()
        if obj.type == "database":
            connector = get_db_connector()

            # Restore the backup file
            connector.restore_backup(obj.file)

        elif obj.type == "media":
            MediaBackupManager().restore(obj.file)

    def has_change_permission(self, request, obj=None):
        return False
