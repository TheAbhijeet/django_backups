from django.conf import settings
from django.contrib import admin
from django.utils.html import format_html

from backups.db_connectors import SqliteConnector, PostgresConnector, get_db_connector
from backups.models import Backup, Restore


@admin.register(Backup)
class BackupBackupAdmin(admin.ModelAdmin):
    list_display = ['type', 'file_link', 'created_at', 'created_by']
    list_filter = ['created_at']
    readonly_fields = ['file', 'created_at', 'created_by']

    def file_link(self, obj):
        if obj.file:
            return format_html("<a href='%s'>download backup</a>" % (obj.file.url,))
        else:
            return "No attachment"

    def save_model(self, request, obj, form, change):
        if obj.type == 'database':
            connector = get_db_connector()
            #  Create the backup file
            backup_file_path = connector.create_backup()
            # Associate the backup with the saved model instance
            obj.file.name = backup_file_path

        obj.created_by = request.user
        obj.save()

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(Restore)
class RestoreBackupAdmin(admin.ModelAdmin):
    list_display = ['type', 'restored_at', 'restored_by']
    list_filter = ['restored_at']
    readonly_fields = ['restored_at', 'restored_by']

    def save_model(self, request, obj, form, change):
        database_engine = settings.DATABASES['default']['ENGINE']

        # Create a backup based on the database type
        if 'sqlite3' in database_engine:
            connector = SqliteConnector()
        elif 'postgresql' in database_engine:
            connector = PostgresConnector()
        else:
            print(f"Database type '{database_engine}' is not supported for backup.")
            return

        # Associate the backup with the saved model instance
        obj.restored_by = request.user
        obj.save()

        # Restore the backup file
        connector.restore_backup(obj.file)

    def has_change_permission(self, request, obj=None):
        return False
