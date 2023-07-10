import os

from django.contrib.auth.models import User
from django.db import models
from django.dispatch import receiver


class Backup(models.Model):
    BACKUP_TYPE_CHOICES = [
        ("database", "Database Backup"),
        ("media", "Media Backup"),
    ]

    type = models.CharField(max_length=10, choices=BACKUP_TYPE_CHOICES)
    file = models.FileField(upload_to="backups/")
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(User, on_delete=models.PROTECT)

    def __str__(self):
        return f"{self.get_type_display()} Backup - {self.created_at}"

    def save(self, *args, **kwargs):
        max_backups = 2  # Maximum number of allowed backups
        backups = Backup.objects.filter(type=self.type).order_by("created_at")

        if backups.count() >= max_backups:
            oldest_backups = backups[: backups.count() - max_backups + 1]
            for backup in oldest_backups:
                backup.delete()

        super().save(*args, **kwargs)


class Restore(models.Model):
    RESTORE_TYPE_CHOICES = [
        ("database", "Database Restore"),
        ("media", "Media Restore"),
    ]

    type = models.CharField(max_length=10, choices=RESTORE_TYPE_CHOICES)
    restored_at = models.DateTimeField(auto_now_add=True)
    file = models.FileField(upload_to="backups/")
    restored_by = models.ForeignKey(User, on_delete=models.PROTECT)

    def __str__(self):
        return f"{self.get_type_display()} Restore - {self.restored_at}"


@receiver(models.signals.post_delete, sender=Backup)
def delete_file_with_backup(sender, instance, **kwargs):
    # Delete the associated file when a Backup object is deleted
    if instance.file:
        if os.path.isfile(instance.file.path):
            os.remove(instance.file.path)
