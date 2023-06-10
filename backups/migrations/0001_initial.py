# Generated by Django 4.2.2 on 2023-06-08 06:13

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="DatabaseBackup",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("database_name", models.CharField(max_length=255)),
                ("backup_format", models.CharField(max_length=255)),
                ("backup_file", models.FileField(upload_to="db_backups/")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "verbose_name": "Database Backup",
                "verbose_name_plural": "Database Backups",
            },
        ),
    ]