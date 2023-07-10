from django import forms
from django.db import connection

from backups.models import Backup, Restore


class BackupForm(forms.ModelForm):
    class Meta:
        model = Backup
        fields = ("type",)


class RestoreForm(forms.ModelForm):
    class Meta:
        model = Restore
        fields = ("type",)
