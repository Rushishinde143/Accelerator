from django import forms
from django.forms import ClearableFileInput
from django import forms

class ZipFileUploadForm(forms.Form):
    zip_file = forms.FileField()


class MultipleFileInput(forms.ClearableFileInput):
    allow_multiple_selected = True


class MultipleFileField(forms.FileField):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("widget", MultipleFileInput())
        super().__init__(*args, **kwargs)

    def clean(self, data, initial=None):
        single_file_clean = super().clean
        if isinstance(data, (list, tuple)):
            result = [single_file_clean(d, initial) for d in data]
        else:
            result = single_file_clean(data, initial)
        return result


class FileFieldForm(forms.Form):
    file_filed = MultipleFileField()


class SubmitForm(forms.Form):
    pass


class UploadForm1(forms.Form):
    file = forms.FileField(label="Select Source Files", widget=forms.FileInput(attrs={'multiple': True}), required=True)


class UploadForm2(forms.Form):
    file = forms.FileField(label="Select Source Files", widget=forms.FileInput(attrs={'multiple': True}), required=True)


class SubmitForm(forms.Form):
    comments = forms.CharField(widget=forms.Textarea, required=True)
