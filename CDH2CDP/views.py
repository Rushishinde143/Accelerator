from django.core.files.storage import FileSystemStorage
from django.shortcuts import render
from .forms import UploadForm, FileFieldForm

from django.views.generic.edit import FormView
from .forms import FileFieldForm
from .code import main

class FileFieldFormView(FormView):
    form_class = FileFieldForm
    template_name = "upload.html"  # Replace with your template.
    success_url = "..."  # Replace with your URL or reverse().

    def post(self, request, *args, **kwargs):
        form_class = self.get_form_class()
        form = self.get_form(form_class)
        if form.is_valid():
            return self.form_valid(form)
        else:
            return self.form_invalid(form)
    def form_valid(self, form):
        files = form.cleaned_data["file_field"]
        for f in files:
            ...  # Do something with each file.
        return super().form_valid()

# Create your views here.
def home_view(request):

    return render(request,'home.html')

def page2_view(request):
    if request.method == 'GET':
        form=UploadForm()
        return render(request, "page2.html",{ 'form': form})
def upload_view(request):
    if request.method == 'POST': 
        form = UploadForm(request.POST, request.FILES)
        
        files = request.FILES.getlist('file')

        print(files)

        for file in files:
            s = FileSystemStorage(location = "C:/Users/SCHILLAL/PycharmProjects/Accelerator/Media/user_input/")
            filename = s.save(file.name, file)
            uploaded_file_path = s.path(filename)
            print('absolute file path', uploaded_file_path)
            # print(type(uploaded_file_path))
        #list of files.
          
        return render(request, "page2.html", {'status': True})


def folder_selection(request):
    if request.method == 'POST':
        selected_folder = request.POST.get('folder')
        # Do something with the selected folder

    return render(request, 'folder_selection.html')

def submit_view(request):
    if request.method == 'POST': 
        data = request.POST['path']

        main.main()
        
        ##write your main logic
    return render(request,'home.html')