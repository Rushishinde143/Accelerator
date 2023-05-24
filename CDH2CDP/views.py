import time

from django.core.files.storage import FileSystemStorage
from django.shortcuts import render
from .forms import UploadForm , FileFieldForm

from django.views.generic.edit import FormView
from .forms import FileFieldForm
from .code import main
from django.views.generic import TemplateView


# files = ""

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

class StoreFiles():
    def __init__(self, files):
        self.files = files

    def setFiles(self, files):
        self.files = files

    def getFiles(self):
        return self.files


storefiles = StoreFiles(None)

# Create your views here.
def home_view(request):

    return render(request,'home.html')

def page2_view(request):
    if request.method == 'GET':
        form=UploadForm()
        return render(request, "page2.html",{ 'form': form})

class AboutView(TemplateView):
    files = ""

    def upload_view(self, request):
        # global files
        if request.method == 'POST':
            form = UploadForm(request.POST, request.FILES)
            self.files = request.FILES.getlist('file')
            storefiles.setFiles(files)
            #list of files.

            return render(request, "page2.html", {'status': True})
    
    def submit_view(self, request):
        if request.method == 'POST': 
            data = request.POST['path']

            # main.main()

            batch_size = 3
            # print(files)
            f1 = self.files
            for i in range(0, len(f1), batch_size):
                for j in range(batch_size):
                    if i + j < len(f1):
                        f = f1[i + j]
                        print('single file', f)
                        s = FileSystemStorage(location="C:/Users/SCHILLAL/PycharmProjects/Accelerator/Media/user_input/")
                        filename = s.save(f.name, f)
                    # uploaded_file_path = s.path(filename)
                    # print('absolute file path', uploaded_file_path)
                    # print(type(uploaded_file_path))
                time.sleep(1)
                main.main()

        
        ##write your main logic
        return render(request,'home.html')

def folder_selection(request):
    if request.method == 'POST':
        selected_folder = request.POST.get('folder')
        # Do something with the selected folder

    return render(request, 'folder_selection.html')

