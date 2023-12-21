import datetime
import glob

import pandas as pd
import asyncio
from django.views.generic.edit import FormView
import os, fnmatch
from django.core.files.storage import FileSystemStorage
from . import forms
from .codes import main
from .codes.spark import spark_main
from .forms import UploadForm1, UploadForm2
from .codes.zip.__init__ import upload
from pathlib import Path
from flask import render_template
from reportlab.platypus import Table, TableStyle
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.units import inch
from paths.config_reader import read_input, write_output,zip_files,unzip_files



BASE_DIR = Path(__file__).resolve().parent.parent
Files_DIR = os.path.join(BASE_DIR, 'Files')
DATA_DIR = os.path.join(BASE_DIR,'data')
MEDIA_ROOT = os.path.join(DATA_DIR, 'Media')
Zip_DIR = os.path.join(MEDIA_ROOT, 'zip_files')
Unzip_DIR = os.path.join(MEDIA_ROOT, 'unzipped_files')
Report_DIR = os.path.join(Unzip_DIR, 'reports')
Input_User_DIR = os.path.join(MEDIA_ROOT, 'user_input')
Destination_DIR = os.path.join(MEDIA_ROOT, 'destination_folder')


global uploaded_file_path

files = ""

cancel_clicked = False
submit_clicked = False
batch_size = ''
s = ''


class FileFieldFormView(FormView):
    form_class = forms.FileFieldForm
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
    global cancel_clicked
    cancel_clicked = False
    return render(request, 'home.html')


def hive_view(request):
    if request.method == 'GET':
        form1 = forms.UploadForm1()
        return render(request, "hive.html", {'form': form1})


def spark_view(request):
    if request.method == 'GET':
        form2 = forms.UploadForm2()
        print("in html zip view...")
        return render(request, "spark.html", {'form': form2})


def zip_view(request):
    print("in zip view html...")
    if request.method == 'GET':
        print("in zip view html...")
        form1 = forms.UploadForm3()
        return render(request, "zip.html", {'form': form1})

def upload_zip(request):
    if request.method == 'POST':
        print("in zip post line 34")
        form = ZipFileUploadForm(request.POST, request.FILES)
        if form.is_valid():
            # Handle the uploaded zip file here
            zip_file = form.cleaned_data['zip_file']
            # Process the zip file or its contents
            # Example: Extract files from the zip file
            # You can use the 'zipfile' module for this purpose
    else:
        form = ZipFileUploadForm()
    return render(request, 'zip.html', {'form': form})

def upload_zip(request):
    if request.method == 'POST':
        form = ZipFileUploadForm(request.POST, request.FILES)
        print("in post zip upload 146")
        if form.is_valid():
            uploaded_file = request.FILES['zip_file']

            if uploaded_file:
                # Create a destination path for the uploaded ZIP file
                #dest_folder = os.path.join("C:/Users/PycharmProjects/Accelerator/data/Media/unzipped_files")
                dest_folder = unzip_files()
                zip_path = os.path.join(dest_folder, uploaded_file.name)

                # Save the uploaded ZIP file to the destination
                with open(zip_path, 'wb') as destination:
                    for chunk in uploaded_file.chunks():
                        destination.write(chunk)

                # Unzip the uploaded file
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(dest_folder)

                return JsonResponse({'message': 'File uploaded, extracted, and saved in the specified location.'})

    else:
        print("in else zip")
        form = ZipFileUploadForm()  # Create a new instance of the form

    return render(request, 'zip.html', {'form': form})


def upload_view_hive(request):
    global cancel_clicked

    if request.method == 'POST':
        form = UploadForm1(request.POST, request.FILES)
        files = request.FILES.getlist('file')
        # selected_file_type = request.POST.get('fileType')  # Get the selected file type from the form

        # Initialize progress variables
        total_size = sum(file.size for file in files)
        uploaded_size = 0

        batch_size = 3

        f1 = files
        print(type(f1))

        for i in range(0, len(f1), batch_size):
            for j in range(batch_size):
                if i + j < len(f1):
                    f = f1[i + j]

                    # s = FileSystemStorage(
                    #     location=r"C:\Users\PycharmProjects\Accelerator\Media\user_input")
                    # from Accelerator.paths.config_reader import read_input
                    # storage_location = read_input()
                    # s = FileSystemStorage(location=storage_location)

                    s = FileSystemStorage(location=read_input())

                    filename = s.save(f.name, f)
                    # Update uploaded size and send progress to the client
                    uploaded_size += f.size
                    # progress_percentage = int(uploaded_size / total_size * 100)

        # Now you can use the 'selected_file_type' variable in your logic
    # print(f"Selected File Type: {selected_file_type}")

    return render(request, "hive.html", {'status': True})


def submit_view_hive(request):
    # if request.method == 'POST':
    print("Request.............", request)
    asyncio.run(main.main())
    param_value = request.GET.get('param', None)
    print(param_value)

    def count(N):
        # des_fol = r'C:\Users\PycharmProjects\Accelerator\Media\destination_folder'
        des_fol = write_output()
        # des_fol = Destination_DIR
        # print(des_fol)
        dest_count = len(os.listdir(des_fol))
        progress = (N - dest_count) / N * 100
        print("progress", N, dest_count, progress)
        if progress == -100 or progress < 0:
            progress = 100
        # print("progress",N,dest_count)
        return progress

    param_value = request.GET.get('param', None)
    print(param_value)
    if request.method == 'GET' and param_value == 'parameter1':
        # user_input = r'C:\Users\LSIVASHA\Downloads\CDP (2)\CDP\CDP\PycharmProject_Updated\Accelerator\Media\user_input'
        user_input = read_input()
        N = len(os.listdir(user_input))
        print("N", N)
        progress = count(N)
        print(progress)
        data = {'progress': progress}

        return JsonResponse(data)

def upload_view_spark(request):
    global cancel_clicked
    status = None
    form = UploadForm2(request.POST, request.FILES)
    if request.method == 'POST':

        files = request.FILES.getlist('file')
        selected_file_type = request.POST.get('fileType')  # Get the selected file type from the form
        print(files)

        # Initialize progress variables
        total_size = sum(file.size for file in files)
        uploaded_size = 0

        batch_size = 3

        f1 = files
        print(type(f1))

        for i in range(0, len(f1), batch_size):
            for j in range(batch_size):
                if i + j < len(f1):
                    f = f1[i + j]
                    print(f.name,"@@@@@@@@@@")
                    ext = os.path.splitext(f.name)[-1].lower()
                    print(ext)

                    # s = FileSystemStorage(
                    # location="C:/Users/PycharmProjects/Accelerator/Media/user_input/")
                    if ext == '.py':
                        s = FileSystemStorage(location=read_input())
                        print(s)
                        filename = s.save(f.name, f)
                        # Update uploaded size and send progress to the client
                        uploaded_size += f.size
                        progress_percentage = int(uploaded_size / total_size * 100)
                        status = True
                    else:
                        status = False
        # Now you can use the 'selected_file_type' variable in your logic
        print(f"Selected File Type: {selected_file_type}")
    print(status)
    return render(request, "spark.html", {'status': status,'form': form})




def submit_view_spark(request):
    global submit_clicked
    print(request)

    if request.method == 'POST':
        # forms.SubmitForm(forms.Form):
        print('submit clicked')
        submit_clicked = True

    if cancel_clicked:
        # cancel_clicked = False
        # print("Cancel reset")
        return render(request, "home.html")
    else:
        if submit_clicked:
            # for spark
            print('------- for spark ------------')
            asyncio.run(spark_main.main())
            return render(request, 'spark.html', {'flag': True, 'status': True})


def help_button(request):
    if request.method == 'POST':
        data = request.POST['path']

    # return render(request,'home.html')
    return render(request, 'page.html', {'status': True})


def cancel_view(request):
    global cancel_clicked
    print("Cancel clicked")
    # data = request.POST['path']
    cancel_clicked = True
    return HttpResponse("Cancel")


def files_view(request):
    global cancel_clicked
    print("Cancel clicked")
    # data = request.POST['path']
    cancel_clicked = True
    return HttpResponse("Cancel")


def upload_zip(request):
    if request.method == 'POST':
        # Handle the uploaded zip file here
        uploaded_file = request.FILES.get('zip_file')
        print("in post zip upload 366")
        # Process the uploaded zip file as needed
        # Example: Extract files from the zip file using Python's zipfile module

        return HttpResponse("File uploaded and processed successfully.")  # Replace with your response

    return render(request, 'zip.html')  # Replace 'upload_zip.html' with your template


def zip_view(request):
    print("in zip get html...")
    if request.method == 'POST':
        print("in zip post html...")
        form3 = ZipFileUploadForm(request.POST, request.FILES)
        if form3.is_valid():
            # Handle the uploaded zip file here
            zip_file = form3.cleaned_data['zip_file']
            upload_file = upload()
            print("upload_file", upload_file)
            # Process the zip file or its contents
            # Example: Extract files from the zip file
            # You can use the 'zipfile' module for this purpose
    else:
        form3 = ZipFileUploadForm()  # Create a new instance of the form

    return render(request, "zip.html", {'form': form3})


from django.http import JsonResponse, HttpResponse
from django.shortcuts import render
import os
import zipfile
from .forms import ZipFileUploadForm

uploaded_file_path = "Hello"


def upload_zip(request):
    global uploaded_file_path
    if request.method == 'POST':
        print("in zip upload post line 400")
        print("before file upload")
        uploaded_file = request.FILES['zip_upload']
        print("after file upload", uploaded_file)
        # form = ZipFileUploadForm(request.POST)
        # print(form)
        # if form.is_valid():

        print("in upload form valid")
        uploaded_file = request.FILES['zip_upload']
        print("uploaded_file before upload fun=", uploaded_file)
        # upload()
        print("uploaded_file after upload fun=", uploaded_file, type(uploaded_file))
        ext = os.path.splitext(uploaded_file.name)[-1].lower()
        print("ext=",ext)
        if uploaded_file and ext == '.zip':
            # Specify the destination folder
            dest_folder = os.path.join(Zip_DIR)
            print(Zip_DIR)
            #dest_folder = zip_files()

            print("dest forlder=", dest_folder)
            zip_path = os.path.join(dest_folder, uploaded_file.name)
            uploaded_file_path = zip_path
            print("zip_path=", zip_path)
            # Save the uploaded ZIP file to the destination
            with open(zip_path, 'wb') as destination:
                for chunk in uploaded_file.chunks():
                    destination.write(chunk)

            # Unzip the uploaded file
            # with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            #     zip_ref.extractall(dest_folder)

            # return messages.success(request, 'File upload successful')
            # return JsonResponse({'message': 'File uploaded, extracted, and saved in the specified location.'})
            return render(request, 'zip.html',
                          {'message': 'File uploaded and saved in the specified location.', 'step1': True})

    else:
        return render(request, 'zip.html')  # {'form': form})


def submit_zip(request):
    global uploaded_file_path
    # print("upload file path ",uploaded_file_path)
    if request.method == 'POST':
        print(" in submit=")
        print("upload file path ", uploaded_file_path)
        uploaded_file = os.path.basename(uploaded_file_path).split('/')[-1]
        print("uploaded_file====", uploaded_file)
        # Define the destination folder where you want to save the processed files
        # destination_folder = "C:/Users/LSIVASHA/Downloads/CDP/CDP/PycharmProject_Updated/Accelerator/Media/unzipped_files"
        # uploaded_file = request.FILES['zip_upload']
        destination_folder = os.path.join(Unzip_DIR)
        #destination_folder = unzip_files()
        print("dest folder in submit=", destination_folder)
        #
        # s = FileSystemStorage()
        # print("upload=", uploaded_file, type(uploaded_file))
        #
        # filename = s.save(uploaded_file.name, uploaded_file)
        # uploaded_file_path = s.path(filename)
        # zip_path = os.path.join(destination_folder, uploaded_file.name)
        zip_path = uploaded_file_path
        print("zip_path=", zip_path)
        # Save the uploaded ZIP file to the destination
        # with open(zip_path, 'wb') as destination:
        #     for chunk in uploaded_file.chunks():
        #         destination.write(chunk)

        # Unzip the uploaded file
        print("submit zip==", uploaded_file)
        if uploaded_file:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(destination_folder)
        # Handle the logic for processing the uploaded and extracted files here
        # Example: You can move the extracted files to the destination folder
        # for file_name in extracted_files:
        #     source_path = os.path.join(dest_folder, file_name)
        #     destination_path = os.path.join(destination_folder, file_name)
        #     shutil.move(source_path, destination_path)
        data = {
            'submit_message': 'Files submitted and processed successfully.',
        }
        # return JsonResponse(data, status=200)
        # return HttpResponse(status=200)
        # return Response(data)
        return render(request, 'zip.html',
                      {'submit_message': 'Files submitted and processed successfully.', 'step2': True})
        # return HttpResponse("Files submitted and processed successfully.")  # Replace with your response
    else:
        return render(request, 'zip.html')


# def classify_ddl_complexity(query):
#     num_lines = len(query.split('\n'))
#     print("num_lines==",num_lines)
#     if num_lines > 200:
#         return 'Complex'
#     elif num_lines >= 100:
#         return 'Medium'
#     else:
#         return 'Simple'

complexity_levels = {
    'Simple': (0, 100),
    'Medium': (101, 200),
    'Complex': (201, float('inf'))
}


def get_complexity(num_lines):
    for level, (min_lines, max_lines) in complexity_levels.items():
        if min_lines <= num_lines <= max_lines:
            return level


def count_lines(file_path):
    with open(file_path, 'rb') as file:
        lines = file.readlines()
        return len(lines)

#hello
# def generate_pdf_report(query, complexity):
#     output_pdf = os.path.join(Report_DIR, 'report.pdf')
#     doc = SimpleDocTemplate(output_pdf, pagesize=landscape(letter))
#     elements = []
#     print("output_pdf in generate pdf==",output_pdf)
#     data = [["DDL Query", "Complexity"]]
#     data.append([query, complexity])
#
#     table = Table(data)
#     style = TableStyle([
#         ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
#         ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
#         ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
#         ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
#         ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
#         ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
#         ('GRID', (0, 0), (-1, -1), 1, colors.black)
#     ])
#     table.setStyle(style)
#
#     elements.append(table)
#     doc.build(elements)
#
#     return output_pdf

def generate_pdf_report(directory_path, output_pdf_path):
    doc = SimpleDocTemplate(output_pdf_path, pagesize=letter)

    styles = getSampleStyleSheet()
    elements = []
    data1 = [["File Name", "No. of Lines", "Complexity", "Percentage"]]
    data2 = [["File Name", "No. of Lines", "Complexity", "Percentage"]]
    data3 = [["File Name", "No. of Lines", "Complexity", "Percentage"]]
    # fnmatch.filter(os.listdir(folder_path), '*.pdf')
    file_type = ""
    # table1 = Table(data1)
    # table2 = Table(data2)
    total_lines1 = 0
    total_lines2 = 0
    total_lines3 = 0
    per = 0
    print("data1 type--", type(data1))
    print("directory path==", directory_path)
    os.chdir(directory_path)
    names = {}
    ext_list = ['*.txt', '*.py', '*.xml']
    # and '*.csv' and '*.json' and '*.pdf' and '*.sql' and '*.xlsx' and '*.orc' and '*.avro' and '*.parquet'
    for fn in glob.glob('*.txt' and '*.py' and '*.xml'):
        with open(fn) as f:
            names[fn] = sum(1 for line in f if line.strip())
    total_lines = sum(names.values())

    for root, dirs, files in os.walk(directory_path):
        for file_name in files:
            ext = os.path.splitext(file_name)[-1].lower()
            # file_path = os.path.join(root, file_name)
            # num_lines = count_lines(file_path)
            # complexity = get_complexity(num_lines)

            if ext in ['.txt', '.sql', '.xlsx']:
                file_type = "Hive"

                file_path = os.path.join(root, file_name)
                # print("file path in generate pdf fun==", file_path)
                num_lines = count_lines(file_path)
                complexity = get_complexity(num_lines)
                # total_lines1 += num_lines
                # print("number lines==",num_lines)
                # print("total =", total_lines1)
                percentage = (num_lines / total_lines) * 100 if total_lines > 0 else 0
                data1.append([file_name, num_lines, complexity, f"{percentage:.2f}%"])


                # file_name = os.path.splitext(file_name)[0]
                # data1.append([file_name, num_lines, complexity, per])

            if ext in ['.orc', '.avro', '.parquet', '.py']:
                # elements.append(Paragraph("<b>Spark</b>", styles['Title']))
                file_type = "Spark"
                file_path = os.path.join(root, file_name)
                # print("file path in generate pdf fun==", file_path)
                num_lines = count_lines(file_path)
                # complexity = get_complexity(num_lines)
                complexity = get_complexity(num_lines)
                # total_lines2 += num_lines
                percentage = (num_lines / total_lines) * 100 if total_lines > 0 else 0
                data2.append([file_name, num_lines, complexity, f"{percentage:.2f}%"])




                # file_name = os.path.splitext(file_name)[0]
                # data2.append([file_name, num_lines, complexity, per])
            if ext in ['.csv', '.json', '.xml', '.txt', '.pdf']:
                file_path = os.path.join(root, file_name)
                # print("file path in generate pdf fun==", file_path)
                num_lines = count_lines(file_path)
                complexity = get_complexity(num_lines)
                # total_lines2 += num_lines
                percentage = (num_lines / total_lines) * 100 if total_lines > 0 else 0
                data3.append([file_name, num_lines, complexity, f"{percentage:.2f}%"])


                # file_name = os.path.splitext(file_name)[0]
                # data3.append([file_name, num_lines, complexity, per])

    ##### Hive table ####
    # if file_type == "Hive":
    table1 = Table(data1)
    style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('HALIGN', (9, 9), (-1, -1), 'BOTTOM'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (9, 9), (-1, 0), 20),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ])
    table1.setStyle(style)

    # elements.append(table1)
    # doc.build(elements)

    ##### Spark table ####
    # if file_type == "Spark":
    table2 = Table(data2)
    print(type(table2))
    style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ])
    table2.setStyle(style)
    # elements.append(Paragraph("<b>Spark</b>", styles['Title']))
    # elements.append(table2)
    # doc.build(elements)

    ##### File format table ####
    table3 = Table(data3)
    # table3.hAlign = "TOP"
    print(type(table3))
    style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ])
    table3.setStyle(style)
    # elements.append(Paragraph("<b>Data file</b></n>", styles['Title']))

    data = [[table1, table2, table3]]
    # adjust the length of tables
    # t1_w = 3 * inch
    # t2_w = 3 * inch
    # t3_w = 3 * inch
    # shell_table = Table(data, colWidths=[t1_w, t2_w, t3_w])
    # shell_table.setStyle(TableStyle([
    #     ('HALIGN', (1, 1), (-1, -1), 'BOTTOM')
    # ]))
    elements.append(Paragraph("<b>Hive</b>", styles['Title']))
    elements.append(table1)
    elements.append(Spacer(4, 0.2 * inch))
    elements.append(Paragraph("<b>Spark</b>", styles['Title']))
    elements.append(table2)
    elements.append(Spacer(4, 0.2 * inch))
    elements.append(Paragraph("<b>Data Files</b>", styles['Title']))
    elements.append(table3)
    doc.build(elements)

    # elements.append(Paragraph(f"<b>File Name:</b> {file_name}", styles['Normal']))
    # elements.append(Paragraph(f"<b>Number of Lines:</b> {num_lines}", styles['Normal']))
    # elements.append(Paragraph(f"<b>Complexity:</b> {complexity}", styles['Normal']))
    # elements.append(Spacer(1, 0.2 * inch))


def bold_max_value_in_series(series):
    highlight = 'font-weight: bold;'
    default = ''

    return highlight


def bold_text(text):
    return "\033[1m" + text + "\033[0m"


def generate_csv_report(directory_path, output_pdf_path):
    doc = SimpleDocTemplate(output_pdf_path, pagesize=letter)
    styles = getSampleStyleSheet()

    # writer_object = writer(filedata)
    # with open(output_pdf_path + ".csv", 'a', encoding='UTF8', newline='') as filedata:
    #     header = ["File Name", "No. of Lines", "Complexity"]
    #     # writer = csv.DictWriter(filedata, delimiter=',', fieldnames=header)
    #     writer = csv.writer(filedata, delimiter=',')
    #     elements = []
    #     # data = [["File Name", "No. of Lines", "Complexity"]]

    data = []
    # fnmatch.filter(os.listdir(folder_path), '*.pdf')
    for root, dirs, files in os.walk(directory_path):
        for file_name in files:

            ext = os.path.splitext(file_name)[-1].lower()
            if ext in ['.pdf', '.txt', '.csv', '.xml', '.orc', '.py']:
                # if ext == ".pdf" or ext == ".txt" or ext == ".csv" or ext == ".xml" or ext == ".orc":

                file_path = os.path.join(root, file_name)

                num_lines = count_lines(file_path)
                complexity = get_complexity(num_lines)

                data.append([file_name, num_lines, complexity])

                df = pd.DataFrame(data, columns=["File Name", "No. of Lines", "Complexity"])
                print(df)
                # print('\003[3m' + 'Your text here' + '\033[0m')
                print(os.path.join(Report_DIR, 'report'))
                df.to_latex(bold_rows=True)
                time_now = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M_%S')
                report_path_today = "report" + "_" + time_now + ".csv"
                # df.style.apply(bold_max_value_in_series)
                # df.style.set_properties(**{'font-weight': 'bold',
                #                            'color': 'green'})

                df.to_csv(os.path.join(Report_DIR, report_path_today), index=False, header=True)
                # sublists = [data[i] for i in range(0, len(data))]

        # table = Table(data)
        # style = TableStyle([
        #     ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        #     ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        #     ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        #     ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        #     ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        #     ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        #     ('GRID', (0, 0), (-1, -1), 1, colors.black)
        # ])
        # table.setStyle(style)
        #
        # elements.append(table)
        # writer.writerow(header)
        # writer.writerow(data)

        # elements.append(Paragraph(f"<b>File Name:</b> {file_name}", styles['Normal']))
        # elements.append(Paragraph(f"<b>Number of Lines:</b> {num_lines}", styles['Normal']))
        # elements.append(Paragraph(f"<b>Complexity:</b> {complexity}", styles['Normal']))
        # elements.append(Spacer(1, 0.2 * inch))

    # doc.build(elements)


def submit_generate_report(request):
    selected_file_type = request.POST.get('fileType')
    if request.method == "POST":
        report_path_today = ""
        if selected_file_type == "pdf":
            print("selected_file_type==", selected_file_type)
            print("in post.....")
            # ddl_query = request.form["ddl_query"]
            # print("ddl_query", ddl_query)
            ddl_query = "PDF"
            # complexity = classify_ddl_complexity(ddl_query)
            # print("complexity", complexity)

            final_dirs_list = []  # Create an empty list
            for root, dirs, files in os.walk(Unzip_DIR, topdown=True):
                for d in dirs:
                    if d != 'reports':  # Check if directory name is node_modules
                        final_dirs_list.append(d)  # If it isn't, append directory name to list

            print(final_dirs_list)
            ## get all .pdf files from folder
            count = 0
            pdf_file_name_list = []
            folder_path = ""
            time_now = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M_%S')
            report_today = 'report'
            report_path = os.path.join(Report_DIR, 'report.pdf')
            # folder_path = os.path.join(Unzip_DIR, folder)
            report_path_today = ""
            time_now = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M_%S')
            report_today = 'report' + "_" + time_now + ".pdf"
            for folder in final_dirs_list:
                folder_path = os.path.join(Unzip_DIR, folder)
                # file_list = os.listdir(folder_path)
                # file_list = len(fnmatch.filter(os.listdir(folder_path), '*.pdf'))
                # pdf_file_name_list.append(file_list)
                # file_name = fnmatch.filter(os.listdir(folder_path), '*.pdf')
                report_file_name = os.path.basename(report_path)
                time_now = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M_%S')

                file_name = os.path.splitext(report_file_name)[0]
                print(report_path)
                print(file_name)
                report_path_today = file_name + "_" + time_now + ".pdf"
                print("report_path_today=======", type(report_path_today))
                # for folder_name in final_dirs_list:
                dt = str(datetime.datetime.now())
                newname = file_name + time_now + '.pdf'
                print("report_file_name===========", report_file_name)
                # report_path_today1 = os.rename(report_path, os.path.join(Report_DIR, newname))

                generate_pdf_report(folder_path, os.path.join(Report_DIR, report_path_today))
                # print("output_pdf after replace==", output_pdf)

            file_count = sum(pdf_file_name_list)

            # output_pdf = generate_pdf_report(ddl_query, complexity)

            with open(os.path.join(Report_DIR, os.path.join(Report_DIR, report_path_today)), "rb") as fprb:
                response = HttpResponse(fprb.read(), content_type='application/pdf')
                response['Content-Disposition'] = 'attachment; filename="' + report_path_today + '"'
                return response

        else:
            print("in csv")
            print("selected_file_type==", selected_file_type)
            final_dirs_list = []  # Create an empty list
            for root, dirs, files in os.walk(Unzip_DIR, topdown=True):
                for d in dirs:
                    if d != 'reports':  # Check if directory name is node_modules
                        final_dirs_list.append(d)  # If it isn't, append directory name to list

            ## get all .pdf files from folder
            count = 0
            pdf_file_name_list = []
            folder_path = ""
            report_path = os.path.join(Report_DIR, 'report')
            # folder_path = os.path.join(Unzip_DIR, folder)
            for folder in final_dirs_list:
                folder_path = os.path.join(Unzip_DIR, folder)
                # file_list = os.listdir(folder_path)
                file_list = len(fnmatch.filter(os.listdir(folder_path), '*.pdf'))
                pdf_file_name_list.append(file_list)
                file_name = fnmatch.filter(os.listdir(folder_path), '*.pdf')
                folder_name = os.path.basename(folder_path)

                report_file_name = os.path.basename(report_path)
                time_now = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M_%S')

                file_name = os.path.splitext(report_file_name)[0]
                print(report_path)
                print(file_name)
                report_path_today = file_name + "_" + time_now + ".csv"
                print("report_path_today=======", type(report_path_today))
                # for folder_name in final_dirs_list:
                generate_csv_report(folder_path, os.path.join(Report_DIR, report_path_today))
                # print("output_pdf after replace==", output_pdf)

            file_count = sum(pdf_file_name_list)

            # output_pdf = generate_pdf_report(ddl_query, complexity)

            with open(os.path.join(Report_DIR, os.path.join(Report_DIR, report_path_today)), "rb") as fprb:
                # response = HttpResponse(fprb.read(), content_type='application/pdf')
                # response['Content-Disposition'] = 'attachment; filename="' + 'report.pdf' + '"'

                response = HttpResponse(fprb.read(),
                                        content_type="text/csv",
                                        headers={
                                            "Content-Disposition": 'attachment; filename="' + report_path_today + '"'},
                                        )
                return response

    return render_template("reports.html")


def view_generate_report(request):
    return render(request, 'reports.html')

def view_new_home(request):
    return render(request, 'home_new.html')

def view_data_files(request):
    if request.method == 'GET':
        form2 = forms.UploadForm2()
        print("in html data files view...")
        return render(request, "data_files.html", {'form': form2})
    # return render(request, 'data_files.html')

def upload_data_files(request):
    global cancel_clicked
    status = False
    form = UploadForm2(request.POST, request.FILES)
    if request.method == 'POST':

        files = request.FILES.getlist('file')
        selected_file_type = request.POST.get('fileType')  # Get the selected file type from the form
        print(files)

        # Initialize progress variables
        total_size = sum(file.size for file in files)
        uploaded_size = 0

        batch_size = 3

        f1 = files
        print(type(f1))

        for i in range(0, len(f1), batch_size):
            for j in range(batch_size):
                if i + j < len(f1):
                    f = f1[i + j]
                    print(f.name, "@@@@@@@@@@")
                    ext = os.path.splitext(f.name)[-1].lower()
                    print(ext)

                    # s = FileSystemStorage(
                    # location="C:/Users/PycharmProjects/Accelerator/Media/user_input/")
                    # if ext == '.py':
                    s = FileSystemStorage(location=read_input())
                    print(s)
                    filename = s.save(f.name, f)
                    # Update uploaded size and send progress to the client
                    uploaded_size += f.size
                    progress_percentage = int(uploaded_size / total_size * 100)
                    status = True
                    # else:
                    #     status = False
        # Now you can use the 'selected_file_type' variable in your logic
        print(f"Selected File Type: {selected_file_type}")
    else:
        status = False
    # print(status)
    return render(request, "data_files.html", {'status': status, 'form': form})


def submit_view_data_files(request):
    global submit_clicked
    print(request)

    if request.method == 'POST':
        # forms.SubmitForm(forms.Form):
        print('submit clicked')
        submit_clicked = True

    if cancel_clicked:
        # cancel_clicked = False
        # print("Cancel reset")
        return render(request, "home.html")
    else:
        if submit_clicked:
            # for spark
            print('------- for spark ------------')
            asyncio.run(spark_main.main())
            return render(request, 'data_files.html', {'flag': True, 'status': True})
