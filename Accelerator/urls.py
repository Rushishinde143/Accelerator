"""Accelerator URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path

#from Accelerator.CDH2CDP import views
# from .CDH2CDP import views
from src import views

# path('select_folder/', views.folder_selection),
urlpatterns = [
    path('admin/', admin.site.urls),
    path('', views.home_view),
    path('spark/', views.spark_view),
    path('hive/', views.hive_view),
    path('zip/', views.zip_view),
    path('Upload-hive/', views.upload_view_hive),
    path('Upload-spark/', views.upload_view_spark),
    #path('Upload-zip/', views.upload_view_zip),
    path('submit-hive/', views.submit_view_hive),
    path('submit-spark/', views.submit_view_spark),
    path('submit-zip/', views.submit_zip),
    path('reports/', views.view_generate_report),
    path('submit_reports/', views.submit_generate_report),
    path('cancel/', views.cancel_view),
    path('contact_us/', views.help_button),
    path('get_random_value/', views.submit_view_hive),
    path('upload-zip/', views.upload_zip, name='upload_zip_file'),



]
