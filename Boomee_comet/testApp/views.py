import os
dir_path = os.path.dirname(os.path.realpath(__file__))
import sys
sys.path.append(dir_path)
import concurrent.futures
from django.shortcuts import render,redirect,HttpResponse
from django.contrib.auth.models import User
from django.contrib.auth import authenticate,login,logout
from django.contrib.auth.decorators import login_required
from django import forms
import csv
import pandas as pd
from DAO import insert_record_into_invoices_alim,migrate_from_invoices_alim_to_invoices,insert_record_into_summary_alim
from DAO import migrate_from_summary_alim_to_summary, delete_all_insert_sum
from DAO import insert_record_into_sched_sum_alim, delete_all_insert_invoices
from DAO import migrate_from_sched_sum_alim_to_sched_sum, delete_all_insert_sched_sum
from DAO import insert_record_into_sched_sec_alim, migrate_from_sched_sec_alim_to_sched_sec, delete_all_insert_sched_sec
from DAO import insert_record_into_usage_detail_alim, migrate_from_usage_detail_alim_to_usage_detail, delete_all_insert_usage_detail
import time
from django.db import transaction
import django
django.setup()
import psycopg2
from psycopg2 import pool
from django.http import JsonResponse

from django.contrib.auth.decorators import login_required, user_passes_test

class csvImportForm(forms.Form):
    csv_upload = forms.FileField(label='Upload invoices file', required=False )
    summary_file = forms.FileField(label='Upload Summary File', required=False)
    sched_sum_file = forms.FileField(label='Upload Scheduled Summary file', required=False)
    sched_sec_file = forms.FileField(label='Upload Scheduled Securities file', required=False)
    usage_detail_file = forms.FileField(label='Upload Usage Detail file', required=False)


def is_superuser(user):
    return user.is_superuser


@login_required(login_url='login')
@user_passes_test(is_superuser)
def homePage(request):
    success_messages = []
    error_messages = {}
    if request.method == 'POST':
        form = csvImportForm(request.POST, request.FILES)
        if form.is_valid():
            csv_file = request.FILES.get('upload-invoices')
            summary_file = request.FILES.get('upload-summary')
            sched_sum_file = request.FILES.get('upload-sched-summary')
            sched_sec_file = request.FILES.get('upload-sched-sec')
            usage_detail_file = request.FILES.get('upload-usage-detail')

            if csv_file:
                error_message_invoices = handle_csv_file(csv_file)
                if error_message_invoices is not None and 'invoices error' in error_message_invoices:
                    error_messages['error_message_invoices'] = error_message_invoices['invoices error']
                else:
                    success_messages.append("Upload invoices file successfully")

            if summary_file:
                error_message_sum = handle_summary_file(summary_file)
                if error_message_sum is not None and 'sum error' in error_message_sum:
                    error_messages['error_message_sum'] = error_message_sum['sum error']
                else:
                    success_messages.append("Upload Summary file successfully")

            if sched_sum_file:
                error_message_sched_sum = handle_sched_sum_file(sched_sum_file)
                if error_message_sched_sum is not None and 'sched sum error' in error_message_sched_sum:
                    error_messages['error_message_sched_sum'] = error_message_sched_sum['sched sum error']
                else:
                    success_messages.append("Upload Scheduled Summary file successfully")

            if sched_sec_file:
                error_message_sched_sec = handle_sched_sec_file(sched_sec_file)
                if error_message_sched_sec is not None and 'sched sec error' in error_message_sched_sec:
                    error_messages['error_message_sched_sec'] = error_message_sched_sec['sched sec error']
                else:
                    success_messages.append("Upload Scheduled Securities file successfully")

            if usage_detail_file:
                error_message_usage_detail = handle_usage_detail_file(usage_detail_file)
                if error_message_usage_detail is not None and 'usage detail error' in error_message_usage_detail:
                    error_messages['error_message_usage_detail'] = error_message_usage_detail['usage detail error']
                else:
                    success_messages.append("Upload Usage Detail file successfully")


    return render(request, 'home.html',
                  {'success_messages': success_messages, 'error_messages': error_messages})

def loginPage(request):
    if request.method == 'POST':
        username = request.POST.get('email')
        password=request.POST.get('pass')
        user=authenticate(request,username=username,password=password)
        if user is not None:
            login(request,user)
            return redirect('report')
        else:
            error_message = 'Username and password are incorrect'
            return render(request, 'login.html', {'error_message': error_message})

    return render (request, 'login.html')
def SignupPage(request):
    if request.method == 'POST':
        email=request.POST.get('email')
        pass1=request.POST.get('password1')
        pass2=request.POST.get('password2')
        if pass1!=pass2:
            return HttpResponse("password mismatch")
        print(email, pass1, pass2)
        user=User.objects.create_user(email,email, pass1)
        user.save()
        return redirect('login')
    return render (request, 'signup.html')
def logoutPage(request):
    logout(request)
    return redirect('login')

@login_required(login_url='login')
def reportPage(request):
    return render(request, 'report.html')


def handle_csv_file(csv_file):
    start_time = time.perf_counter()
    chunk_size = 5000
    max_workers = 10
    # Handle CSV file processing
    file_data = csv_file.read().decode('utf-8')
    lines = file_data.split('\n')
    lines.remove(lines[0])
    lines = [line for line in lines if
              not (len(line.split(';')) >= 3 and "Sub-Total Current Model:" in line.split(';')[2])]

    lines = [line.split(';') for line in lines if line.strip()]  # Split each line by ';'

    # Create chunks
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]
    print('chunks', chunks)

    # Process chunks concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for chunk in chunks:
            future = executor.submit(insert_invoices_file, chunk)
            futures.append(future)

        # Wait for all tasks to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Wait for the task to finish
            except Exception as e:
                print(f"An error occurred: {e}")

    end_time = time.perf_counter()
    # Calculate the elapsed time
    elapsed_time = end_time - start_time
    print(f"The function took {elapsed_time} seconds to complete.")

    for future in futures:
        if future.result() is not None and 'invoices error' in future.result():
            print("The key 'invoices error' exists in the dictionary.")
            delete_all_insert_invoices()
            return future.result()

    result_migrate = migrate_from_invoices_alim_to_invoices(date_of_file)
    if result_migrate is not None and 'invoices error' in result_migrate:
        delete_all_insert_invoices()
        return result_migrate

def insert_invoices_file(row):
    data = insert_record_into_invoices_alim(row)
    return data


def handle_summary_file(file):
    start_time = time.perf_counter()
    chunk_size = 5000
    max_workers = 10
    # Handle summary file processing
    name = file.name
    print("Name",name)
    date_str= name[-6:]
    print("date",date_str)
    year = date_str[:4]
    month = date_str[4:]
    date_of_file = month + "/01/" + year
    print("Date of file ",date_of_file)
    print("yEAR",year)
    print("month",month)
    print("Hello",name)
    file_data = file.read().decode('utf-8')
    lines = file_data.split('\n')

    # Split each line and remove header
    lines = [line.split('|') for line in lines[1:] if line.strip()]
    print('Total lines:', len(lines))

    # Create chunks
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

    # Process chunks concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for chunk in chunks:
            future = executor.submit(insert_summary_file, chunk)
            futures.append(future)

        # Wait for all tasks to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Wait for the task to finish
            except Exception as e:
                print(f"An error occurred: {e}")

    end_time = time.perf_counter()
    # Calculate the elapsed time
    elapsed_time = end_time - start_time
    print(f"The function took {elapsed_time} seconds to complete.")

    for future in futures:
        if future.result() is not None and 'sum error' in future.result():
            print("The key 'sum error' exists in the dictionary.")
            delete_all_insert_sum()
            return future.result()

    result_migrate = migrate_from_summary_alim_to_summary(date_of_file)
    if result_migrate is not None and 'sum error' in result_migrate:
        delete_all_insert_sum()
        return result_migrate
def insert_summary_file(row):
    data = insert_record_into_summary_alim(row)
    return data


def handle_sched_sum_file(file):
    start_time = time.perf_counter()
    chunk_size = 5000
    max_workers = 10
    # Handle text file processing
    name = file.name
    date_str= name[-6:]
    year = date_str[:4]
    month = date_str[4:]
    date_of_file = month + "/01/" + year
    file_data = file.read().decode('utf-8')
    lines = file_data.split('\n')
    # Split each line and remove header
    lines = [line.split('|') for line in lines[1:] if line.strip()]
    # Create chunks
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

    # Process chunks concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for chunk in chunks:
            future = executor.submit(insert_sched_sum_file, chunk)
            futures.append(future)

        # Wait for all tasks to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Wait for the task to finish
            except Exception as e:
                print(f"An error occurred: {e}")

    # Record the end time
    end_time = time.perf_counter()
    # Calculate the elapsed time
    elapsed_time = end_time - start_time
    print(f"The function took {elapsed_time} seconds to complete.")

    for future in futures:
        if future.result() is not None and 'sched sum error' in future.result():
            print("The key 'sched sum error' exists in the dictionary.")
            delete_all_insert_sched_sum()
            return future.result()

    result_migrate = migrate_from_sched_sum_alim_to_sched_sum(date_of_file)
    if result_migrate is not None and 'sched sum error' in result_migrate:
        delete_all_insert_sched_sum()
        return result_migrate

def insert_sched_sum_file(row):
    data = insert_record_into_sched_sum_alim(row)
    return data


def handle_sched_sec_file(file):
    start_time = time.perf_counter()
    # Define chunk size and max workers
    chunk_size = 5000
    max_workers = 10
    # Handle text file processing
    name = file.name
    date_str = name[-6:]
    year = date_str[:4]
    month = date_str[4:]
    date_of_file = month + "/01/" + year
    file_data = file.read().decode('utf-8')
    lines = file_data.split('\n')
    # Split each line and remove header
    lines = [line.split('|') for line in lines[1:] if line.strip()]

    # Create chunks
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

    # Process chunks concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for chunk in chunks:
            future = executor.submit(insert_sched_sec_file, chunk)
            futures.append(future)

        # Wait for all tasks to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Wait for the task to finish
            except Exception as e:
                print(f"An error occurred: {e}")

    # Record the end time
    end_time = time.perf_counter()
    # Calculate the elapsed time
    elapsed_time = end_time - start_time
    print(f"The function took {elapsed_time} seconds to complete.")

    for future in futures:
        if future.result() is not None and 'sched sec error' in future.result():
            print("The key 'sched sec error' exists in the dictionary.")
            delete_all_insert_sched_sec()
            return future.result()

    result_migrate = migrate_from_sched_sec_alim_to_sched_sec(date_of_file)
    if result_migrate is not None and 'sched sec error' in result_migrate:
        delete_all_insert_sched_sec()
        return result_migrate

def insert_sched_sec_file(row):
    data = insert_record_into_sched_sec_alim(row)
    return data

def handle_usage_detail_file(file):
    start_time = time.perf_counter()
    # Define chunk size and max workers
    chunk_size = 3000
    max_workers = 10
    # Handle text file processing
    name = file.name
    date_str = name[-6:]
    year = date_str[:4]
    month = date_str[4:]
    date_of_file = month + "/01/" + year
    file_data = file.read().decode('utf-8')
    lines = file_data.split('\n')

    # Split each line and remove header
    lines = [line.split('|') for line in lines[1:] if line.strip()]

    # Create chunks
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

    # Process chunks concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for chunk in chunks:
            future = executor.submit(insert_usage_detail_file, chunk)
            futures.append(future)

        # Wait for all tasks to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"An error occurred: {e}")

    end_time = time.perf_counter()
    # Calculate the elapsed time
    elapsed_time = end_time - start_time
    print(f"The function took {elapsed_time} seconds to complete.")

    for future in futures:
        if future.result() is not None and 'usage detail error' in future.result():
            print("The key 'usage detail error' exists in the dictionary.")
            delete_all_insert_usage_detail()
            return future.result()
    result_migrate = migrate_from_usage_detail_alim_to_usage_detail(date_of_file)
    if result_migrate is not None and 'usage detail error' in result_migrate:
        delete_all_insert_usage_detail()
        return result_migrate
def insert_usage_detail_file(row):
    data = insert_record_into_usage_detail_alim(row)
    return data