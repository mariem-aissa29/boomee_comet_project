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
from datetime import datetime
from connexion_pool import get_connection

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
def homePage(request):
    print('home page')
    success_messages = []
    error_messages = {}
    user_name = request.user.username
    if request.method == 'POST':
        print('request method')
        form = csvImportForm(request.POST, request.FILES)
        print('post')
        if form.is_valid():
            csv_file = request.FILES.get('upload-invoices')
            summary_file = request.FILES.get('upload-summary')
            sched_sum_file = request.FILES.get('upload-sched-summary')
            sched_sec_file = request.FILES.get('upload-sched-sec')
            usage_detail_file = request.FILES.get('upload-usage-detail')
            print('usage_detail_file', usage_detail_file)

            if csv_file:
                print('hello csv file')
                # Get the file extension
                file_extension = csv_file.name.split('.')[-1].lower()
                exist_message_invoices=""

                # Check the file extension
                if file_extension not in ['csv', 'xls', 'xlsx']:
                    print('The uploaded file is not a CSV or Excel file')
                    error_messages['error_message_extension_invoices'] = "Vérifier l'extension du fichier factures"
                else:
                    print('Excel extension')
                    message_invoices = handle_csv_file(csv_file)
                    if message_invoices is not None and 'invoices error' in message_invoices:
                        error_messages['error_message_invoices'] = message_invoices['invoices error']
                    else:
                        if message_invoices['exist']:
                            exist_message_invoices = "Fichier invoices déjà existe et remplacé avec succées;;   "
                        if message_invoices['file_data_lines'] == message_invoices['rowcounts']:
                            success_messages.append(exist_message_invoices+"Nombre de lignes dans le fichier invoices: "+ str(message_invoices['file_data_lines']) +" lignes et Nombre de lignes chargées dans la base de données: "+ str(message_invoices['rowcounts']) +" lignes")
                        else:
                            error_messages['error_message_data_not_uploaded'] = "Fichier invoices déjà existe ;; Nombre de lignes dans le fichier invoices: "+ str(message_invoices['file_data_lines']) +" lignes et Nombre de lignes chargées dans la base de données: "+ str(message_invoices['rowcounts']) +" lignes"



            if summary_file:
                # Get the file extension
                file_extension = summary_file.name
                exist_message_summary=""
                if '.' not in file_extension:
                    print('The uploaded file does not have an extension')
                    message_sum = handle_summary_file(summary_file)
                    if message_sum is not None and 'sum error' in message_sum:
                        error_messages['error_message_sum'] = message_sum['sum error']
                    else:
                        if message_sum['exist']:
                            exist_message_summary = "Fichier summary déjà existe et remplacé avec succées;;   "
                        if message_sum['file_data_lines'] == message_sum['rowcounts']:
                            success_messages.append(exist_message_summary+"Nombre de lignes dans le fichier summary: "+ str(message_sum['file_data_lines']) +" lignes et Nombre de lignes chargées dans la base de données: "+ str(message_sum['rowcounts']) +" lignes")
                        else:
                            error_messages['error_message_data_not_uploaded'] = "Fichier summary déjà existe ;; Nombre de lignes dans le fichier summary: "+ str(message_sum['file_data_lines']) +" lignes et Nombre de lignes chargées dans la base de données: "+ str(message_sum['rowcounts']) +" lignes"

                else:
                    error_messages['error_message_extension_summary'] = "Vérifier l'extension du fichier de résumé"


            if sched_sum_file:
                # Get the file extension
                file_extension = sched_sum_file.name
                if '.' not in file_extension:
                    print('The uploaded file does not have an extension')
                    message_sched_sum = handle_sched_sum_file(sched_sum_file)
                    if message_sched_sum is not None and 'sched sum error' in message_sched_sum:
                        error_messages['error_message_sched_sum'] = message_sched_sum['sched sum error']
                    else:
                        if message_sched_sum['exist']:
                            exist_message_summary = "Fichier scheduled summary déjà existe et remplacé avec succées;;   "
                        if message_sched_sum['file_data_lines'] == message_sched_sum['rowcounts']:
                            success_messages.append(exist_message_summary+"Nombre de lignes dans le fichier scheduled summary: "+ str(message_sched_sum['file_data_lines']) +" lignes et Nombre de lignes chargées dans la base de données: "+ str(message_sched_sum['rowcounts']) +" lignes")
                        else:
                            error_messages['error_message_data_not_uploaded'] = "Fichier scheduled summary déjà existe ;; Nombre de lignes dans le fichier scheduled summary: "+ str(message_sched_sum['file_data_lines']) +" lignes et Nombre de lignes chargées dans la base de données: "+ str(message_sched_sum['rowcounts']) +" lignes"
                else:
                    error_messages['error_message_extension_sched_sum'] = "Vérifier l'extension du fichier de résumé planifié"


            if sched_sec_file:
                # Get the file extension
                file_extension = sched_sec_file.name
                exist_message_sched_sec=""
                if '.' not in file_extension:
                    print('The uploaded file does not have an extension')
                    message_sched_sec = handle_sched_sec_file(sched_sec_file)
                    if message_sched_sec is not None and 'sched sec error' in message_sched_sec:
                        error_messages['error_message_sched_sec'] = message_sched_sec['sched sec error']
                    else:
                        if message_sched_sec['exist']:
                            exist_message_sched_sec = "Fichier scheduled securities déjà existe et remplacé avec succées;;   "
                            
                        if message_sched_sec['file_data_lines'] == message_sched_sec['rowcounts']:
                            success_messages.append(exist_message_sched_sec+"Nombre de lignes dans le fichier scheduled securities: "+ str(message_sched_sec['file_data_lines']) +" lignes et Nombre de lignes chargées dans la base de données: "+ str(message_sched_sec['rowcounts']) +" lignes")
                        else:
                            error_messages['error_message_data_not_uploaded'] = "Fichier scheduled securities déjà existe ;; Nombre de lignes dans le fichier scheduled securities: "+ str(message_sched_sec['file_data_lines']) +" lignes et Nombre de lignes chargées dans la base de données: "+ str(message_sched_sec['rowcounts']) +" lignes"
                else:
                    error_messages['error_message_extension_sched_sec'] = "Vérifier l'extension du fichier de titres planifiés"


            if usage_detail_file:
                # Get the file extension
                file_extension = usage_detail_file.name
                exist_message_usage_detail=""
                if '.' not in file_extension:
                    print('The uploaded file does not have an extension')
                    message_usage_detail = handle_usage_detail_file(usage_detail_file)
                    print('message_usage_detail', message_usage_detail)
                    if message_usage_detail is not None and 'usage detail error' in message_usage_detail:
                        error_messages['error_message_usage_detail'] = message_usage_detail['usage detail error']
                    else:
                        if message_usage_detail['exist']:
                            exist_message_usage_detail = "Fichier usage détail déjà existe et remplacé avec succées;;   "
                            
                        if message_usage_detail['file_data_lines'] == message_usage_detail['rowcounts']:
                            success_messages.append(exist_message_usage_detail+"Nombre de lignes dans le fichier usage detail: "+ str(message_usage_detail['file_data_lines']) +" lignes et Nombre de lignes chargées dans la base de données: "+ str(message_usage_detail['rowcounts']) +" lignes")
                        else:
                            error_messages['error_message_data_not_uploaded'] = "Fichier usage détail déjà existe ;; Nombre de lignes dans le fichier usage detail: "+ str(message_usage_detail['file_data_lines']) +" lignes et Nombre de lignes chargées dans la base de données: "+ str(message_usage_detail['rowcounts']) +" lignes"

                else:
                    error_messages['error_message_extension_usage_detail'] = "Vérifier l'extension du fichier de usage détail"


    return render(request, 'home.html',
                  {'success_messages': success_messages, 'error_messages': error_messages, 'user_name': user_name})

def loginPage(request):
    if request.method == 'POST':
        username = request.POST.get('email')
        password=request.POST.get('pass')
        user=authenticate(request,username=username,password=password)
        if user is not None:
            login(request,user)
            return redirect('report')
        else:
            error_message = 'Email ou mot de passe incorrect'
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
    user_name = request.user.username
    return render(request, 'report.html', {'user_name': user_name})


def handle_csv_file(csv_file):
    start_time = time.perf_counter()
    exist=False
    chunk_size = 5000
    max_workers = 10
    
    # Handle CSV file processing
    file_data = csv_file.read().decode('utf-8')
    lines = file_data.split('\n')
    lines.remove(lines[0])
    lines.remove(lines[1])
    lines.remove(lines[2])

    print('****', len(lines))
    lines.remove(lines[-1])
    lines.remove(lines[-2])
    lines.remove(lines[-3])


    print('****lines', len(lines))

    print('*********', lines)

    lines = [line.split(';') for line in lines if line.strip()]  # Split each line by ';'
    length_lines = len(lines)
    print('lines de 2', lines[2][0].split(',')[-7])
    substring = lines[2][0].split(',')[-7]
    print('substring', substring)

    end_date = substring.replace("=", "").replace('"', '')
    print('end_date', end_date)

    
    #end_dates = [line[9] for line in lines]
    exist_date_of_file = check_end_date_in_database("invoices", end_date)
    print('***exist_date_of_file***', exist_date_of_file)

    if exist_date_of_file:
        print('exist file csv')
        exist=True
        delete_exist_data_invoices("invoices", end_date)
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
            #delete_all_insert_invoices()
            return future.result()

    result_migrate = migrate_from_invoices_alim_to_invoices(end_dates[0], length_lines)
    print('result migrate invoices', result_migrate)
    if result_migrate is not None:
        if 'invoices error' in result_migrate:
            delete_all_insert_invoices()
            return result_migrate
    if result_migrate is not None and 'rowcounts' in result_migrate:
        result_migrate['exist']=exist
        return result_migrate

def insert_invoices_file(row):
    data = insert_record_into_invoices_alim(row)
    return data


def handle_summary_file(file):
    start_time = time.perf_counter()
    exist=False
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

    exist_date_of_file = check_date_of_file_in_database("summary", date_of_file)

    if exist_date_of_file:
        exist=True
        delete_exist_data("summary", date_of_file)
        
    file_data = file.read().decode('utf-8')
    lines = file_data.split('\n')

    # Split each line and remove header
    lines = [line.split('|') for line in lines[1:] if line.strip()]
    print('Total lines:', len(lines))
    length_lines = len(lines)
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

    result_migrate = migrate_from_summary_alim_to_summary(date_of_file, length_lines)
    if result_migrate is not None and 'sum error' in result_migrate:
        delete_all_insert_sum()
        return result_migrate
    if result_migrate is not None and 'rowcounts' in result_migrate:
        result_migrate['exist']=exist
        return result_migrate

def insert_summary_file(row):
    data = insert_record_into_summary_alim(row)
    return data


def handle_sched_sum_file(file):
    start_time = time.perf_counter()
    exist=False
    chunk_size = 5000
    max_workers = 10
    # Handle text file processing
    name = file.name
    date_str= name[-6:]
    year = date_str[:4]
    month = date_str[4:]
    date_of_file = month + "/01/" + year
    print('date_of_file', date_of_file)

    exist_date_of_file = check_date_of_file_in_database("sched_sum", date_of_file)
    if exist_date_of_file:
        exist=True
        delete_exist_data("sched_sum", date_of_file)
    
    file_data = file.read().decode('utf-8')
    lines = file_data.split('\n')
    
    # Split each line and remove header
    lines = [line.split('|') for line in lines[1:] if line.strip()]
    length_lines = len(lines)
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

    result_migrate = migrate_from_sched_sum_alim_to_sched_sum(date_of_file, length_lines)
    if result_migrate is not None and 'sched sum error' in result_migrate:
        delete_all_insert_sched_sum()
        return result_migrate
    if result_migrate is not None and 'rowcounts' in result_migrate:
        result_migrate['exist']=exist
        return result_migrate

def insert_sched_sum_file(row):
    data = insert_record_into_sched_sum_alim(row)
    return data


def handle_sched_sec_file(file):
    start_time = time.perf_counter()
    exist=False
    # Define chunk size and max workers
    chunk_size = 5000
    max_workers = 10
    # Handle text file processing
    name = file.name
    date_str = name[-6:]
    year = date_str[:4]
    month = date_str[4:]
    date_of_file = month + "/01/" + year
    print('date_of_file', date_of_file)

    exist_date_of_file = check_date_of_file_in_database("sched_sec", date_of_file)
    print('exist_date_of_file', exist_date_of_file)
    if exist_date_of_file:
        print('true')
        exist=True
        delete_exist_data("sched_sec", date_of_file)

    file_data = file.read().decode('utf-8')
    lines = file_data.split('\n')
    # Split each line and remove header
    lines = [line.split('|') for line in lines[1:] if line.strip()]
    length_lines = len(lines)

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

    result_migrate = migrate_from_sched_sec_alim_to_sched_sec(date_of_file, length_lines)
    if result_migrate is not None and 'sched sec error' in result_migrate:
        delete_all_insert_sched_sec()
        return result_migrate
    if result_migrate is not None and 'rowcounts' in result_migrate:
        result_migrate['exist']=exist
        return result_migrate

def insert_sched_sec_file(row):
    data = insert_record_into_sched_sec_alim(row)
    return data


def handle_usage_detail_file(file):
    start_time = time.perf_counter()
    exist=False
    # Define chunk size and max workers
    chunk_size = 3000
    max_workers = 10
    # Handle text file processing
    name = file.name
    date_str = name[-6:]
    year = date_str[:4]
    month = date_str[4:]
 
    date_of_file = month + "/01/" + year
    print('date_of_file', date_of_file)
    exist_date_of_file = check_date_of_file_in_database("usage_detail", date_of_file)
    if exist_date_of_file:
        print('exist date of file true')
        exist=True
        delete_exist_data("usage_detail", date_of_file)

    file_data = file.read().decode('utf-8')
    lines = file_data.split('\n')

    #Split each line and remove header
    lines = [line.split('|') for line in lines[1:] if line.strip()]
    length_lines = len(lines)
    print('****length_lines****', length_lines)

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

    result_migrate = migrate_from_usage_detail_alim_to_usage_detail(date_of_file, length_lines)
    print('result_migrate', result_migrate)
    if result_migrate is not None and 'usage detail error' in result_migrate:
        delete_all_insert_usage_detail()
        return result_migrate
    if result_migrate is not None and 'rowcounts' in result_migrate:
        result_migrate['exist']=exist
        return result_migrate

def insert_usage_detail_file(row):
    data = insert_record_into_usage_detail_alim(row)
    return data




def check_date_of_file_in_database(table_name, date_of_file):
    conn = get_connection()
    cur = conn.cursor()
    # Parse the input date string
    parsed_date = datetime.strptime(date_of_file, '%d/%m/%Y')
    print('parsed_date', parsed_date)
    # Format the date to the desired format
    formatted_date = parsed_date.strftime('%Y-%m-%d')
    print('formatted_date', formatted_date)

    # Execute a raw SQL query to check if the date exists in the database
    cur.execute(f"SELECT * FROM {table_name} WHERE date_of_file = %s LIMIT 1", [date_of_file])
    row = cur.fetchone()
    print('roww', row)
    count = row[0]
    print('count', count)
    return count

def check_end_date_in_database(table_name, end_date):
    conn = get_connection()
    cur = conn.cursor()
    # Parse the input date string
    parsed_date = datetime.strptime(end_date, '%m/%d/%Y')

    # Execute a raw SQL query to check if the date exists in the database
    cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE end_date = %s", [parsed_date])
    row = cur.fetchone()
    count = row[0]
    print('count', count)

    cur.close()
    conn.close() 
    return count

def delete_exist_data(table_name, date_of_file):
    print('date_of_file', date_of_file)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"delete FROM {table_name} WHERE date_of_file = %s", [date_of_file])
    # res = cur.execute(f"select count(*) FROM {table_name} WHERE date_of_file = %s", [date_of_file])
    # result = cur.fetchone()
    # count = result[0]
    # print('***count***', count)
    conn.commit()
    cur.close()
    conn.close() 

def delete_exist_data_invoices(table_name, end_date):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"delete FROM {table_name} WHERE end_date = %s", [end_date])
    conn.commit()
    cur.close()
    conn.close() 