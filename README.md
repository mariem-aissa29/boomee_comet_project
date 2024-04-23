## Setting the Project Locally:

#### Cloning the project:

Once you have all the needed requirements installed, clone the project:

``` bash
https://github.com/mariem-aissa29/boomee_comet_project.git
```

### Installation
create an virtual environment
```sh
$ virtualenv envname
```
(i hope you know how to activate the environment)
if you don't have installed virtual environment in your pc


```sh
$ pip install virtualenv 
```
install django in your environment

```sh
$ pip install django 
```
or
```sh
$ pip3 install django 
```

# Create django project

```sh
    django-admin- startproject projcetname
```
# Test

here i'm useing authentication as my app
check if everything setup nicely!

```sh
    python3 manage.py runserver
```
and open browser go to http://127.0.0.1:8000/

# create app inside the project

```sh
    python manage.py startapp appname
```

now see you will have new folder with your app name 

then create a urls.py folder inside yor app folder for routing 


```

you have to insatll an adapter to connect your databse with your application
so we have to install psyopg2

```
pip install psyopg2
```
or 
```
pip3 install psycopg2
```

