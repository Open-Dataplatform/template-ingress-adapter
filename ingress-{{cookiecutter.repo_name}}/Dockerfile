FROM python:3.8-slim-bullseye

COPY ./ingress_{{cookiecutter.module_name}} ./ingress_{{cookiecutter.module_name}}
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt
