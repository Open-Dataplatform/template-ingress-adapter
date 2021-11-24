@ECHO OFF

CALL E:\ingress-{{cookiecutter.repo_name}}\venv\Scripts\activate.bat
PUSHD E:
CD E:\ingress-{{cookiecutter.repo_name}}
CALL python -m ingress_{{cookiecutter.module_name}}
CALL E:\ingress-{{cookiecutter.repo_name}}\venv\Scripts\deactivate.bat
