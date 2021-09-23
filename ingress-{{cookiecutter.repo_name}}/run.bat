@ECHO OFF

CALL E:\ingress-adapter-{{cookiecutter.repo_name}}\venv\Scripts\activate.bat
PUSHD E:
CD E:\ingress-adapter-{{cookiecutter.repo_name}}
CALL python -m ingress_adapter_{{cookiecutter.module_name}}.adapter
CALL E:\ingress-adapter-{{cookiecutter.repo_name}}\venv\Scripts\deactivate.bat
