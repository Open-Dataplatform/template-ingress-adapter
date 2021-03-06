# template-ingress-adapter <!-- omit in toc -->
- [Introduction](#introduction)
- [Installation](#installation)
  - [Logging](#usage)
  
## Introduction

This cookiecutter template can be used to generate a repository for an ingress adapter. This repository will
contain all the basic project structure including tox, configuration files, readme, docker etc.

## Installation

You need to install cookiecutter. It is best if you install it as part of your local Python libraries and 
not inside a venv.

You can install cookiecutter using the following command:

```
pip install cookiecutter
```

Insure that cookiecutter is on your PATH. The installation process will typically give you are warning if
this is not the case.

### Usage

You can create a new ingress adapter repository (locally) using the following command (you need to 
be in the directory where you want to place the repository): 

```sh
cookiecutter https://github.com/Open-Dataplatform/template-ingress-adapter.git
```

Say yes to the question "Is it okay to delete and re-download it?".

You will be prompted for a `name`. This name must be in lower case and should identify the 
adapter you are building, such as jao or ikontrol. The name will be prefixed with ingress-, e.g. `ingress-ikontrol`.
Answer the following two questions (`repo_name` and `module_name`) by pressing enter, unless you want to change the
default values.

You will be prompted for the environment (`test` or `prod`) and you can type the dataset GUID and service principle. If
you want to fill them out later, simply skip by pressing enter.
