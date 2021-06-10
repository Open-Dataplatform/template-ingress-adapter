# template-ingress-adapter <!-- omit in toc -->
- [Introduction](#introduction)
- [Installation](#installation)
  - [Logging](#logging)
- [Development](#development)
  - [Running locally](#running-locally)
  - [tox](#tox)
  - [Commands](#commands)
    - [Linting](#linting)
    - [Tests](#tests)
  
## Introduction

This cookiecutter template can be used to generate a repository for an ingress adapter. This repository will
contain all the basic project structure including tox, configuration files, readme, docker etc.

## Installation

You need to install cookiecutter. It is best if you install it as part of your local Python libraries and 
not inside a venv.

You can install cookiecutter usinge the following command:

```
pip install cookiecutter
```

Insure that cookiecutter is on your PATH. The installation process will typically give you are warning if
this is not the case.

### Usage

You can create a new ingress adapter repository (locally )using the following command: 

```sh
cookiecutter https://github.com/Open-Dataplatform/template-ingress-adapter.git
```

This will prompt your for a `system_name`. This name must be in lower case and should identify the 
adapter you are building, such as jao or ikontrol.
