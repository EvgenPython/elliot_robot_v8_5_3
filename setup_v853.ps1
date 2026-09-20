$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
py -3.11 -m venv .venv
& .\.venv\Scripts\python.exe -m pip install -U pip
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt
& .\.venv\Scripts\python.exe -X utf8 .\test_v853_resilience.py
& .\.venv\Scripts\python.exe -X utf8 .\verify_v853_resilience.py
