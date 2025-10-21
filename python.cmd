@echo off
REM Lightweight shim to expose the project virtualenv's interpreter as `python`.
"%~dp0\.venv\Scripts\python.exe" %*
