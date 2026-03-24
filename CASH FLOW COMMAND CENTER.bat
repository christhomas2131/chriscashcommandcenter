@echo off
title CHRI$$$ CA$H FLOW COMMAND CENTER

:: Kill anything currently on port 8501
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8501 "') do (
    taskkill /f /pid %%a >nul 2>&1
)

:: Start the app
cd /d "C:\Users\CZUS\Documents\some-project"
start "Job Tracker" /min cmd /c "python -m streamlit run app.py"

:: Wait for it to boot then open browser
timeout /t 4 /nobreak >nul
start http://localhost:8501
