@echo off
setlocal
cd /d "%~dp0.."
if not exist build mkdir build
go test ./...
if errorlevel 1 exit /b %errorlevel%
go build -trimpath -ldflags="-s -w" -o build\chusan-lan-lab.exe .
if errorlevel 1 exit /b %errorlevel%
echo Built: %CD%\build\chusan-lan-lab.exe
