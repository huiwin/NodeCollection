@echo off
chcp 65001 >nul 2>&1
title NodeCollection Pro - 一键更新

echo.
echo  ============================================================
echo    NodeCollection Pro - 一键更新
echo  ============================================================
echo.
echo  请选择操作:
echo.
echo    [1] 远程触发更新 (推荐)  - 通过 GitHub API 触发 Actions
echo    [2] 本地运行            - 在本机运行完整流水线
echo    [3] 查看运行状态        - 查看最近 Actions 运行记录
echo    [4] 拉取最新结果        - 从 GitHub 同步订阅文件
echo    [5] 显示订阅链接        - 查看最新订阅地址
echo    [0] 退出
echo.
set /p choice="请输入选项 (默认 1): "

if "%choice%"=="" set choice=1

if "%choice%"=="1" (
    bash "%~dp0update.sh" remote
) else if "%choice%"=="2" (
    bash "%~dp0update.sh" local
) else if "%choice%"=="3" (
    bash "%~dp0update.sh" status
) else if "%choice%"=="4" (
    bash "%~dp0update.sh" pull
) else if "%choice%"=="5" (
    bash "%~dp0update.sh" links
) else if "%choice%"=="0" (
    exit /b 0
) else (
    echo 无效选项
)

echo.
pause
