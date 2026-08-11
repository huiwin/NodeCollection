#!/bin/bash
# ============================================================
# NodeCollection 一键推送脚本
# 使用前请先在 GitHub 上创建一个名为 NodeCollection 的新仓库
# （不要勾选 README / .gitignore / license，保持空仓库）
# 然后把下面的用户名改成你的 GitHub 用户名
# ============================================================

REPO_URL="https://github.com/你的用户名/NodeCollection.git"

# ---- 以下无需修改 ----

echo "正在推送到: $REPO_URL"
git remote remove origin 2>/dev/null
git remote add origin "$REPO_URL"
git push -u origin main

if [ $? -eq 0 ]; then
    echo ""
    echo "========================================"
    echo "  推送成功！"
    echo "  接下来去 GitHub 仓库的 Actions 页面"
    echo "  确认 workflow 已启用即可"
    echo "========================================"
else
    echo ""
    echo "推送失败，请检查："
    echo "1. REPO_URL 是否正确"
    echo "2. 新仓库是否已在 GitHub 上创建"
    echo "3. 是否有 GitHub 登录权限（可能需要输入用户名和密码/token）"
fi
