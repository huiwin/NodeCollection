#!/bin/bash
# ============================================================
# NodeCollection Pro - 一键更新脚本
#
# 功能:
#   1. remote  (默认): 通过 GitHub API 远程触发 Actions 工作流
#   2. local         : 本地运行完整流水线 (下载 subconverter + 运行 main.py + 推送)
#   3. status        : 查看最近一次 Actions 运行状态
#   4. pull          : 拉取 GitHub 上最新的订阅文件到本地
#   5. links         : 显示最新的订阅链接
#
# 用法:
#   bash update.sh              # 等同于 remote
#   bash update.sh remote       # 远程触发更新
#   bash update.sh local        # 本地运行
#   bash update.sh status       # 查看状态
#   bash update.sh pull         # 拉取最新结果
#   bash update.sh links        # 显示订阅链接
#
# 认证方式 (按优先级):
#   1. gh CLI 已登录 → 自动使用
#   2. 环境变量 GITHUB_TOKEN → 使用 curl
#   3. 脚本内 TOKEN 变量 → 使用 curl
# ============================================================

set -euo pipefail

# === 配置区 ===
GITHUB_OWNER="huiwin"
GITHUB_REPO="NodeCollection"
GITHUB_BRANCH="main"
REPO_URL="https://github.com/${GITHUB_OWNER}/${GITHUB_REPO}.git"
API_BASE="https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}"

# 认证 Token (留空则尝试 gh CLI 或环境变量)
# 如需使用，在此填入你的 GitHub Personal Access Token (需要 repo 权限)
TOKEN="${GITHUB_TOKEN:-}"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ============================================================
# 工具函数
# ============================================================

print_banner() {
    echo -e "${CYAN}"
    echo "============================================================"
    echo "  NodeCollection Pro - 一键更新"
    echo "============================================================"
    echo -e "${NC}"
}

print_info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
print_ok()      { echo -e "${GREEN}[OK]${NC}    $*"; }
print_warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
print_error()   { echo -e "${RED}[ERROR]${NC} $*"; }
print_step()    { echo -e "${CYAN}[STEP]${NC}  $*"; }

# 获取认证方式
get_auth_method() {
    # 优先使用 gh CLI
    if command -v gh &>/dev/null && gh auth status &>/dev/null 2>&1; then
        echo "gh"
        return
    fi
    # 其次使用 TOKEN
    if [ -n "$TOKEN" ]; then
        echo "token"
        return
    fi
    # 公开仓库的 GET 请求可以匿名访问
    echo "anonymous"
}

# gh API 封装
gh_api() {
    local method="$1"
    local endpoint="$2"
    local data="${3:-}"

    if [ "$method" = "GET" ]; then
        gh api "$endpoint" 2>/dev/null
    elif [ "$method" = "POST" ]; then
        if [ -n "$data" ]; then
            echo "$data" | gh api "$endpoint" --input - 2>/dev/null
        else
            gh api "$endpoint" --method POST 2>/dev/null
        fi
    fi
}

# curl API 封装 (带 Token)
curl_api() {
    local method="$1"
    local endpoint="$2"
    local data="${3:-}"

    local url="${API_BASE}${endpoint}"
    local args=(-s -X "$method" -H "Accept: application/vnd.github+json" -H "Authorization: Bearer $TOKEN")

    if [ -n "$data" ]; then
        args+=(-H "Content-Type: application/json" -d "$data")
    fi

    curl "${args[@]}" "$url" 2>/dev/null
}

# curl 匿名访问 (公开仓库 GET 请求)
curl_anon() {
    local method="$1"
    local endpoint="$2"

    local url="${API_BASE}${endpoint}"
    curl -s -X "$method" -H "Accept: application/vnd.github+json" "$url" 2>/dev/null
}

# 统一 API 调用
api_call() {
    local method="$1"
    local endpoint="$2"
    local data="${3:-}"

    local auth_method
    auth_method=$(get_auth_method)

    case "$auth_method" in
        gh)
            gh_api "$method" "$endpoint" "$data"
            ;;
        token)
            curl_api "$method" "$endpoint" "$data"
            ;;
        anonymous)
            if [ "$method" = "POST" ]; then
                print_error "触发工作流需要 GitHub 认证"
                print_info "请选择以下任一方式:"
                echo "  1. 安装并登录 gh CLI:  https://cli.github.com/"
                echo "  2. 设置环境变量:       export GITHUB_TOKEN=your_token"
                echo "  3. 编辑本脚本 TOKEN 变量"
                exit 1
            fi
            # GET 请求匿名访问
            curl_anon "$method" "$endpoint"
            ;;
    esac
}

# 检查命令是否存在
check_cmd() {
    if ! command -v "$1" &>/dev/null; then
        return 1
    fi
    return 0
}

# ============================================================
# 远程触发 Actions 工作流
# ============================================================

do_remote() {
    print_step "远程触发 GitHub Actions 工作流"

    # 先检查认证 (POST 请求需要认证)
    local auth_method
    auth_method=$(get_auth_method)

    if [ "$auth_method" = "anonymous" ]; then
        print_error "触发工作流需要 GitHub 认证"
        print_info "请选择以下任一方式:"
        echo ""
        echo "  方式1 (推荐): 安装并登录 gh CLI"
        echo "    下载: https://cli.github.com/"
        echo "    登录: gh auth login"
        echo ""
        echo "  方式2: 设置环境变量"
        echo "    export GITHUB_TOKEN=ghp_your_personal_access_token"
        echo "    (Token 需要 repo 权限, 在 GitHub Settings → Developer settings → Tokens 生成)"
        echo ""
        echo "  方式3: 编辑本脚本内的 TOKEN 变量"
        echo "    打开 update.sh, 找到 TOKEN= 行, 填入你的 Token"
        echo ""
        print_info "配置完成后重新运行: bash update.sh remote"
        exit 1
    fi

    # 触发 workflow_dispatch
    local result
    result=$(api_call "POST" "/actions/workflows/fetch.yaml/dispatches" '{"ref":"'"$GITHUB_BRANCH"'"}' 2>&1)

    # 检查是否触发成功 (gh 成功时无输出，curl 成功时返回空或 204)
    if [ $? -eq 0 ]; then
        print_ok "工作流已触发"
    else
        print_error "触发失败，请检查认证和仓库权限"
        print_info "返回信息: $result"
        exit 1
    fi

    echo ""
    print_info "等待 5 秒后查询运行状态..."
    sleep 5

    # 查询最新的运行
    monitor_latest_run
}

# ============================================================
# 监控最新的 workflow run
# ============================================================

monitor_latest_run() {
    print_step "查询最新的工作流运行"

    local runs
    runs=$(api_call "GET" "/actions/runs?per_page=1")

    if [ -z "$runs" ] || [ "$runs" = "null" ]; then
        print_error "无法获取工作流运行信息"
        return 1
    fi

    # 提取运行信息 (兼容 gh 和 curl 的 JSON 输出)
    local run_id run_status run_conclusion run_url
    run_id=$(echo "$runs" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    run = data.get('workflow_runs', [{}])[0]
    print(run.get('id', ''))
except: print('')
" 2>/dev/null || echo "")

    if [ -z "$run_id" ]; then
        print_error "无法解析运行 ID"
        return 1
    fi

    run_url="https://github.com/${GITHUB_OWNER}/${GITHUB_REPO}/actions/runs/${run_id}"
    print_ok "找到运行: ID=${run_id}"
    print_info "运行链接: ${run_url}"
    echo ""

    # 轮询状态
    print_step "监控运行状态 (每 10 秒刷新一次, Ctrl+C 退出)"
    echo ""

    local spinner='|/-\'
    local i=0
    local prev_status=""

    while true; do
        local run_detail
        run_detail=$(api_call "GET" "/actions/runs/${run_id}")

        local status conclusion
        status=$(echo "$run_detail" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data.get('status', 'unknown'))
except: print('error')
" 2>/dev/null || echo "error")

        conclusion=$(echo "$run_detail" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    c = data.get('conclusion', '')
    print(c if c else 'pending')
except: print('error')
" 2>/dev/null || echo "error")

        # 状态翻译
        local status_cn
        case "$status" in
            queued)       status_cn="排队中" ;;
            in_progress)  status_cn="运行中" ;;
            completed)    status_cn="已完成" ;;
            *)            status_cn="$status" ;;
        esac

        # 显示进度
        printf "\r${YELLOW}[%c]${NC} 状态: ${status_cn}  " "${spinner:$((i % 4)):1}"
        i=$((i + 1))

        if [ "$status" = "completed" ]; then
            echo ""
            echo ""
            case "$conclusion" in
                success)
                    print_ok "运行成功! 结论: ${conclusion}"
                    echo ""
                    show_latest_links
                    ;;
                failure)
                    print_error "运行失败! 结论: ${conclusion}"
                    print_info "查看日志: ${run_url}"
                    ;;
                cancelled)
                    print_warn "运行已取消"
                    ;;
                *)
                    print_warn "运行结束: ${conclusion}"
                    ;;
            esac
            break
        fi

        sleep 10
    done
}

# ============================================================
# 查看最近运行状态
# ============================================================

do_status() {
    print_step "查询最近 5 次工作流运行"

    local runs
    runs=$(api_call "GET" "/actions/runs?per_page=5")

    if [ -z "$runs" ]; then
        print_error "无法获取运行信息"
        exit 1
    fi

    echo ""
    printf "%-12s %-12s %-15s %-20s %s\n" "RUN ID" "状态" "结论" "创建时间" "链接"
    printf "%-12s %-12s %-15s %-20s %s\n" "------------" "----------" "---------------" "--------------------" "----------"

    echo "$runs" | python3 -c "
import sys, json
data = json.load(sys.stdin)
runs = data.get('workflow_runs', [])
for r in runs[:5]:
    rid = r.get('id', '')
    status = r.get('status', '')
    conclusion = r.get('conclusion', '') or '...'
    created = r.get('created_at', '')[:19].replace('T', ' ')
    url = r.get('html_url', '')
    print(f'{rid:<12} {status:<12} {conclusion:<15} {created:<20} {url}')
" 2>/dev/null

    echo ""
    print_info "完整列表: https://github.com/${GITHUB_OWNER}/${GITHUB_REPO}/actions"
}

# ============================================================
# 本地运行完整流水线
# ============================================================

do_local() {
    print_step "本地运行完整流水线"
    echo ""

    local start_time
    start_time=$(date +%s)

    # 1. 检查 Python
    print_step "[1/7] 检查 Python 环境"
    if check_cmd python3; then
        PYTHON=python3
    elif check_cmd python; then
        PYTHON=python
    else
        print_error "未找到 Python，请先安装 Python 3"
        exit 1
    fi
    print_ok "Python: $($PYTHON --version 2>&1)"

    # 2. 检查依赖
    print_step "[2/7] 检查 Python 依赖"
    local need_install=false
    for pkg in requests yaml tqdm loguru retry; do
        if ! $PYTHON -c "import $pkg" 2>/dev/null; then
            need_install=true
            break
        fi
    done

    if [ "$need_install" = true ]; then
        print_info "安装依赖..."
        $PYTHON -m pip install -r "${SCRIPT_DIR}/requirements.txt" -q
        print_ok "依赖安装完成"
    else
        print_ok "依赖已安装"
    fi

    # 3. 下载/检查 subconverter
    print_step "[3/7] 检查 subconverter"

    local sc_dir="${SCRIPT_DIR}/subconverter_bin"
    local sc_binary=""

    # 判断操作系统
    local os_type
    os_type=$(uname -s)

    case "$os_type" in
        MINGW*|MSYS*|CYGWIN*)
            # Windows
            sc_binary="${sc_dir}/subconverter/subconverter.exe"
            local sc_url="https://github.com/tindy2013/subconverter/releases/latest/download/subconverter_windows64.7z"
            local need_7z=true
            ;;
        Linux)
            sc_binary="${sc_dir}/subconverter/subconverter"
            local sc_url="https://github.com/tindy2013/subconverter/releases/latest/download/subconverter_linux64.tar.gz"
            ;;
        Darwin)
            sc_binary="${sc_dir}/subconverter/subconverter"
            local sc_url="https://github.com/tindy2013/subconverter/releases/latest/download/subconverter_darwin64.tar.gz"
            ;;
        *)
            print_error "不支持的操作系统: $os_type"
            exit 1
            ;;
    esac

    if [ -f "$sc_binary" ]; then
        print_ok "subconverter 已存在"
    else
        print_info "下载 subconverter..."
        mkdir -p "$sc_dir"

        local tmp_file="${sc_dir}/sc_download"

        if [ "$os_type" = "MINGW"* ] || [ "$os_type" = "MSYS"* ] || [ "$os_type" = "CYGWIN"* ]; then
            # Windows: 下载 7z 并解压
            curl -sL -o "${tmp_file}.7z" "https://github.com/tindy2013/subconverter/releases/latest/download/subconverter_windows64.7z"
            if check_cmd 7z; then
                7z x "${tmp_file}.7z" -o"$sc_dir" -y >/dev/null 2>&1
            else
                print_warn "未找到 7z，尝试使用 PowerShell 解压..."
                powershell.exe -Command "Expand-Archive -Path '${tmp_file}.7z' -DestinationPath '$sc_dir' -Force" 2>/dev/null || \
                print_error "无法解压 7z 文件，请安装 7-Zip"
            fi
            rm -f "${tmp_file}.7z"
        else
            # Linux/macOS: 下载 tar.gz 并解压
            curl -sL -o "${tmp_file}.tar.gz" "$sc_url"
            tar xzf "${tmp_file}.tar.gz" -C "$sc_dir"
            rm -f "${tmp_file}.tar.gz"
        fi

        if [ -f "$sc_binary" ]; then
            chmod +x "$sc_binary" 2>/dev/null || true
            print_ok "subconverter 下载完成"
        else
            print_error "subconverter 下载失败"
            print_info "可手动下载并解压到: ${sc_dir}"
            exit 1
        fi
    fi

    # 4. 启动 subconverter
    print_step "[4/7] 启动 subconverter"

    # 先检查是否已有实例在运行
    if curl -s http://127.0.0.1:25500/version >/dev/null 2>&1; then
        print_ok "subconverter 已在运行"
        local sc_was_running=true
    else
        local sc_was_running=false
        cd "$sc_dir/subconverter"
        if [ "$os_type" = "MINGW"* ] || [ "$os_type" = "MSYS"* ] || [ "$os_type" = "CYGWIN"* ]; then
            ./subconverter.exe >/dev/null 2>&1 &
        else
            nohup ./subconverter >/dev/null 2>&1 &
        fi
        local sc_pid=$!
        cd "$SCRIPT_DIR"
        sleep 3

        if curl -s http://127.0.0.1:25500/version >/dev/null 2>&1; then
            print_ok "subconverter 已启动 (PID: $sc_pid)"
        else
            print_error "subconverter 启动失败"
            exit 1
        fi
    fi

    # 5. 运行主程序
    print_step "[5/7] 运行 main.py"
    echo ""
    cd "$SCRIPT_DIR"
    $PYTHON main.py
    echo ""
    print_ok "main.py 执行完成"

    # 6. 停止 subconverter (如果不是之前就在运行的)
    print_step "[6/7] 清理 subconverter"
    if [ "$sc_was_running" = false ]; then
        if [ "$os_type" = "MINGW"* ] || [ "$os_type" = "MSYS"* ] || [ "$os_type" = "CYGWIN"* ]; then
            taskkill.exe //F //IM subconverter.exe >/dev/null 2>&1 || true
        else
            pkill -f subconverter 2>/dev/null || true
        fi
        print_ok "subconverter 已停止"
    else
        print_info "subconverter 之前已在运行，保持不动"
    fi

    # 7. 显示结果
    print_step "[7/7] 运行结果"

    local end_time
    end_time=$(date +%s)
    local elapsed=$((end_time - start_time))

    echo ""
    echo -e "${GREEN}============================================================${NC}"
    echo -e "${GREEN}  本地运行完成!  耗时: ${elapsed}s${NC}"
    echo -e "${GREEN}============================================================${NC}"
    echo ""

    # 显示输出文件
    print_info "输出文件:"
    if [ -d "${SCRIPT_DIR}/output" ]; then
        find "${SCRIPT_DIR}/output" -type f -newer "${SCRIPT_DIR}/main.py" 2>/dev/null | while read -r f; do
            local size
            size=$(du -h "$f" 2>/dev/null | cut -f1)
            echo "  $f  (${size})"
        done
    fi

    if [ -f "${SCRIPT_DIR}/sub" ]; then
        find "${SCRIPT_DIR}/sub" -type f -name "*.yaml" -newer "${SCRIPT_DIR}/main.py" 2>/dev/null | while read -r f; do
            local size
            size=$(du -h "$f" 2>/dev/null | cut -f1)
            echo "  $f  (${size})"
        done
    fi

    # 询问是否推送到 GitHub
    echo ""
    if [ -d "${SCRIPT_DIR}/.git" ]; then
        read -p "是否将结果推送到 GitHub? (y/N): " push_confirm
        if [ "$push_confirm" = "y" ] || [ "$push_confirm" = "Y" ]; then
            push_to_github
        else
            print_info "已跳过推送。文件保存在本地。"
        fi
    else
        print_info "当前目录不是 Git 仓库，如需推送请先初始化或使用 deploy.sh"
        echo ""
        show_latest_links
    fi
}

# ============================================================
# 推送到 GitHub
# ============================================================

push_to_github() {
    print_step "推送到 GitHub"

    cd "$SCRIPT_DIR"

    git add ./sub ./output ./README.md 2>/dev/null || true

    local staged
    staged=$(git diff --staged --stat)

    if [ -z "$staged" ]; then
        print_info "没有需要提交的变更"
        return
    fi

    echo ""
    print_info "变更概览:"
    git status --short
    echo ""

    local commit_msg
    commit_msg="Local update $(date '+%Y-%m-%d %H:%M:%S')"

    git commit -m "$commit_msg"
    git push origin "$GITHUB_BRANCH"

    print_ok "推送完成"
    echo ""
    show_latest_links
}

# ============================================================
# 拉取 GitHub 最新结果
# ============================================================

do_pull() {
    print_step "从 GitHub 拉取最新订阅文件"

    local temp_dir
    temp_dir="/tmp/nc-pull-$$"

    print_info "克隆仓库到临时目录..."
    git clone --depth 1 "$REPO_URL" "$temp_dir" 2>/dev/null

    if [ ! -d "$temp_dir" ]; then
        print_error "克隆失败"
        exit 1
    fi

    # 复制 sub/ 和 output/ 到本地
    print_info "同步文件..."

    if [ -d "$temp_dir/sub" ]; then
        cp -r "$temp_dir/sub" "${SCRIPT_DIR}/"
        print_ok "已同步 sub/"
    fi

    if [ -d "$temp_dir/output" ]; then
        cp -r "$temp_dir/output" "${SCRIPT_DIR}/"
        print_ok "已同步 output/"
    fi

    if [ -f "$temp_dir/README.md" ]; then
        cp "$temp_dir/README.md" "${SCRIPT_DIR}/"
        print_ok "已同步 README.md"
    fi

    # 清理
    rm -rf "$temp_dir"

    echo ""
    print_ok "拉取完成"
    echo ""
    show_latest_links
}

# ============================================================
# 显示最新订阅链接
# ============================================================

show_latest_links() {
    print_step "最新订阅链接"

    # 尝试从本地 README.md 读取
    local readme="${SCRIPT_DIR}/README.md"

    if [ -f "$readme" ]; then
        # 提取 README 中的订阅链接表格
        echo ""
        cat "$readme"
        echo ""
        return
    fi

    # 如果本地没有，从 GitHub Raw 获取
    print_info "本地无 README.md，从 GitHub 获取..."
    local raw_url="https://raw.githubusercontent.com/${GITHUB_OWNER}/${GITHUB_REPO}/${GITHUB_BRANCH}/README.md"

    local content
    content=$(curl -sL "$raw_url" 2>/dev/null)

    if [ -n "$content" ]; then
        echo ""
        echo "$content"
        echo ""
    else
        print_warn "无法获取 README.md"
        print_info "手动访问: ${raw_url}"
    fi
}

# ============================================================
# 主入口
# ============================================================

main() {
    print_banner

    local mode="${1:-remote}"

    case "$mode" in
        remote|trigger)
            do_remote
            ;;
        local)
            do_local
            ;;
        status)
            do_status
            ;;
        pull)
            do_pull
            ;;
        links|link)
            show_latest_links
            ;;
        help|--help|-h)
            echo "用法: bash update.sh [命令]"
            echo ""
            echo "命令:"
            echo "  remote   (默认) 远程触发 GitHub Actions 工作流并监控"
            echo "  local    本地运行完整流水线 (subconverter + main.py)"
            echo "  status   查看最近 5 次工作流运行状态"
            echo "  pull     从 GitHub 拉取最新订阅文件到本地"
            echo "  links    显示最新订阅链接"
            echo "  help     显示帮助"
            echo ""
            echo "认证:"
            echo "  仅 remote 模式 (触发工作流) 需要 GitHub 认证"
            echo "  status/pull/links 读取公开仓库, 无需认证"
            echo "  方式1: 安装并登录 gh CLI (推荐)"
            echo "  方式2: export GITHUB_TOKEN=your_token"
            echo "  方式3: 编辑脚本内 TOKEN 变量"
            ;;
        *)
            print_error "未知命令: $mode"
            echo "运行 'bash update.sh help' 查看帮助"
            exit 1
            ;;
    esac
}

main "$@"
