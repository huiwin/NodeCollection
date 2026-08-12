# About NodeCollection

Telegram + Airport 订阅源采集与多格式转换一体化工具，通过 GitHub Actions 每 4 小时自动运行。

## 特色

- **多源采集**：34+ Telegram 频道 + 50+ 机场域名，双通道并行爬取
- **自动分类**：智能识别机场订阅、Clash 配置、V2Ray Base64 三种类型
- **多格式输出**：通过 subconverter 引擎转换，支持 Clash / V2Ray / Surge / Mixed 四种格式
- **ACL4SSR 规则**：内置完整分流规则集（广告拦截、国内外分流、流媒体解锁等）
- **代理组优化**：自动按地区分组（香港/台湾/日本/新加坡/美国），支持自动测速、故障转移、负载均衡
- **Emoji 标注**：节点名称自动添加国旗 Emoji，一目了然
- **安全防护**：SSRF 防护（内网 IP 过滤）、URL 安全校验、连接超时控制
- **稳定可靠**：ThreadPoolExecutor 并发 + 连接池复用 + retry 重试机制
- **自动部署**：GitHub Actions 全自动，subconverter 二进制运行时自动下载，无需额外配置
- **向后兼容**：保留原始 YAML 格式输出，不破坏已有工作流

## 目录结构

```
NodeCollection/
├── main.py                        主程序 (采集 + 转换 + README 生成)
├── config.yaml                    Telegram 频道列表配置
├── airports.yaml                  机场域名列表 (50+ 条目)
├── requirements.txt               Python 依赖
├── README.md                      订阅链接展示页 (自动生成)
├── ABOUT.md                       本文件 (项目说明)
├── update.sh                      一键更新脚本 (5种模式)
├── update.bat                     Windows 批处理入口 (双击运行)
├── .gitignore                     Git 忽略规则
├── .github/
│   └── workflows/
│       └── fetch.yaml             GitHub Actions 工作流 (每4小时)
├── subconverter/
│   └── external_config.ini        subconverter 外部配置 (代理组+分流规则)
├── sub/                           原始采集数据 (向后兼容)
│   └── YYYY/
│       └── M/
│           └── M-D.yaml           按日期组织的 YAML 文件
└── output/                        多格式转换输出
    ├── clash/
    │   └── M-D.yaml               Clash 配置文件
    ├── v2ray/
    │   └── M-D.txt                V2Ray Base64 订阅
    ├── surge/
    │   └── M-D.conf               Surge 配置文件
    ├── mixed/
    │   └── M-D.txt                混合格式 Base64
    └── index.json                 最新输出索引
```

## 技术栈

| 组件 | 技术 | 说明 |
| :--- | :--- | :--- |
| 语言 | Python 3 | 标准库 + 第三方库 |
| HTTP 请求 | requests + Session | 连接池复用，降低开销 |
| 并发模型 | ThreadPoolExecutor | 32 线程校验 + 8 线程爬取 |
| 日志 | loguru | 结构化日志输出 |
| 进度条 | tqdm | 实时进度显示 |
| 重试 | retry | 网络请求自动重试 |
| 配置 | PyYAML | YAML 读写 |
| 格式转换 | subconverter | C++ 引擎，本地 API 调用 |
| CI/CD | GitHub Actions | 每 4 小时定时执行 |
| 规则集 | ACL4SSR | 分流规则 + 广告拦截 |

## 架构

```
config.yaml (TG频道)           airports.yaml (机场域名)
       |                                |
       v                                v
 crawl_all_channels()          probe_all_airports()
 (8线程并发爬取)                (8线程并发探测)
       |                                |
       +---------- 合并去重 ------------+
                      |
                      v
             check_all_urls()
             (32线程校验+分类)
                      |
         +------------+------------+
         |                         |
         v                         v
  sub/YYYY/M/M-D.yaml      subconverter API
  (原始YAML,向后兼容)       (127.0.0.1:25500)
                                |
                   +-----+-----+-----+-----+
                   |     |     |     |     |
                   v     v     v     v     v
                 clash  v2ray surge mixed index.json
                                |
                                v
                        generate_readme()
                        (自动更新README.md)
                                |
                                v
                     git add + commit + push
```

## 订阅格式说明

| 格式 | 适用客户端 | 说明 |
| :--- | :--- | :--- |
| Clash | Clash for Windows / ClashX / Mihomo / Clash Meta | 完整配置含代理组+规则 |
| V2Ray | V2RayN / V2RayNG / Shadowrocket | Base64 编码，兼容性最广 |
| Surge | Surge 4+ (iOS/macOS) | Surge 原生配置格式 |
| Mixed | 通用 | 所有协议混合的 Base64 |
| 原始 YAML | 开发者 / 调试 | 含分类信息的原始数据 |

## 加速代理说明

国内直连 `raw.githubusercontent.com` 可能较慢，支持以下加速方式：

| 加速方式 | 格式 | 实时性 | 说明 |
| :--- | :--- | :--- | :--- |
| kkgithub | 替换域名 | 实时 | `raw.githubusercontent.com` → `raw.kkgithub.com` |
| ghproxy.net | 加前缀 | 实时 | 在原始链接前加 `https://ghproxy.net/` |
| gh-proxy.com | 加前缀 | 实时 | 在原始链接前加 `https://gh-proxy.com/` |
| ghfast.top | 加前缀 | 实时 | 在原始链接前加 `https://ghfast.top/` |
| jsdelivr | 改路径 | 缓存延迟 | `cdn.jsdelivr.net/gh/user/repo@branch/path` |

> 推荐：主用 kkgithub（格式最简单），备记一个 ghproxy 地址以防万一。

## 一键更新脚本

项目附带 `update.sh` + `update.bat` 一键更新工具，支持五种操作模式：

| 命令 | 说明 | 适用场景 |
| :--- | :--- | :--- |
| `bash update.sh remote` | 远程触发 Actions 并实时监控 | 不想等定时任务，立即更新 (默认) |
| `bash update.sh local` | 本地运行完整流水线 | GitHub Actions 不可用或需调试 |
| `bash update.sh status` | 查看最近 5 次运行状态 | 检查 Actions 是否正常 |
| `bash update.sh pull` | 从 GitHub 拉取最新订阅文件 | 同步远程结果到本地 |
| `bash update.sh links` | 显示最新订阅链接 | 快速查看可用订阅地址 |

Windows 用户可直接双击 `update.bat` 选择操作。

**认证方式** (仅 `remote` 模式触发工作流需要)：
1. 安装并登录 [gh CLI](https://cli.github.com/) (推荐)
2. 或设置环境变量 `export GITHUB_TOKEN=your_token`
3. 或编辑 `update.sh` 内的 `TOKEN` 变量

> `status`、`pull`、`links` 模式读取公开仓库数据，无需认证。

## 部署方式

### GitHub Actions（推荐）

仓库已配置 `.github/workflows/fetch.yaml`，每 4 小时自动执行：
1. 检出代码 → 安装 Python 依赖
2. 下载并启动 subconverter
3. 运行 `main.py` 采集 + 转换
4. 停止 subconverter
5. 自动 commit + push

无需任何手动操作。如需立即更新，运行 `bash update.sh remote` 即可。

### 本地运行

```bash
# 方式1: 使用一键脚本 (自动下载 subconverter + 安装依赖 + 运行 + 推送)
bash update.sh local

# 方式2: 手动运行
pip install -r requirements.txt
# 下载 subconverter 并启动
./subconverter &
python main.py
```

## Credits

- 原始项目: [huiwin/collectSub-google](https://github.com/huiwin/collectSub-google)
- Fork 来源: [RenaLio/proxy-minging](https://github.com/RenaLio/proxy-minging)
- 格式转换: [tindy2013/subconverter](https://github.com/tindy2013/subconverter)
- 机场列表: [moneyfly1/jichangnodes](https://github.com/moneyfly1/jichangnodes)
- 分流规则: [ACL4SSR](https://github.com/ACL4SSR/ACL4SSR)
