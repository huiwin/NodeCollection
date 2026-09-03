# NodeCollection Pro 参照文档汇总

> **文档用途**：整合项目所有参照文档，供团队统一阅读、查阅和后续维护。
> **当前版本**：v2.7.0 (P15 完成)
> **最后更新**：2026-09-03
> **仓库地址**：https://github.com/huiwin/NodeCollection

---

## 目录 (TOC)

- [1. 项目概述](#1-项目概述)
- [2. 版本历史](#2-版本历史)
- [3. 目录结构](#3-目录结构)
- [4. 文件参照详解](#4-文件参照详解)
  - [4.1 main.py — 主程序](#41-mainpy--主程序)
  - [4.2 config.yaml — Telegram 频道配置](#42-configyaml--telegram-频道配置)
  - [4.3 airports.yaml — 机场域名列表](#43-airportsyaml--机场域名列表)
  - [4.4 requirements.txt — Python 依赖](#44-requirementstxt--python-依赖)
  - [4.5 .gitignore — Git 忽略规则](#45-gitignore--git-忽略规则)
  - [4.6 fetch.yaml — GitHub Actions 工作流](#46-fetchyaml--github-actions-工作流)
  - [4.7 external_config.ini — subconverter 配置](#47-external_configini--subconverter-配置)
  - [4.8 update.sh — 一键更新脚本](#48-updatesh--一键更新脚本)
  - [4.9 update.bat — Windows 入口](#49-updatebat--windows-入口)
  - [4.10 deploy.sh — 部署脚本](#410-deploysh--部署脚本)
  - [4.11 clean_and_normalize.sh — Git 历史清理](#411-clean_and_normalizesh--git-历史清理)
  - [4.12 ABOUT.md — 项目说明](#412-aboutmd--项目说明)
  - [4.13 README.md — 订阅展示页（自动生成）](#413-readmemd--订阅展示页自动生成)
  - [4.14 merge.yaml — 上游订阅融合白名单（v1.4.0）](#414-mergeyaml--上游订阅融合白名单v140)
- [5. 关键参数速查表](#5-关键参数速查表)
- [6. 操作指南](#6-操作指南)
  - [6.1 日常更新](#61-日常更新)
  - [6.2 部署代码变更](#62-部署代码变更)
  - [6.3 本地调试](#63-本地调试)
  - [6.4 Git 历史清理](#64-git-历史清理)
- [7. 常见问题排查](#7-常见问题排查)
- [8. Credits](#8-credits)
- [9. 更新记录](#9-更新记录)

---

## 1. 项目概述

Telegram + Airport 订阅源采集与多格式转换一体化工具，通过 GitHub Actions 每 4 小时自动运行。

**核心流程**：

```
config.yaml (TG频道)  +  airports.yaml (机场域名)
           ↓                      ↓
   并发爬取频道            探测机场公开订阅
           ↓                      ↓
              合并去重 → 校验分类
                          ↓
              ┌───────────┴───────────┐
              ↓                       ↓
      原始 YAML (向后兼容)     subconverter API
                                    ↓
              ┌─────┬─────┬─────┬─────┐
              ↓     ↓     ↓     ↓     ↓
            Clash  V2Ray Surge Mixed index.json
                                    ↓
                          generate_readme()
                                    ↓
                      git add + commit + push
```

**特色**：多源采集（34+ TG 频道 + 50+ 机场）、自动分类、四种格式输出、ACL4SSR 分流规则、按地区代理组、Emoji 标注、SSRF 防护、并发 + 重试机制、GitHub Actions 全自动。

---

## 2. 版本历史

采用 [语义化版本](https://semver.org/lang/zh-CN/)（Semantic Versioning），从 v1.0.0 起递增。

| 版本 | 日期 | 变更类型 | 说明 |
| :--- | :--- | :--- | :--- |
| v1.0.0 | 2026-08-11 | 首次稳定版 | collectSub → NodeCollection，基础采集功能 |
| v1.1.0 | 2026-08-11 | minor | 集成 subconverter + jichangnodes 机场列表 |
| v1.2.0 | 2026-08-11 | minor | 新增 update.sh / update.bat 一键更新脚本 |
| v1.3.0 | 2026-08-12 | minor | 目录结构重组 + archive/ 归档 |
| v1.3.1 | 2026-08-13 | patch | 版本号规范化 + README 超链接化 + deploy.sh 修复 |
| v1.3.1+ | 2026-08-17 | 补丁 | 订阅链接固定化 (latest 机制)：日期戳文件照常生成，同时复制一份到 `latest.{ext}` 固定路径；README/index.json 改为指向固定路径，URL 永不变 |
| v1.3.2 | 2026-08-17 | patch | subconverter 加固：超时 30s→60s + 3 次重试 (间隔 5s)；latest 回退保护——转换失败且 latest 缺失时按文件名日期复制最近历史文件，固定链接永不 404 |
| v1.4.1 | 2026-08-17 | patch | 修复: ① 上游拉取失败 (429/10054) 增加 3 镜像回退; ② 转换失败且目录不存在时 os.listdir 崩溃 (FileNotFoundError); ③ merged 输出 v2ray/mixed 同名 .txt 互相覆盖, 文件名增加格式 token; ④ subconverter 不可用提示启动命令 |
| v1.4.2 | 2026-08-18 | patch | 修复 P0 A4 验收缺陷: generate_merged_format 调用 call_subconverter 时误将 'merged/<格式>' 作为 subconverter target 参数 (实为非法格式名), 导致 4 种融合格式转换全部失败、output/merged/ 文件从未发布 (404); 修正为仅传格式名 (clash/v2ray/surge&ver=4/mixed), 输出目录由 MERGED_DIR 控制 |
| v1.4.0 | 2026-08-17 | minor | 上游订阅融合 (External Sources)：merge.yaml 白名单 + 拉取解析安检 + `[ext:来源]` 前缀 + 独立输出 `output/merged/`；README 免责声明与来源致谢表 |
| v1.5.0 | 2026-08-19 | minor | P1 质量控制 (全部完成)。T1.1 延迟测速器：extract_host_port 协议无关提取 host/port、_tcp_connect_latency TCP 握手计时、measure_uri_latencies/measure_proxy_latencies 32 线程并发测速。T1.2 排序与剔除：generate_merged_format 流程扩展为 解析→去重→测速→健康更新→剔除→排序→转换；filter_and_sort_nodes 按延迟升序 (None 排末尾) + 连续 LATENCY_FAIL_THRESHOLD(2) 次不可达剔除；load_node_health 从 index.json 读取历史健康记录，健康记录嵌入 merged.quality.node_health 跨周期持久化。T1.4 截断升级：删除 parse_upstream_text 简单 max_nodes 截断，改在 generate_merged_format 排序后做「单源 max_nodes + 总量 MERGED_MAX_NODES(200)」两层按延迟截断，单源截断后重新全局排序。T1.5 质量指标日志：quality 字段扩展为 total_parsed/total_available(剔除后存活)/excluded/truncated/output_count/availability_rate/avg_latency_ms/max_latency_ms/min_latency_ms/node_health；main() 运行统计输出融合可用率与平均延迟。T1.3 地区分组：新增 subconverter/merged_config.ini (7 地区分组含新增韩英)；call_subconverter 增加 config_path 参数；generate_merged_format 传入 merged 独立配置；generate_readme 融合订阅段落增加节点分组说明表。T1.1 43/43、T1.2 31/31、T1.4 24/24、T1.5 32/32、T1.3 39/39 验收测试全部通过 |
| v1.6.0 | 2026-08-19 | minor | P2 体验增强 (全部完成)。T2.1 Sing-box 输出：OUTPUT_FORMATS 新增 singbox target (output/singbox/{M-D}.json + latest.json)，主订阅与融合订阅均支持，含 latest 回退保护，README 新增 Sing-box 订阅链接。T2.2 历史版本保留：新增 cleanup_old_versions 函数，每格式保留最近 HISTORY_KEEP_COUNT(5) 版日期文件，超出自动清理最旧版本；主订阅按格式目录清理，融合订阅按格式 token 分组清理；latest.* 文件与非日期文件不受影响。T2.3 上游监控与降级标记：新增 load_upstream_health 函数，上游拉取健康记录嵌入 index.json merged.upstream_health；连续 UPSTREAM_DEGRADE_THRESHOLD(3) 次拉取失败标记 degraded:true，恢复后自动解除；README 来源表新增「状态」列显示 ✅正常 / ⚠️异常(N次失败) / ⚠️已降级(连续N次失败)。T2.4 Credits 自动同步：merge.yaml 新增可选 repo 字段，README 来源表优先级为 merge.yaml repo 字段 > 从 raw.githubusercontent.com URL 自动提取 > UPSTREAM_REPO_MAP 回退；增删上游无需手改 README。P2 验收测试 57/57 通过，全量回归 226/226 通过 |
| v1.7.0 | 2026-08-20 | minor | P3 性能优化与可观测性增强 (全部完成)。T3.1 性能优化组合：LATENCY_TIMEOUT 5→4s、LATENCY_THREADS 32→48、LATENCY_FAIL_THRESHOLD 2→3 (对冲超时缩短的误杀)；新增冷却期跳过测速机制——连续失败 COOLDOWN_ENTRY_THRESHOLD(2) 次进入冷却期，冷却期内跳过测速 (fail_count 冻结)，每 COOLDOWN_RETRY_INTERVAL(3) 周期强制重试一次，最多重试 COOLDOWN_MAX_RETRIES(3) 次，全部失败则剔除，重试成功退出冷却期；measure_uri_latencies/measure_proxy_latencies 增加 health_data 参数支持冷却期跳过；filter_and_sort_nodes 重写冷却期逻辑；健康记录扩展 cooling_down/cooldown_cycles/retry_count 字段。T3.2 智能去重：新增 dedup_uris_by_host_port/dedup_proxies_by_host_port 函数，从基于完整 URI/name 去重改为基于 host:port 去重，同一节点多参数只保留一个，无法提取 host:port 时回退到完整 URI/name 去重。T3.3 协议过滤：新增 PROTOCOL_COMPATIBILITY 协议兼容性矩阵 (clash/v2ray/surge/mixed/singbox 各支持协议列表) 与 PROTOCOL_NAMES 中文名称映射；README 各格式订阅段落新增「支持协议」标注。T3.4 上游贡献统计：generate_merged_format 新增 upstream_parsed 解析数统计，测速剔除后计算每上游 after_dedup/available/avg_latency/availability_rate，写入 index.json merged.upstream_stats；README 来源表扩展为 8 列 (来源/项目地址/解析/去重后/可达/可用率/平均延迟/状态)。T3.5 Web 状态页：新增 generate_status_page 函数，从 index.json 读取数据生成 output/status.html (概览卡片+质量指标表+上游贡献统计表，单文件内嵌 CSS 暗色主题)；fetch.yaml 新增 peaceiris/actions-gh-pages 部署步骤，将 output 目录部署到 GitHub Pages。P3 验收测试 149/149 通过，全量回归 288/288 通过 |
| v1.8.0 | 2026-08-21 | minor | P4 上游扩展与资源优化 (全部完成)。T4.1 上游扩展：移除低质量上游 freenode (可用率仅 7.1%, 14节点仅1可达)；新增 Pawdroid/Free-servers (base64 vmess 订阅, 6小时自动更新, raw.githubusercontent.com)；新增 Jsnzkpg (外部域名 sub.445569.xyz, 多协议 vless/hysteria2/vmess/ss/trojan, 标准 base64 订阅)；更新 UPSTREAM_REPO_MAP，上游从 3 个变为 4 个 (NoMoreWalls/FreeNodes/Pawdroid/Jsnzkpg)。T4.2 资源优化：清理 config.yaml 中 5 个无效频道 (v2list/freev2rays/vmess_tg/mftizi/ssrList)，这些频道分享裸节点而非订阅链接，无法被 check_all_urls 识别为有效订阅，浪费爬取时间；频道从 42 个精简至 37 个。T4.3 subconverter 配置修复：修复融合订阅 Clash 报错问题——subconverter 的 config=file:// API 参数在 Linux 上不生效，导致 external_config.ini/merged_config.ini 未加载，subconverter 使用自带默认配置 (默认配置代理组定义有 bug 导致 YAML 结构错误，第一个代理组出现两个 proxies 键，NoMoreWalls/FreeNodes 节点全部丢失)；修复方案：fetch.yaml 启动 subconverter 前将 external_config.ini 复制为工作目录下的 pref.ini (subconverter 默认配置文件名)；main.py 移除 call_subconverter API 调用中的 config=file:// 参数 (配置已通过 pref.ini 加载)；external_config.ini 新增 🇰🇷 韩国 / 🇬🇧 英国分组 (与 merged_config.ini 统一，主订阅和融合订阅共用一个配置)。T4.4 版本与文档：版本号 v1.7.0→v1.8.0；reference-docs.md 同步更新版本历史、上游白名单、Credits、更新记录 |
| v1.9.0 | 2026-08-24 | minor | P5 质量优化与体验增强 (全部完成)。T5.1 无效节点过滤增强：新增 is_invalid_node_host() 函数，过滤 127.0.0.x/0.0.0.0/10.x/192.168.x/172.16-31.x 等本地保留地址节点；URI 节点在 parse_upstream_text 解析后、测速前过滤无效地址。T5.2 融合订阅地区分组修复：地区组正则增加 [代码] 前缀匹配，新增 🇩🇪德国/🇫🇷法国/🇷🇺俄罗斯地区组。T5.3 节点地区识别增强 (轻量方案)：新增 REGION_PATTERNS (19个地区) 和 detect_region() 函数，基于服务器名正则匹配识别地区，为无标识节点添加 [地区代码] 前缀。T5.4 subconverter 过滤调优：调整 exclude_remarks 正则，移除宽泛词避免过度过滤。T5.5 Web 状态页增强：新增 Chart.js 上游贡献饼图和延迟分布柱状图，质量卡片扩展到6个，暗色渐变+毛玻璃+响应式。T5.6 告警通知预留接口：新增 ALERT_WEBHOOK_URL 配置项和 send_alert() 函数，支持钉钉/飞书/Server酱/通用Webhook，默认不启用 |
| v2.7.0 | 2026-09-03 | patch | P15 主订阅转换持续失败根因修复。T15.1 根因定位——①主订阅 all_sub_urls 混入大量空壳/失效订阅 (20 URL 中 16 个无效: HTTP 200 但 proxies 为空的机场模板页/404/不可达/403), subconverter 合并请求 (| 分隔) 只要一个 URL 无法处理就整体返回 400, 导致主订阅转换失败, output/clash/ 最新日期文件停留在 8-21, latest 一直保留旧缓存; ②主订阅用 _rename_with_region_emoji 生成 🇫🇷 01 (含 emoji), subconverter remove_old_emoji 拆掉 emoji 后纯数字被转成 int (1,2,3...), 地区 filter 无法匹配导致地区组无法填充。T15.2 新增 filter_valid_sub_urls (合并测试优先 + 失败逐个剔除)——先合并所有 URL 用 subconverter 测试一次, 成功则零额外开销; 失败 (400) 则逐个 URL 测试剔除无效源; 主订阅 20 URL → 4 个有效 (go4sharing/chromego/glados/xship)。T15.3 主订阅重命名改回 _rename_with_region (US 01 无 emoji), 与融合订阅一致, 由 subconverter emoji 规则负责加 emoji, 转换后 _filter_and_rename_clash_file 兜底为 🇫🇷 01 格式。T15.4 external_config.ini 删除冲突 rename 规则 (会把序号 01 误判为倍率 (01x)) + 扩充 emoji 规则 (加拿大/澳大利亚/荷兰/印度/巴西/波兰) + 新增 🇨🇦 加拿大节点组。效果: 主订阅 87 节点/15 代理组/地区组填充 (美 8/港 8/德 6/日 4/加 1 等)/🇫🇷 01 格式/0 违规词, 回归测试 149 断言通过; 版本号 v2.6.2→v2.7.0 |
| v2.6.1 | 2026-09-01 | patch | P13 修复地区分组未恢复 + 兜底重命名失效。T13.1 修复 detect_region 类型保护——subconverter 输出有 name 为纯数字 (int) 的节点, detect_region 调用 int.lower() 抛 AttributeError, 导致 _filter_and_rename_clash_file 兜底函数整体异常返回 False, 违规词过滤和重命名全部失效 (输出保留 '🇺🇸 US 01'/原始名称混排); 修复: text = str(text) 类型保护。T13.2 修复 merged_config.ini section——整个文件只有 [custom] section, 而 subconverter 标准是 [common]/[rulesets] (与 P6/P9.1 修复 external_config.ini 同因), 导致 subconverter 完全不加载 merged_config.ini, 融合订阅地区分组从未生效 (实际输出是默认模板的 10 个通用组); 修复: 重写 merged_config.ini 对齐 external_config.ini 结构 ([custom]→[common], 补 clash_rule_base, ruleset 移到 [rulesets], 地区组扩展为 11 个含韩国/英国/德国/法国/俄罗斯/加拿大, 地区 filter 含 emoji+[代码] 格式)。T13.3 同步更新 proxy-groups 引用——兜底重命名只改 proxies 的 name 未同步 proxy-groups 引用, 导致代理组引用失效; 修复: 建立 rename_map 同步更新所有 proxy-groups 的 proxies 列表。效果: 融合订阅节点名统一为 🇺🇸 01/🇫🇷 02, 地区分组恢复, 违规词兜底重新生效; 版本号 v2.6.0→v2.6.1 |
| v2.6.0 | 2026-08-31 | minor | P12 保留地区+序号重命名, 恢复地区分组。T12.1 新增 REGION_EMOJI_MAP (地区代码→旗帜emoji, 18个地区) + _rename_with_region (生成 US 01/JP 02, 转换前使用) + _rename_with_region_emoji (生成 🇺🇸 01/🇭🇰 02, 输出兜底使用)。T12.2 融合订阅重命名保留地区 (generate_merged_format)——原统一重命名为纯序号 01/02/03 丢失地区信息, 导致地区分组 filter 匹配不到任何节点、香港/台湾/日本/新加坡/美国/韩国/英国地区组全部消失; 新: 从原始节点名提取地区, 重命名为 US 01/JP 02, 既规避违规词又保留地区分组。T12.3 主订阅重命名同样保留地区 (US 01 格式)。T12.4 输出兜底 _filter_and_rename_clash_file 改为保留地区 emoji (🇺🇸 01/🇯🇵 02), 不再破坏预处理保留的地区信息。T12.5 merged_config.ini 地区 filter 补充 emoji 匹配 (如 (美国|US|United States|🇺🇸)), 确保兜底后的 emoji 节点名能被地区组匹配; external_config.ini 原本已含 emoji 无需修改。效果: 融合/主订阅地区分组恢复正常, 无地区节点进自动选择; 版本号 v2.5.6→v2.6.0 |
| v2.5.6 | 2026-08-31 | patch | P11.6 修复融合订阅节点数异常。T11.11 修复 call_subconverter 忽略 config_path 参数 Bug——之前融合订阅调用时 merged_config.ini (含韩国/英国分组) 从未生效, 实际始终使用主订阅的 external_config.ini; 修复: effective_config = config_path if config_path else SUBCONVERTER_EXTERNAL_CONFIG。T11.12 过滤 subconverter 无法输出的协议节点——上游解析支持 vless/hysteria2/hy2/tuic 等新协议, 但 subconverter 的 Clash 输出模板 (all_base.tpl) 不支持这些协议, 导致融合订阅解析 458 个节点 (含 hysteria2 115 + vless 43) 而 subconverter 转换后只剩 26 个 trojan/ss/vmess 节点, index.json 记录 output_count=150 与实际输出不符; 修复: 在 generate_merged_format 解析后过滤不支持的协议 (保留 ss/ssr/vmess/trojan/snell/socks5/http, 过滤 vless/hysteria/hysteria2/hy2/tuic/wireguard/mixed), available_count 与实际输出一致, 且被过滤协议无需测速减少耗时; 版本号 v2.5.5→v2.5.6 |
| v2.5.5 | 2026-08-30 | patch | P11.5 融合订阅兜底重命名 + 延迟阈值放宽。T11.9 融合订阅兜底重命名——v2.5.4 的最终兜底只遍历 output/clash/ 未覆盖 output/merged/, 导致融合订阅重命名不生效; 在 generate_merged_format 末尾添加遍历 output/merged/ 目录所有 .clash.yaml 文件的兜底。T11.10 延迟阈值放宽——LATENCY_MAX_THRESHOLD 800→2000ms, 恢复融合订阅节点数量; 版本号 v2.5.4→v2.5.5 |
| v2.5.4 | 2026-08-29 | patch | P11.4 最终兜底——遍历 output/clash/ 目录所有 yaml 文件调用 _filter_and_rename_clash_file(), 主订阅节点重命名终于生效 (01~87); 版本号 v2.5.3→v2.5.4 |
| v2.5.3 | 2026-08-29 | patch | P11.3 新增 _filter_and_rename_clash_file() 输出兜底函数 (解析 Clash YAML → 过滤违规词 → 重命名 → 写回), 在 generate_multi_format 输出循环中调用; 版本号 v2.5.2→v2.5.3 |
| v2.5.2 | 2026-08-28 | patch | P11.2 修复重命名预处理返回值 bug——预处理失败时返回值被 if processed_urls 误判为成功; 版本号 v2.5.1→v2.5.2 |
| v2.5.1 | 2026-08-28 | patch | P11.1 新增 Python 层违规词源头过滤 (ILLEGAL_KEYWORDS + contains_illegal_keyword), 主订阅预处理改为直接 requests.get 调 subconverter API; 版本号 v2.5.0→v2.5.1 |
| v2.5.0 | 2026-08-28 | minor | P11 节点质量优化——LATENCY_MAX_THRESHOLD 800ms、MERGED_MAX_NODES 150、质量排序 (延迟+稳定性); 版本号 v2.4.1→v2.5.0 |
| v2.4.1 | 2026-08-27 | patch | P10.1 在 generate_merged_format 添加融合订阅重命名; 版本号 v2.4.0→v2.4.1 |
| v2.4.0 | 2026-08-27 | minor | P10 节点统一重命名 (01/02/03) 规避违规词 + 新增订阅源 (TG stymei1 + maflya); 版本号 v2.3.3→v2.4.0 |
| v2.0.0 | 2026-08-24 | major | P6 subconverter 配置修复与地区分组恢复 (根本修复)。T6.1 subconverter 配置 section 名称修正：自 P4 以来融合订阅 Clash 一直缺少地区组/故障转移/负载均衡等自定义代理组，根本原因是 external_config.ini 错误使用 [custom] section，而 subconverter 标准 section 是 [common]/[rulesets]/[proxy_groups]。重写 external_config.ini 将配置项分到正确的三个 section，所有 custom_proxy_group（11个地区组+故障转移+负载均衡+节点选择）正确加载。T6.2 融合订阅节点数预计从 58 恢复正常。版本号 v1.9.0→v2.0.0 |

---

## 3. 目录结构

```
NodeCollection/
│
├── main.py                        主程序 (采集 + 转换 + README 生成)
├── config.yaml                    Telegram 频道列表配置
├── airports.yaml                  机场域名列表 (50+ 条目)
├── requirements.txt               Python 依赖
├── README.md                      订阅链接展示页 (Actions 自动生成)
├── ABOUT.md                       项目说明
├── update.sh                      一键更新脚本 (5种模式)
├── update.bat                     Windows 批处理入口 (双击运行)
├── merge.yaml                     上游订阅融合白名单 (v1.4.0)
├── .gitignore                     Git 忽略规则
│
├── .github/
│   └── workflows/
│       └── fetch.yaml             GitHub Actions 工作流 (每4小时)
│
├── subconverter/
│   └── external_config.ini        subconverter 外部配置 (代理组+分流规则)
│
├── sub/                           原始采集数据 (运行时生成)
│   └── YYYY/M/M-D.yaml            按日期组织的 YAML 文件
│
├── output/                        多格式转换输出 (运行时生成)
│   ├── .gitkeep                   目录占位
│   ├── clash/M-D.yaml             Clash 配置
│   ├── v2ray/M-D.txt              V2Ray Base64
│   ├── surge/M-D.conf             Surge 配置
│   ├── mixed/M-D.txt              混合格式 Base64
│   ├── singbox/M-D.json           Sing-box JSON (v1.6.0 新增)
│   ├── merged/M-D.{format}.{ext}   融合节点输出 (v1.4.0, 独立于主订阅, 文件名带格式 token 防止 v2ray/mixed 同为 .txt 互相覆盖)
│   └── index.json                 最新输出索引 (含 merged 段)
│
├── deploy.sh                      部署脚本 (本地, gitignored)
├── clean_and_normalize.sh         Git 历史清理脚本 (本地, gitignored)
└── archive/                       一次性脚本归档 (gitignored)
    ├── gen_configs.py
    ├── gen_html.py
    ├── deployment-guide.html
    └── integration-plan.html
```

> **仓库文件** vs **本地文件**：`deploy.sh`、`clean_and_normalize.sh`、`archive/` 已加入 `.gitignore`，仅本地使用，不上传 GitHub。

---

## 4. 文件参照详解

### 4.1 main.py — 主程序

| 属性 | 值 |
| :--- | :--- |
| 文件类型 | Python 3 |
| 仓库路径 | `main.py` |
| 是否部署 | 是（FILES_TO_COPY 第一项） |
| 自动生成 | 否 |

**核心功能**：采集 Telegram 频道节点 + 探测机场公开订阅 → 校验分类去重 → subconverter 多格式转换 → 生成 README.md

**关键配置常量**（第 42-87 行）：

```python
# 路径
CONFIG_PATH = './config.yaml'
AIRPORTS_PATH = './airports.yaml'
MERGE_PATH = './merge.yaml'      # 上游订阅白名单 (v1.4.0)
SUB_DIR = 'sub'
OUTPUT_DIR = 'output'
MERGED_DIR = 'merged'            # 融合输出子目录 (v1.4.0)

# 并发与超时
MAX_THREADS = 32          # 校验线程数
CHANNEL_THREADS = 8       # 频道爬取线程数
AIRPORT_THREADS = 8       # 机场探测线程数
REQUEST_TIMEOUT = 10      # 通用请求超时 (秒)
CHANNEL_TIMEOUT = 15      # 频道爬取超时 (秒)
AIRPORT_TIMEOUT = 8       # 机场探测超时 (秒)
RETRY_TIMES = 2           # 重试次数

# subconverter
SUBCONVERTER_URL = os.environ.get('SUBCONVERTER_URL', 'http://127.0.0.1:25500')
SUBCONVERTER_TIMEOUT = 60        # 单次转换请求超时 (秒)
SUBCONVERTER_RETRIES = 3         # 转换失败重试次数
SUBCONVERTER_RETRY_DELAY = 5     # 重试间隔 (秒)
SUBCONVERTER_EXTERNAL_CONFIG = 'subconverter/external_config.ini'

# 上游订阅融合 (v1.4.0)
UPSTREAM_THREADS = 8             # 上游拉取并发线程
UPSTREAM_TIMEOUT = 60            # 上游拉取超时 (秒)
UPSTREAM_RETRIES = 3             # 上游拉取重试次数
UPSTREAM_RETRY_DELAY = 5         # 重试间隔 (秒)

# GitHub 仓库信息 (环境变量可覆盖)
GITHUB_OWNER = os.environ.get('GITHUB_OWNER', 'huiwin')
GITHUB_REPO = os.environ.get('GITHUB_REPO', 'NodeCollection')
GITHUB_BRANCH = os.environ.get('GITHUB_BRANCH', 'main')

# 输出格式: (target参数, 输出子目录, 文件扩展名)
# v1.6.0 新增 singbox (Sing-box / SagerNet / Hiddify)
OUTPUT_FORMATS = [
    ('clash', 'clash', 'yaml'),
    ('v2ray', 'v2ray', 'txt'),
    ('surge&ver=4', 'surge', 'conf'),
    ('mixed', 'mixed', 'txt'),
    ('singbox', 'singbox', 'json'),
]
```

**加速代理前缀配置**（第 72-79 行）：

```python
PROXY_PREFIXES = [
    ('原生', '{url}'),
    ('kkgithub', 'https://raw.kkgithub.com/{GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_BRANCH}{path}'),
    ('ghproxy.net', 'https://ghproxy.net/{url}'),
    ('gh-proxy.com', 'https://gh-proxy.com/{url}'),
    ('ghfast.top', 'https://ghfast.top/{url}'),
    ('jsdelivr', 'https://fastly.jsdelivr.net/gh/{GITHUB_OWNER}/{GITHUB_REPO}@{GITHUB_BRANCH}{path}'),
]
```

**README 订阅链接配置**（`generate_readme()` 函数，无参数，使用 latest 固定路径）：

```python
# 格式: (显示名, 文件路径, 格式说明, GitHub项目链接)
# 全部使用 latest 固定路径，URL 永不改变，内容随每次运行自动更新
# 有链接 → ### [Clash](https://github.com/...)
# 无链接 → ### Surge
sub_files = [
    ('Clash', 'output/clash/latest.yaml', 'Clash / Clash Meta / Mihomo',
     'https://github.com/clash-verge-rev/clash-verge-rev'),
    ('V2Ray', 'output/v2ray/latest.txt', 'V2RayN / V2RayNG / Shadowrocket (Base64)',
     'https://github.com/2dust/v2rayN'),
    ('Surge', 'output/surge/latest.conf', 'Surge 4+',
     None),
    ('Mixed', 'output/mixed/latest.txt', '混合格式 Base64 (全协议)',
     None),
    # v1.6.0 新增 Sing-box 输出
    ('Sing-box', 'output/singbox/latest.json', 'Sing-box / SagerNet / Hiddify (JSON)',
     'https://github.com/SagerNet/sing-box'),
    ('原始 YAML', 'sub/latest.yaml', '向后兼容格式 (含分类)',
     None),
]
```

**latest 固定链接机制**（`generate_multi_format()` / `main()` 第 8.5 步）：

- 日期戳文件照常生成（如 `output/clash/8-17.yaml`），用于历史归档
- 每次转换成功后，`shutil.copy2()` 复制一份到同目录 `latest.{ext}`（如 `output/clash/latest.yaml`）
- 原始 YAML 同样复制到 `sub/latest.yaml`
- `output/index.json` 新增 `latest` 字段，记录各格式的固定路径
- **回退保护**（v1.3.2）：若某格式连续重试后仍转换失败，且该格式 latest 文件不存在，则按**文件名中的日期**（而非 mtime，CI 检出会重置 mtime）选取最近的历史文件复制为 latest，保证固定链接永不 404

**subconverter API 调用参数**（`call_subconverter()` 函数，第 456-466 行）：

```
{SUBCONVERTER_URL}/sub?
  target={target}           # clash / v2ray / surge&ver=4 / mixed
  url={encoded_url}         # 多个订阅URL用 | 分隔后 URL-encode
  config={encoded_config}   # file:// 绝对路径指向 external_config.ini
  emoji=true                # 添加 Emoji
  udp=true                  # 启用 UDP
  tfo=false                 # 禁用 TFO
  expand=true               # 展开订阅
  append_info=true          # 附加信息
  sort=false                # 不排序
```

**注意事项**：
- 环境变量 `GITHUB_OWNER` / `GITHUB_REPO` / `GITHUB_BRANCH` / `SUBCONVERTER_URL` 可覆盖默认值
- `generate_readme()` 由 Actions 运行时自动调用，不应被 deploy.sh 覆盖
- SSRF 防护：`is_private_ip()` 过滤内网 IP 地址

---

### 4.2 config.yaml — Telegram 频道配置

| 属性 | 值 |
| :--- | :--- |
| 文件类型 | YAML |
| 仓库路径 | `config.yaml` |
| 是否部署 | 是 |

**结构**：

```yaml
tgchannel:
  - https://t.me/univstar
  - https://t.me/iosfulishare
  - https://t.me/hkaa0
  # ... 共 34+ 个频道
```

**维护说明**：
- 每行一个 Telegram 频道 URL，格式 `https://t.me/频道名`
- 可添加 `#` 注释说明频道类型
- 新增频道直接在列表末尾追加即可
- 部分频道仅分享节点信息而非订阅源，抓取效果有限（已在注释中标注）

---

### 4.3 airports.yaml — 机场域名列表

| 属性 | 值 |
| :--- |
| 文件类型 | YAML |
| 仓库路径 | `airports.yaml` |
| 是否部署 | 是 |
| 数据来源 | [jichangnodes](https://github.com/moneyfly1/jichangnodes) |

**结构**：

```yaml
airports:
  - domain: example-airport.com   # 机场域名 (不含 https://)
    clash: true                   # 已知支持 Clash (可选)
    note: 备注                    # 说明 (可选)
    enabled: false                # 暂时跳过 (可选，默认 true)
```

**维护说明**：
- `domain` 不含协议前缀，脚本自动加 `https://`
- `enabled: false` 可临时禁用某个机场而不用删除条目
- 共 50+ 条目，脚本会探测每个域名页面中的订阅/节点链接

---

### 4.4 requirements.txt — Python 依赖

| 属性 | 值 |
| :--- |
| 文件类型 | TXT |
| 仓库路径 | `requirements.txt` |
| 是否部署 | 是 |

**依赖列表**：

| 包名 | 最低版本 | 用途 |
| :--- | :--- | :--- |
| requests | >=2.28.1 | HTTP 请求 + Session 连接池 |
| PyYAML | >=5.3.1 | YAML 读写 |
| tqdm | >=4.64.0 | 进度条 |
| retry | >=0.9.2 | 网络请求自动重试 |
| loguru | >=0.6.0 | 结构化日志 |

---

### 4.5 .gitignore — Git 忽略规则

| 属性 | 值 |
| :--- |
| 文件类型 | GIT |
| 仓库路径 | `.gitignore` |
| 是否部署 | 是 |

**忽略内容**：

```gitignore
__pycache__/               # Python 缓存
*.pyc / *.pyo              # 编译文件
*.egg-info/                # 包信息
dist/ build/ .eggs/        # 构建目录
*.log                      # 日志文件
.env                       # 环境变量文件
subconverter_linux64.tar.gz   # subconverter 下载包 (Linux)
subconverter_bin/             # subconverter 解压目录
subconverter_windows64.7z     # subconverter 下载包 (Windows)
trial.cache                   # 缓存文件
archive/                      # 归档目录 (一次性脚本)
deploy.sh                     # 部署脚本 (仅本地)
```

**注意事项**：
- `archive/` 和 `deploy.sh` 是本地文件，不应上传 GitHub
- `subconverter_bin/` 在本地运行时会自动生成
- 如需添加新的本地文件，在此文件末尾追加即可

---

### 4.6 fetch.yaml — GitHub Actions 工作流

| 属性 | 值 |
| :--- |
| 文件类型 | YAML |
| 仓库路径 | `.github/workflows/fetch.yaml` |
| 是否部署 | 是 |

**触发方式**：

| 触发器 | 条件 | 说明 |
| :--- | :--- | :--- |
| `schedule` | `cron: '0 */4 * * *'` | 每 4 小时自动运行 (UTC) |
| `workflow_dispatch` | 手动触发 | 通过 Actions 页面或 `gh workflow run` |
| `watch` | `types: started` | 用户 Star 仓库时触发 |

**执行步骤**（共 7 步）：

| 步骤 | 名称 | 说明 |
| :--- | :--- | :--- |
| 1 | Checkout | 检出代码，`persist-credentials: true` |
| 2 | Setup Python | 安装 Python 3.x |
| 3 | Cache pip | 缓存 pip 依赖加速 |
| 4 | Set timezone | 设置时区为 `Asia/Shanghai` |
| 5 | Install dependencies | `pip install -r requirements.txt` |
| 6 | Download and start subconverter | 下载 subconverter → 解压 → 后台启动 → 验证 |
| 7 | Run collector | `python ./main.py` |
| 8 | Stop subconverter | `pkill -f subconverter`（`if: always()` 始终执行） |
| 9 | Commit and Push | `git add sub/ output/ README.md` → commit → push |

**Commit 信息格式**：
- Actions 自动提交：`Update YYYY-MM-DD HH:MM:SS`（上海时区）
- Commit 作者：`GitHub Actions <actions@github.com>`

**注意事项**：
- `permissions: contents: write` 必须设置，否则 push 会失败
- subconverter 在步骤 6 下载到 `subconverter_bin/`，已 gitignored
- 步骤 8 用 `if: always()` 确保即使 main.py 失败也会清理 subconverter
- 两个 Actions 同时运行可能导致 git push 竞态冲突（非代码问题）

---

### 4.7 external_config.ini — subconverter 配置

| 属性 | 值 |
| :--- |
| 文件类型 | INI |
| 仓库路径 | `subconverter/external_config.ini` |
| 是否部署 | 是 |
| 被调用方 | `main.py` → `call_subconverter()` |

**配置内容**：

**代理组**（`custom_proxy_group`）：

| 组名 | 类型 | 策略 |
| :--- | :--- | :--- |
| 🚀 节点选择 | select | 自动选择 / 各地区节点 / DIRECT |
| ♻️ 自动选择 | url-test | 全部节点，测速间隔 300s |
| 🇭🇰 香港节点 | url-test | 正则 `(香港\|HK\|Hong Kong)` |
| 🇹🇼 台湾节点 | url-test | 正则 `(台湾\|TW\|Taiwan)` |
| 🇯🇵 日本节点 | url-test | 正则 `(日本\|JP\|Japan)` |
| 🇸🇬 新加坡节点 | url-test | 正则 `(新加坡\|SG\|Singapore)` |
| 🇺🇸 美国节点 | url-test | 正则 `(美国\|US\|United States)` |
| 🔗 故障转移 | fallback | 全部节点 |
| ⚖️ 负载均衡 | load-balance | 全部节点 |

**分流规则**（ACL4SSR 规则集）：

| 规则组 | 策略 | 规则来源 |
| :--- | :--- | :--- |
| 🛑 广告拦截 | 阻断 | BanAD + BanEasyList |
| 🛑 隐私防护 | 阻断 | BanEasyPrivacy |
| 🎯 全球直连 | 直连 | LocalAreaNetwork + ChinaDomain + ChinaIp |
| 🚀 国外加速 | 代理 | ProxyLite |
| 📲 电报消息 | 代理 | Telegram |
| 🍎 苹果服务 | 代理 | Apple |
| 🪚 微软服务 | 代理 | Microsoft |
| 🌍 谷歌服务 | 代理 | Google |
| 🎬 国外媒体 | 代理 | ProxyMedia |
| 🎬 国内媒体 | 直连 | ChinaMedia |
| 🐟 漏网之鱼 | FINAL | 兜底规则 |

**节点过滤**：
```
exclude_remarks=(?i)(到期|剩余流量|官网|产品|平台|流量|时间|Expire|Traffic|Website)
```

**Emoji 映射**：香港🇭🇰 / 台湾🇹🇼 / 日本🇯🇵 / 新加坡🇸🇬 / 美国🇺🇸 / 韩国🇰🇷 / 英国🇬🇧 / 德国🇩🇪

**节点重命名**：移除重复倍率信息 `(?倍率)x → 倍率x`

---

### 4.8 update.sh — 一键更新脚本

| 属性 | 值 |
| :--- |
| 文件类型 | Bash |
| 仓库路径 | `update.sh` |
| 是否部署 | 是 |
| 运行环境 | Linux / macOS / Git Bash (Windows) |

**5 种模式**：

| 命令 | 说明 | 需要认证 | 适用场景 |
| :--- | :--- | :--- | :--- |
| `bash update.sh remote` | 远程触发 Actions 并实时监控 | 是 | 立即更新（默认） |
| `bash update.sh local` | 本地运行完整流水线 | 否 | Actions 不可用或调试 |
| `bash update.sh status` | 查看最近 5 次运行状态 | 否 | 检查 Actions 是否正常 |
| `bash update.sh pull` | 从 GitHub 拉取最新订阅文件 | 否 | 同步远程结果到本地 |
| `bash update.sh links` | 显示最新订阅链接 | 否 | 快速查看订阅地址 |

**认证方式**（仅 `remote` 模式需要）：

| 优先级 | 方式 | 说明 |
| :--- | :--- | :--- |
| 1 | gh CLI 已登录 | `gh auth login`（推荐） |
| 2 | 环境变量 `GITHUB_TOKEN` | `export GITHUB_TOKEN=ghp_xxx` |
| 3 | 脚本内 `TOKEN` 变量 | 编辑 `update.sh` 第 37 行 |

> `status`、`pull`、`links` 读取公开仓库数据，匿名访问无需认证。

**local 模式执行流程**（7 步）：
1. 检查 Python 环境
2. 检查/安装 Python 依赖
3. 检查/下载 subconverter（自动判断 OS：Windows/Linux/macOS）
4. 启动 subconverter
5. 运行 `main.py`
6. 停止 subconverter
7. 显示结果 + 询问是否推送

**关键配置**（第 29-37 行）：

```bash
GITHUB_OWNER="huiwin"
GITHUB_REPO="NodeCollection"
GITHUB_BRANCH="main"
TOKEN="${GITHUB_TOKEN:-}"   # 留空则尝试 gh CLI
```

---

### 4.9 update.bat — Windows 入口

| 属性 | 值 |
| :--- |
| 文件类型 | Batch |
| 仓库路径 | `update.bat` |
| 是否部署 | 是 |
| 运行环境 | Windows (CMD) |

**功能**：双击运行弹出菜单，选择 1-5 对应 `update.sh` 的 5 种模式，内部调用 `bash update.sh <mode>`。

**菜单选项**：

```
[1] 远程触发更新 (推荐)  - 通过 GitHub API 触发 Actions
[2] 本地运行            - 在本机运行完整流水线
[3] 查看运行状态        - 查看最近 Actions 运行记录
[4] 拉取最新结果        - 从 GitHub 同步订阅文件
[5] 显示订阅链接        - 查看最新订阅地址
[0] 退出
```

**注意事项**：
- 需要 Git Bash 环境（Windows 安装 Git 后自带）
- `chcp 65001` 设置 UTF-8 编码，确保中文正常显示

---

### 4.10 deploy.sh — 部署脚本

| 属性 | 值 |
| :--- |
| 文件类型 | Bash |
| 本地路径 | `D:\Temp\nodecollection-pro\deploy.sh` |
| 是否部署 | **否**（已 gitignored，仅本地使用） |
| 当前版本 | v1.4.2 |

**功能**：将本地源文件部署到 GitHub 仓库，5 步流程：

| 步骤 | 说明 |
| :--- | :--- |
| 1/5 | 克隆现有仓库到临时目录（保留 sub/ 历史数据） |
| 2/5 | 复制 FILES_TO_COPY 列表中的 12 个文件 (v1.4.0 新增 merge.yaml) |
| 3/5 | 清理残留文件（pre_check.py, push.sh 等） |
| 4/5 | 显示变更概览，等待用户确认 |
| 5/5 | 提交并推送到 GitHub，清理临时目录 |

**FILES_TO_COPY（部署文件清单，共 12 个）**：

```
main.py
config.yaml
airports.yaml
requirements.txt
.gitignore
ABOUT.md
update.sh
update.bat
output/.gitkeep
.github/workflows/fetch.yaml
subconverter/external_config.ini
```

> **README.md 不在列表中**：README 由 Actions 运行时 `main.py` 的 `generate_readme()` 自动生成。deploy.sh 部署本地陈旧 README 会覆盖 Actions 刚生成的新版本。

**FILES_TO_REMOVE（残留文件清理列表）**：

```
pre_check.py, push.sh, integration-plan.html,
deployment-guide.html, gen_configs.py, gen_html.py
```

**注意事项**：
- `set -e` 模式下 `((var++))` 从 0 自增时退出码为 1 会被捕获中断，已修复为 `removed_count=$((removed_count + 1))`
- 部署完成后需手动触发 Actions 重新生成 README：`bash update.sh remote`
- commit message 当前为 `Upgrade to v1.4.2`（含上游融合变更说明与 v1.4.1/v1.4.2 修复要点），部署脚本版本号已同步

---

### 4.11 clean_and_normalize.sh — Git 历史清理

| 属性 | 值 |
| :--- |
| 文件类型 | Bash |
| 本地路径 | `D:\Temp\nodecollection-pro\clean_and_normalize.sh` |
| 是否部署 | **否**（已 gitignored，仅本地使用） |
| 状态 | 已执行（2026-08-13） |

**功能**：一次性合并邮箱清除 + 版本号规范化，单次 `git filter-branch` + 单次 force push。

**两个 Filter**：

| Filter | 作用 | 处理对象 |
| :--- | :--- | :--- |
| `--env-filter` | 替换泄露的邮箱/姓名 | author/committer 的 name + email |
| `--msg-filter` | 规范化版本号 | commit message (subject + body) |

**邮箱替换映射**：

```
旧: 郑辉T016547 <T016547@citicso.com>
新: huiwin <113569136+huiwin@users.noreply.github.com>
```

**版本号替换映射**（sed，`.` 已转义为 `\.`）：

```
NodeCollection v2.0 stable           → Initial release v1.0.0
Upgrade to NodeCollection Pro v3.0   → Upgrade to v1.1.0
Upgrade to NodeCollection Pro v3.1   → Upgrade to v1.2.0
Upgrade to NodeCollection Pro v3.2   → Upgrade to v1.3.0
Restructure project to v3.2          → Upgrade to v1.3.0
```

> GitHub Actions 自动 commit（`Update ...`、`🍀 爬取订阅源 ...`）不含版本号，保持不变。

**执行流程**（5 步）：
1. `git clone --mirror` 克隆完整历史
2. `git filter-branch` 同时执行两个 filter
3. 验证结果（检查邮箱泄露 + 旧版本号残留）
4. 二次确认后 `git push --force --mirror`
5. 清理临时目录

**注意事项**：
- 会改变所有 commit hash，操作不可逆
- `--mirror` push 会同时推送 `refs/original/` 备份引用，需手动删除：
  ```bash
  MSYS_NO_PATHCONV=1 gh api --method DELETE /repos/huiwin/NodeCollection/git/refs/original/refs/heads/main
  ```
  > Git Bash 的 MSYS2 会将以 `/` 开头的参数当作文件路径，需 `MSYS_NO_PATHCONV=1` 禁用。GitHub API 的 `{ref}` 参数是去掉 `refs/` 前缀后的部分。
- 本地非 Git 仓库目录（如 `D:\Temp\nodecollection-pro\`）不需要 `git fetch` / `git reset`

---

### 4.12 ABOUT.md — 项目说明

| 属性 | 值 |
| :--- |
| 文件类型 | Markdown |
| 仓库路径 | `ABOUT.md` |
| 是否部署 | 是 |

**内容概要**：项目特色、目录结构、技术栈、架构图、订阅格式说明、加速代理说明、一键更新脚本用法、部署方式、Credits。

**维护说明**：作为仓库的自述文档，与本文档（reference-docs.md）互补。ABOUT.md 面向仓库访客，本文档面向项目维护者。

---

### 4.13 README.md — 订阅展示页（自动生成）

| 属性 | 值 |
| :--- |
| 文件类型 | Markdown |
| 仓库路径 | `README.md` |
| 是否部署 | **否**（Actions 自动生成，不在 FILES_TO_COPY 中） |
| 生成函数 | `main.py` → `generate_readme()` |

**内容结构**：

```
# NodeCollection
> 自动更新时间: YYYY-MM-DD HH:MM:SS
## 订阅链接
---
### [Clash](https://github.com/clash-verge-rev/clash-verge-rev)     ← 超链接
### [V2Ray](https://github.com/2dust/v2rayN)                        ← 超链接
### Surge                                                          ← 纯文本
### Mixed                                                          ← 纯文本
### 原始 YAML                                                      ← 纯文本
```

每个格式下包含 6 行加速代理表格：原生 / kkgithub / ghproxy.net / gh-proxy.com / ghfast.top / jsdelivr。

**注意事项**：
- 每次 Actions 运行或本地 `main.py` 运行时自动覆盖
- 订阅链接全部使用 latest 固定路径（如 `output/clash/latest.yaml`），URL 永不变，内容随每次运行自动更新
- deploy.sh 不部署 README.md，避免本地旧版本覆盖远程新版本
- 如需修改 README 格式，编辑 `main.py` 的 `generate_readme()` 函数

---

### 4.14 merge.yaml — 上游订阅融合白名单（v1.4.0 新增）

| 属性 | 值 |
| :--- |
| 文件类型 | YAML |
| 仓库路径 | `merge.yaml` |
| 是否部署 | 是（FILES_TO_COPY 第 3 项） |
| 生效阶段 | main() 第 9.5 步 |

**功能**：声明融合的外部订阅来源（白名单制）。运行时按此拉取上游订阅 → 解析（Base64 / 明文链接 / Clash YAML 自动识别）→ 安检（SSRF 校验 + 内网剔除 + 协议白名单）→ 节点名加 `[ext:来源]` 前缀 → 独立输出到 `output/merged/`，**不混入主订阅**。

**配置结构**：

```yaml
upstreams:
  - name: freenode            # 来源标识，即节点前缀 [ext:freenode]
    url: https://...           # 上游订阅地址
    repo: owner/repo           # GitHub 仓库路径 (v1.6.0 新增, 可选, 用于 README 来源表致谢)
    enabled: true              # false 可临时停用
    max_nodes: 80              # 该来源节点数上限，0 不限制
```

**当前白名单（4 个，v1.8.0 更新）**：

| name | 上游项目 | 格式 | 说明 |
| :--- | :--- | :--- | :--- |
| NoMoreWalls | peasoft/NoMoreWalls | 明文分享链接 | 长期活跃 |
| FreeNodes | Barabama/FreeNodes | 明文分享链接 (vless) | feat/ai-crawler-v2 分支 |
| Pawdroid | Pawdroid/Free-servers | Base64 (vmess) | v1.8.0 新增, 6小时自动更新 |
| Jsnzkpg | Jsnzkpg/Jsnzkpg | Base64 (多协议) | v1.8.0 新增, 外部域名 sub.445569.xyz, 含 vless/hysteria2/vmess/ss/trojan |

> v1.8.0 移除 freenode (可用率仅 7.1%, 14节点仅1可达)，新增 Pawdroid 和 Jsnzkpg 两个高质量上游。

**关键函数**（main.py）：

| 函数 | 职责 |
| :--- | :--- |
| `load_upstreams()` | 加载白名单，缺失/为空返回空列表不报错 |
| `fetch_upstream()` / `fetch_all_upstreams()` | 拉取上游：60s 超时 + 3 次重试 + 失败隔离 |
| `parse_upstream_text()` | 三格式解析 + `[ext:]` 前缀 + max_nodes 截断 |
| `rename_uri_node()` | vmess ps 字段 / 其他协议 #fragment 名称改写 |
| `filter_clash_proxies()` | Clash 代理类型白名单 + 内网地址剔除 |
| `_serve_local_files()` | 本地随机端口 HTTP 服务，向 subconverter 提供解析后节点（安检前置，不直连上游 URL） |
| `generate_merged_format()` | 融合主入口：去重 → 本地文件 → 转换 → `output/merged/latest.{format}.{ext}`（含回退保护）→ index.json merged 段 |
| `_upstream_fetch_candidates()` | 构造上游拉取候选 URL：主 URL + 3 个镜像前缀 (ghfast.top / gh-proxy.com / ghproxy.net, 仅 raw.githubusercontent.com) |
| `_fetch_upstream_once()` | 单次拉取单个 URL (供主 URL 重试与镜像回退共用) |

**使用规则**：
- 仅添加明确公开免费、长期活跃的来源；**绝不添加付费机场订阅**
- v1.6.0 起 README 来源表自动同步：优先使用 merge.yaml 的 `repo` 字段，无 repo 时从 raw.githubusercontent.com URL 自动提取仓库路径，最后回退到 `UPSTREAM_REPO_MAP`；增删上游无需手改 README
- 上游节点协议支持：ss / ssr / vmess / vless / trojan / hysteria / hysteria2 / tuic

---

## 5. 关键参数速查表

### main.py 配置常量

| 参数 | 默认值 | 环境变量覆盖 | 说明 |
| :--- | :--- | :--- | :--- |
| `MAX_THREADS` | 32 | — | 校验线程数 |
| `CHANNEL_THREADS` | 8 | — | 频道爬取线程数 |
| `AIRPORT_THREADS` | 8 | — | 机场探测线程数 |
| `REQUEST_TIMEOUT` | 10 | — | 通用请求超时 (秒) |
| `CHANNEL_TIMEOUT` | 15 | — | 频道爬取超时 (秒) |
| `AIRPORT_TIMEOUT` | 8 | — | 机场探测超时 (秒) |
| `RETRY_TIMES` | 2 | — | 重试次数 |
| `SUBCONVERTER_URL` | `http://127.0.0.1:25500` | `SUBCONVERTER_URL` | subconverter API 地址 |
| `SUBCONVERTER_TIMEOUT` | 60 | — | subconverter 单次请求超时 (秒) |
| `SUBCONVERTER_RETRIES` | 3 | — | subconverter 转换重试次数 |
| `SUBCONVERTER_RETRY_DELAY` | 5 | — | 重试间隔 (秒) |
| `UPSTREAM_THREADS` | 8 | — | 上游订阅拉取并发线程 (v1.4.0) |
| `UPSTREAM_TIMEOUT` | 60 | — | 上游拉取超时 (秒) |
| `UPSTREAM_RETRIES` | 3 | — | 上游拉取重试次数 |
| `UPSTREAM_RETRY_DELAY` | 5 | — | 上游拉取重试间隔 (秒) |
| merge.yaml `max_nodes` | 80 | merge.yaml | 单上游节点数上限 (0 不限制) |
| `MERGED_MAX_NODES` | 200 | — | 融合输出每格式总量上限 (v1.5.0) |
| `LATENCY_FAIL_THRESHOLD` | 3 | — | 连续 N 次不可达则剔除 (v1.7.0 从 2 增至 3, 对冲超时缩短) |
| `LATENCY_TIMEOUT` | 4 | — | 单次测速 TCP 连接超时秒数 (v1.7.0 从 5 降至 4) |
| `LATENCY_THREADS` | 48 | — | 测速并发线程数 (v1.7.0 从 32 增至 48) |
| `COOLDOWN_ENTRY_THRESHOLD` | 2 | — | 连续失败 N 次进入冷却期 (v1.7.0 新增) |
| `COOLDOWN_RETRY_INTERVAL` | 3 | — | 冷却期内每 N 周期强制重试一次 (v1.7.0 新增) |
| `COOLDOWN_MAX_RETRIES` | 3 | — | 冷却期内最多重试 N 次, 超过则剔除 (v1.7.0 新增) |
| `HISTORY_KEEP_COUNT` | 5 | — | 每格式保留最近 N 版日期文件 (v1.6.0) |
| `UPSTREAM_DEGRADE_THRESHOLD` | 3 | — | 连续 N 次拉取失败标记 degraded (v1.6.0) |
| `GITHUB_OWNER` | `huiwin` | `GITHUB_OWNER` | GitHub 仓库所有者 |
| `GITHUB_REPO` | `NodeCollection` | `GITHUB_REPO` | GitHub 仓库名 |
| `GITHUB_BRANCH` | `main` | `GITHUB_BRANCH` | GitHub 分支名 |
| `USER_AGENT` | `Clashforwindows/0.18.1` | — | 请求 UA |

### fetch.yaml 关键配置

| 参数 | 值 | 说明 |
| :--- | :--- | :--- |
| cron | `0 */4 * * *` | 每 4 小时 (UTC) |
| 时区 | `Asia/Shanghai` | commit 时间用上海时区 |
| Python 版本 | `3.x` | 最新 Python 3 |
| commit 作者 | `GitHub Actions <actions@github.com>` | 自动提交身份 |
| commit message | `Update YYYY-MM-DD HH:MM:SS` | 上海时区时间 |

### deploy.sh 文件清单

| 类别 | 文件 | 说明 |
| :--- | :--- | :--- |
| 部署 | 11 个文件 | 见 [4.10](#410-deploysh--部署脚本) |
| 不部署 | `README.md` | Actions 自动生成 |
| 不部署 | `deploy.sh` / `clean_and_normalize.sh` | 本地工具 |
| 清理 | 6 个残留文件 | pre_check.py, push.sh 等 |

---

## 6. 操作指南

### 6.1 日常更新

**最常用操作**——立即触发一次订阅采集：

```bash
# 方式一：命令行（推荐）
bash update.sh remote
# 或直接用 gh CLI
gh workflow run fetch.yaml --repo huiwin/NodeCollection

# 方式二：Windows 双击 update.bat → 选 [1]

# 方式三：GitHub 网页 → Actions → "NodeCollection" → Run workflow
```

触发后约 2-3 分钟完成，README 和订阅文件自动更新。

### 6.2 部署代码变更

修改了 `main.py`、`config.yaml` 等文件后推送到 GitHub：

```bash
cd /d/Temp/nodecollection-pro
bash deploy.sh
# → 确认变更概览 → 输入 y 推送
# → 部署完成后触发 Actions 重新生成 README：
bash update.sh remote
```

### 6.3 本地调试

不依赖 GitHub Actions，在本机运行完整流水线：

```bash
bash update.sh local
# 脚本自动：检查 Python → 安装依赖 → 下载 subconverter → 运行 main.py
```

### 6.4 Git 历史清理

> 仅在泄露隐私或需要规范化历史时使用，操作不可逆。

```bash
cd /d/Temp/nodecollection-pro
bash clean_and_normalize.sh
# → 输入 yes 确认重写 → 检查结果 → 输入 yes 确认 force push

# 清理 refs/original 备份引用：
MSYS_NO_PATHCONV=1 gh api --method DELETE /repos/huiwin/NodeCollection/git/refs/original/refs/heads/main
```

---

## 7. 常见问题排查

| 问题 | 原因 | 解决方案 |
| :--- | :--- | :--- |
| deploy.sh 输出 "nothing to commit" | 文件已全部同步，无新变更 | 正常现象，无需处理 |
| README.md 不更新 | deploy.sh 覆盖了 Actions 生成的版本 | 已从 FILES_TO_COPY 移除 README.md，触发 Actions 即可 |
| deploy.sh 中断在清理步骤 | `set -e` + `((var++))` 冲突 | 已修复为 `removed_count=$((removed_count + 1))` |
| Actions push 失败 | 两个 Actions 同时运行导致 git 竞态 | 非代码问题，下次定时运行会正常 |
| `gh api` 路径错误 `C:/Program Files/Git/repos/...` | Git Bash MSYS2 路径转换 | 加 `MSYS_NO_PATHCONV=1` 或去掉开头 `/` |
| `gh api DELETE` 返回 422 "Reference does not exist" | ref 路径多了 `refs/` 前缀 | API 的 `{ref}` 参数去掉 `refs/` 前缀 |
| `git fetch` 报 "not a git repository" | 源文件目录不是 Git 仓库 | 正常现象，deploy.sh 通过临时 clone 推送 |
| raw.githubusercontent.com 访问慢 | 国内直连 GitHub 慢 | 使用加速代理：kkgithub / ghproxy.net / ghproxy.com / ghfast.top |
| GitHub 文件列表显示旧版本号 | commit message 含旧版本号 | 运行 `clean_and_normalize.sh` 重写历史 |
| 某格式 latest 文件缺失 / 固定链接 404 | 该格式 subconverter 转换失败（超时等），旧版本无重试无回退 | v1.3.2 已修复：60s 超时 + 3 次重试 + 历史文件回退；存量缺失可重新触发 Actions 生成 |
| README 链接指向的文件访问 404 | 转换失败导致文件未生成 | 同上，触发 `bash update.sh remote` 重新运行 |
| 某个上游订阅拉取失败 | 上游仓库失效 / 改格式 / 反爬 | 失败隔离机制自动跳过 (仅告警)；连续失败可在 merge.yaml 置 `enabled: false` 或移除 |
| output/merged 无输出 | 全部上游失败或解析后无有效节点 / subconverter 不可用 | 查看日志中 `[ext:xxx]` 告警；上游节点问题不影响主订阅 |
| 融合节点名无 [ext:] 前缀 | vmess ps / fragment 改写失败的边缘格式 | 少量节点可接受；如大量出现检查 parse_upstream_text 日志 |

---

## 8. Credits

| 项目 | 链接 | 贡献 |
| :--- | :--- | :--- |
| 原始项目 | [huiwin/collectSub-google](https://github.com/huiwin/collectSub-google) | 基础采集逻辑 |
| Fork 来源 | [RenaLio/proxy-minging](https://github.com/RenaLio/proxy-minging) | 多源采集改进 |
| 格式转换 | [tindy2013/subconverter](https://github.com/tindy2013/subconverter) | subconverter 引擎 |
| 机场列表 | [moneyfly1/jichangnodes](https://github.com/moneyfly1/jichangnodes) | 机场域名数据 |
| 分流规则 | [ACL4SSR](https://github.com/ACL4SSR/ACL4SSR) | Clash 规则集 |
| 上游订阅 | [peasoft/NoMoreWalls](https://github.com/peasoft/NoMoreWalls) | 融合来源 (v1.4.0) |
| 上游订阅 | [Barabama/FreeNodes](https://github.com/Barabama/FreeNodes) | 融合来源 (v1.4.0) |
| 上游订阅 | [Pawdroid/Free-servers](https://github.com/Pawdroid/Free-servers) | 融合来源 (v1.8.0 新增) |
| 上游订阅 | [Jsnzkpg/Jsnzkpg](https://github.com/Jsnzkpg/Jsnzkpg) | 融合来源 (v1.8.0 新增, 订阅地址 sub.445569.xyz) |

---

## 9. 更新记录

| 日期 | 变更摘要 |
| :--- | :--- |
| 2026-08-13 | 初始创建：整合所有参照文档，覆盖 v1.3.1 全部文件和配置 |
| 2026-08-17 | 补记 v1.3.1+ 订阅链接固定化 (latest 机制)；升级至 v1.3.2：subconverter 超时 30s→60s、新增 3 次重试、latest 回退保护（按文件名日期选历史文件，规避 CI mtime 重置）；同步更新第 2/4/5/7 节 |
| 2026-08-18 | 升级至 v1.4.2：修复 P0 A4 缺陷——generate_merged_format 误传 'merged/<格式>' 为 subconverter target 致融合文件未发布 (404)，改为仅传格式名；版本号与文档同步至 v1.4.2 |
| 2026-08-18 | P1 启动：完成 P0 环境完整备份 (backup/p0-v1.4.2-20260818 + tar.gz + restore.sh + MD5 校验)；实施 v1.5.0 T1.1 延迟测速器 (43 项验收测试全部通过)；制定 p1-implementation-plan.md |
| 2026-08-19 | 实施 v1.5.0 T1.2 排序与剔除 (31 项验收测试通过)：generate_merged_format 流程扩展为 解析→去重→测速→健康更新→剔除→排序→转换；新增 filter_and_sort_nodes (延迟升序 + 连续2次不可达剔除) 与 load_node_health；健康记录嵌入 index.json merged.quality.node_health 跨周期持久化；generate_merged_format 返回值改为过滤后实际节点数 |
| 2026-08-19 | 实施 v1.5.0 T1.4 截断升级 (20 项验收测试通过，T1.2 无回归 31/31)：删除 parse_upstream_text 简单 max_nodes 截断，改在 generate_merged_format 测速排序后做两层截断——①单源截断：按 uri_source_map/proxy_source_map 分组，每源取前 max_nodes (延迟最低)；②总量截断：URI+Clash proxy 合并按延迟升序取前 MERGED_MAX_NODES(200)；单源截断后重新全局排序保证输出有序；available_count 与 quality.total_available 反映截断后数量；新增 archive/test_v150_truncation.py |
| 2026-08-19 | 实施 v1.5.0 T1.5 质量指标日志 (32 项验收测试通过，T1.1 43/43、T1.2 31/31、T1.4 24/24 无回归)：quality 字段扩展为 total_parsed/total_available(剔除后存活)/excluded/truncated/output_count/availability_rate/avg_latency_ms/max_latency_ms/min_latency_ms/node_health；延迟统计基于剔除后可达节点；main() 运行统计新增「融合质量」行输出可用率与平均延迟；新增 archive/test_v150_quality.py |
| 2026-08-19 | 实施 v1.5.0 T1.3 地区分组 (39 项验收测试通过，P1 全部完成)：新增 subconverter/merged_config.ini (7 地区分组 HK/TW/JP/SG/US/KR/UK，主选择组同步引用)；call_subconverter 增加可选 config_path 参数 (默认 None→SUBCONVERTER_EXTERNAL_CONFIG)；generate_merged_format 调用时传入 SUBCONVERTER_MERGED_CONFIG；generate_readme 融合订阅段落新增「节点分组」说明表；主订阅 generate_multi_format 保持默认配置不变；新增 archive/test_v150_region_group.py |
| 2026-08-19 | P1 线上验证：部署 v1.5.0 后发现 merge.yaml 两个上游分支名配置错误 (NoMoreWalls 应为 master 非 main，FreeNodes 应为 feat/ai-crawler-v2 非 main)，修正后三源全部拉取成功，解析 286 节点输出 160，平均延迟 210ms，P1 质量控制功能线上验证通过 |
| 2026-08-19 | 实施 v1.6.0 P2 体验增强 (全部完成，57 项验收测试通过，全量回归 226/226)：T2.1 Sing-box 输出 (OUTPUT_FORMATS 新增 singbox target，主订阅+融合订阅均支持，含 latest 回退，README 新增链接)；T2.2 历史版本保留 (cleanup_old_versions 函数，每格式保留最近 5 版自动清理旧版本)；T2.3 上游监控与降级标记 (load_upstream_health，连续 3 次失败标记 degraded，README 来源表新增状态列)；T2.4 Credits 自动同步 (merge.yaml 新增 repo 字段，优先级 repo>URL提取>UPSTREAM_REPO_MAP 回退)；新增 archive/test_v160_p2.py |
| 2026-08-20 | 实施 v1.7.0 P3 性能优化与可观测性增强 (全部完成，149 项验收测试通过，全量回归 288/288)：T3.1 性能优化组合 (LATENCY_TIMEOUT 5→4s、LATENCY_THREADS 32→48、LATENCY_FAIL_THRESHOLD 2→3；新增冷却期跳过测速机制，连续 2 次失败进入冷却期，每 3 周期强制重试，最多 3 次重试失败剔除，重试成功退出冷却期；measure_uri_latencies/measure_proxy_latencies 增加 health_data 参数；filter_and_sort_nodes 重写冷却期逻辑；健康记录扩展 cooling_down/cooldown_cycles/retry_count 字段)；T3.2 智能去重 (dedup_uris_by_host_port/dedup_proxies_by_host_port，基于 host:port 去重替代完整 URI 去重，无法提取时回退)；T3.3 协议过滤 (PROTOCOL_COMPATIBILITY 协议兼容性矩阵 + PROTOCOL_NAMES 中文映射，README 各格式新增支持协议标注)；T3.4 上游贡献统计 (upstream_stats 字段含 parsed/after_dedup/available/avg_latency/availability_rate，README 来源表扩展为 8 列)；T3.5 Web 状态页 (generate_status_page 生成 output/status.html，暗色主题概览卡片+质量指标+上游贡献表；fetch.yaml 新增 peaceiris/actions-gh-pages GitHub Pages 部署)；新增 archive/test_v170_t31~t35_*.py；新增 p3-iteration-plan.md |
| 2026-08-21 | 实施 v1.8.0 P4 上游扩展与资源优化 (全部完成)：T4.1 上游扩展——移除低质量上游 freenode (可用率仅 7.1%)，新增 Pawdroid/Free-servers (base64 vmess, raw.githubusercontent.com) 和 Jsnzkpg (外部域名 sub.445569.xyz, 多协议 vless/hysteria2/vmess/ss/trojan)，上游从 3 个变为 4 个，更新 UPSTREAM_REPO_MAP；T4.2 资源优化——清理 config.yaml 中 5 个无效频道 (v2list/freev2rays/vmess_tg/mftizi/ssrList，分享裸节点而非订阅链接)，频道从 42 个精简至 37 个；T4.3 subconverter 配置修复——修复融合订阅 Clash 报错 (config=file:// 在 Linux 不生效导致配置未加载、YAML 结构错误、节点丢失)，fetch.yaml 复制 external_config.ini 为 pref.ini，main.py 移除 config=file:// 参数，external_config.ini 新增韩英分组；T4.4 版本与文档更新——版本号 v1.7.0→v1.8.0，reference-docs.md 同步更新版本历史、上游白名单、Credits、更新记录；deploy.sh 更新版本号与 commit message |
| 2026-08-24 | 实施 v2.0.0 P6 subconverter 配置修复与地区分组恢复 (根本修复)：T6.1 subconverter 配置 section 名称修正——自 P4 以来融合订阅 Clash 一直缺少地区组/故障转移/负载均衡等自定义代理组，根本原因是 external_config.ini 错误使用 [custom] section，而 subconverter 标准 section 是 [common]（exclude_remarks/add_emoji/remove_old_emoji/rule/rename）、[rulesets]（enable_rule_generator/overwrite_original_rules/ruleset）、[proxy_groups]（custom_proxy_group）；重写 external_config.ini 将配置项分到正确的三个 section，所有 custom_proxy_group（11个地区组+故障转移+负载均衡+节点选择）正确加载；T6.2 融合订阅节点数预计从 58 恢复正常；版本号 v1.9.0→v2.0.0；reference-docs.md 和 deploy.sh 同步更新 |
| 2026-08-24 | 实施 v1.9.0 P5 质量优化与体验增强 (全部完成)：T5.1 无效节点过滤增强——新增 is_invalid_node_host() 函数，过滤 127.0.0.x/0.0.0.0/10.x/192.168.x/172.16-31.x 等本地保留地址节点，URI 节点在 parse_upstream_text 解析后测速前过滤；T5.2 融合订阅地区分组修复——external_config.ini 地区组正则增加 [代码] 前缀匹配，新增 🇩🇪德国/🇫🇷法国/🇷🇺俄罗斯地区组；T5.3 节点地区识别增强 (轻量方案)——新增 REGION_PATTERNS (19个地区) 和 detect_region() 函数，基于服务器名正则匹配识别地区，为无标识节点添加 [地区代码] 前缀，新增 _enhance_uris_region()/_enhance_clash_proxies_region() 辅助函数；T5.4 subconverter 过滤调优——调整 exclude_remarks 正则，移除 '流量''时间' 等宽泛词避免过度过滤，改为更精确的短语匹配；T5.5 Web 状态页增强——新增 Chart.js 上游贡献饼图和延迟分布柱状图，质量卡片从4个扩展到6个，质量指标表格新增说明列和总量截断指标，暗色渐变背景+毛玻璃卡片+hover动效+响应式布局；T5.6 告警通知预留接口——新增 ALERT_WEBHOOK_URL 配置项 (环境变量，默认空不启用) 和 send_alert() 函数，支持钉钉/飞书/Server酱/通用Webhook，自动识别Webhook类型并构造对应payload；版本号 v1.8.0→v1.9.0；reference-docs.md 和 deploy.sh 同步更新 |
| 2026-08-25 | 实施 v2.1.0 P7 代理组配置优化与 fetch.yaml 工作流稳定性增强：T7.1 代理组名称优化——融合订阅 Clash 中有两个同名的"🚀 节点选择"代理组（一个是 subconverter 自动生成的默认组，一个是 external_config.ini 中定义的引用地区组的组），将 external_config.ini 中的"🚀 节点选择"改名为"🎯 地区选择"，用户可以清楚区分"所有节点选择"和"按地区选择"两个组；T7.2 地区组 filter 正则增强——所有地区组 filter 增加 emoji（🇭🇰🇹🇼🇯🇵🇸🇬🇺🇸🇰🇷🇬🇧🇩🇪🇫🇷🇷🇺）和 [代码] 前缀匹配，提升地区分组覆盖率；T7.3 fetch.yaml 工作流稳定性增强（第一批4项）——优化1并发控制（添加 concurrency 配置避免同时运行导致 git push 冲突）、优化2启动轮询（subconverter 启动从 sleep 3 改为轮询等待端口就绪，最多15秒，启动失败直接报错退出）、优化3超时设置（给所有关键 step 添加 timeout-minutes：依赖安装5min/subconverter5min/采集15min/提交5min/Pages部署5min）、优化4输出验证（commit 前验证 latest.clash.yaml/index.json/status.html/README.md 存在且非空，避免提交失败结果）；版本号 v2.0.0→v2.1.0；reference-docs.md 和 deploy.sh 同步更新 |
| 2026-08-26 | 实施 v2.2.0 P8 fetch.yaml 工作流第二批优化 + 韩国组确认 + git push 并发冲突修复：T8.1 fetch.yaml 第二批优化（4项）——T8.1a subconverter 下载缓存（新增 Get subconverter latest version step 获取版本号作为 cache key，新增 Cache subconverter step 缓存 subconverter_bin 目录，缓存命中时跳过下载，减少重复下载时间）、T8.1b Commit 信息增强（从 output/index.json 读取 output_count 和 avg_latency_ms，commit message 格式为"Update <时间> | <节点数> nodes | <延迟>ms avg"，方便回溯每次运行的质量指标）、T8.1c GitHub Pages action 升级（peaceiris/actions-gh-pages v3→v4）、T8.1d subconverter 下载重试（wget 新增 --tries=3 --retry-connrefused --timeout=30，网络不稳定时自动重试3次）；T8.2 韩国代理组缺失排查——结论：韩国组实际已存在（P7 v2.1.0 地区组 filter 增强已解决），线上融合订阅包含 🇰🇷 韩国节点组，包含2个韩国节点，之前误判为缺失是因为终端编码显示问题（中文/emoji 显示为乱码）；T8.3 git push 并发冲突修复（热修复）——#109 手动触发与 #108 定时任务几乎同时运行（差29秒），后 push 的因 non-fast-forward 失败（Commit and Push step），修复：Commit and Push step 添加 git pull --rebase + 3次重试机制，push 前先同步远程最新代码，失败自动重试，rebase 失败则 abort 后重试；版本号 v2.1.0→v2.2.0；reference-docs.md 和 deploy.sh 同步更新 |
| 2026-08-27 | 实施 v2.3.0 P9 主订阅地区组支持 + config.yaml 资源审查：T9.1 主订阅地区组支持——问题：主订阅（output/clash/latest.yaml）只有13个 subconverter 默认代理组，没有地区组/故障转移/负载均衡，而融合订阅（output/merged/latest.clash.yaml）有22个代理组含全部地区组；根因：fetch.yaml 只追加了 external_config.ini（标准 section）到 pref.ini，而 merged_config.ini（旧 [custom] section，含 custom_proxy_group 地区组定义）未被追加，subconverter 对 [custom] section 和标准 section 的处理方式不同，导致主订阅缺少地区组；修复：fetch.yaml 中同时追加 merged_config.ini 到 pref.ini，确保主订阅也能获得地区组/故障转移/负载均衡配置；影响：部署后主订阅将拥有与融合订阅相同的地区分组功能；T9.2 config.yaml 频道资源审查——当前31个 TG 频道，主订阅产出123个节点，整体有效，由于无法直接验证 TG 频道有效性，保留当前列表，建议用户发现失效频道可手动从 config.yaml 移除；版本号 v2.2.0→v2.3.0；reference-docs.md 和 deploy.sh 同步更新 |
| 2026-08-27 | 实施 v2.3.1 P9.1 地区组根本原因修复（热修复）：问题——P9（v2.3.0）部署后地区组仍未生效，主订阅13个代理组，融合订阅10个，都没有地区组/故障转移/负载均衡；真正根因——P6（v2.0.0）错误地将 custom_proxy_group 从 [common] section 移到了 [proxy_groups] section，但 subconverter 官方规范要求 custom_proxy_group 必须放在 [common] section 中，导致地区组配置全部未加载；P9 的修复方向错误——在 fetch.yaml 中追加 merged_config.ini（[custom] section）也无法生效，因为 [custom] section 同样不是 subconverter 识别的标准 section；修复——将 external_config.ini 中的 custom_proxy_group 从 [proxy_groups] section 移回 [common] section，与 add_emoji / exclude_remarks / rename 等配置同 section；撤销——fetch.yaml 中追加 merged_config.ini 的修改（merged_config.ini 的 [custom] section 格式错误，不需要追加）；影响——部署后主订阅和融合订阅都将拥有地区组（10个地区）/故障转移/负载均衡；版本号 v2.3.0→v2.3.1；reference-docs.md 和 deploy.sh 同步更新 |
| 2026-08-27 | 实施 v2.3.2 P9.2 地区组修复 + 违规词过滤（第二轮热修复）：T9.2 地区组修复——问题：P9.1（v2.3.1）部署后地区组仍未生效，主订阅13个代理组，没有地区组；根因：fetch.yaml 使用追加方式（cat external_config.ini >> pref.ini），导致 pref.ini 中出现重复 [common] section，subconverter 可能只读取第一个 [common] section（来自 pref.example.ini），导致后面追加的 custom_proxy_group / emoji / ruleset 配置全部被忽略；修复：1. external_config.ini 添加基础配置（clash_rule_base / surge_rule_base），使其成为完整的 pref.ini 替代文件；2. fetch.yaml 改为直接用 external_config.ini 替换 pref.ini（cp 而非 >>），彻底避免重复 section 问题；影响：部署后主订阅和融合订阅都将拥有地区组（10个地区）/故障转移/负载均衡。T9.3 违规词过滤——问题：部分上游节点名称包含违规内容（如"高清无码"、"AVToday"等），导入订阅时会显示这些不良内容；修复：external_config.ini 的 exclude_remarks 添加违规词过滤，屏蔽包含色情/成人/高清无码/AVToday 等违规关键词的节点；影响：部署后输出的订阅将不再包含违规名称的节点；版本号 v2.3.1→v2.3.2；reference-docs.md 和 deploy.sh 同步更新 |
| 2026-08-27 | 实施 v2.3.3 P9.3 地区组修复（第三轮-最终方案）+ 违规词过滤：T9.4 地区组修复——问题：P9.2（v2.3.2）部署后地区组仍未生效，主订阅13个代理组，没有地区组；截图报错"🔴 广告拦截 not found"，说明直接替换 pref.ini 丢失了默认配置导致代理组与规则不匹配；根因：通过 pref.ini 加载配置存在多种问题——1. 追加方式导致重复 [common] section，subconverter 只读取第一个；2. 直接替换方式丢失 pref.example.ini 的默认配置，导致代理组与规则不匹配；最终方案：不再依赖 pref.ini 加载自定义配置，改为通过 subconverter API 的 config 参数直接传递 external_config.ini 的 file:// 绝对路径；修改：call_subconverter() 函数的 API URL 添加 config 参数，使用 os.path.abspath() 获取 external_config.ini 的绝对路径，URL-encode 后传递；fetch.yaml 恢复为从 pref.example.ini 复制默认配置，不追加 external_config.ini；优势：1. 配置直接通过 API 传递，不依赖 pref.ini 的 section 合并逻辑；2. pref.ini 保留默认配置，避免代理组与规则不匹配；3. 配置路径动态获取，不硬编码。T9.3 违规词过滤（延续）：external_config.ini 的 exclude_remarks 添加违规词过滤，屏蔽包含色情/成人/高清无码/AVToday 等违规关键词的节点；版本号 v2.3.2→v2.3.3；reference-docs.md 和 deploy.sh 同步更新 |
| 2026-08-27 | 实施 v2.4.0 P10 节点统一重命名 + 新增订阅源 + 违规词彻底规避：T10.1 节点统一重命名（核心功能）——问题：上游节点名称包含违规词（如"高清无码"、"AVToday"等），subconverter 的 exclude_remarks 过滤未完全生效；方案：在 generate_multi_format() 中新增预处理步骤——1. 先调用 subconverter 拉取解析所有订阅，生成 Clash YAML 临时文件；2. 解析临时文件，获取所有 proxies；3. 统一重命名为 01、02、03...（保留原始顺序）；4. 写入新的临时 Clash YAML 文件（仅含 proxies）；5. 启动本地 HTTP 服务提供该文件，替代原始 URL 传递给 subconverter；优势：1. 彻底规避所有违规词，不依赖 exclude_remarks 正则匹配；2. 节点名称简洁统一，便于管理和选择；3. 所有格式（Clash/V2Ray/Sing-box/Surge/Mixed）的节点都会被重命名；新增函数：_rename_and_prepare_subscriptions()。T10.2 新增订阅源——TG 频道：https://t.me/stymei1（添加到 config.yaml）；订阅链接：https://sub.maflya.com/（添加到 merge.yaml 作为融合订阅上游）；maflya 网站参考：节点索引编号、上游源状态监控、实时在线设备统计等。T10.3 违规词彻底规避——通过节点统一重命名（T10.1）彻底规避违规词，保留 external_config.ini 的 exclude_remarks 作为双重保障；版本号 v2.3.3→v2.4.0；reference-docs.md 和 deploy.sh 同步更新
| 2026-08-28 | 实施 v2.5.0 P11 质量优化第一版：延迟阈值过滤 (LATENCY_MAX_THRESHOLD) + 总量上限降低 (MERGED_MAX_NODES 200→150) + 质量排序优化；后续 v2.5.1~v2.5.6 (P11.1~P11.6) 逐步修复违规词源头过滤、主订阅重命名修复、重命名预处理返回值 bug、Clash 输出兜底重命名 (_filter_and_rename_clash_file)、主订阅最终兜底重命名 (遍历 output/clash/)、融合订阅兜底重命名 + 延迟阈值放宽 (LATENCY_MAX_THRESHOLD 放宽至 2000ms)、协议过滤 (subconverter 不支持 vless/hysteria2, 融合节点恢复) 与 config_path 参数被忽略修复 (融合订阅始终用主订阅配置的 bug)；版本号 v2.4.0→v2.5.6 |
| 2026-08-30 | 实施 v2.6.0 P12 保留地区+序号重命名：新增 REGION_EMOJI_MAP + _rename_with_region (转换前生成 "US 01"/"JP 02") + _rename_with_region_emoji (输出兜底生成 "🇺🇸 01"/"🇭🇰 02")，三处重命名逻辑全部改为保留地区；merged_config.ini 地区 filter 补 emoji；版本号 v2.5.6→v2.6.0 |
| 2026-08-31 | 实施 v2.6.1 P13 修复 detect_region 类型崩溃 + merged_config.ini section 修复：T13.1 detect_region 增加类型保护 (text = str(text))，修复 subconverter 输出 name 为 int 的节点 (58/59/60) 导致兜底重命名整体异常返回 False 的问题；T13.2 merged_config.ini 重写为 [common]+[rulesets]（当时误判方向）；T13.3 _filter_and_rename_clash_file 建立 rename_map 同步更新 proxy-groups 引用；版本号 v2.6.0→v2.6.1 |
| 2026-09-03 | 实施 v2.7.0 P15 主订阅转换持续失败根因修复（双重重罪）：① 主订阅 URL 混入大量空壳/失效订阅 (20 中 16 无效), subconverter 合并整体 400 → 主订阅断更 (latest 停留 8-21); 新增 filter_valid_sub_urls 合并测试优先+失败逐个剔除; ② 主订阅 emoji 节点名被 remove_old_emoji 拆成 int, 改回 _rename_with_region (US 01) 由 subconverter 加 emoji; ③ external_config.ini 删除冲突 rename 规则+扩充 emoji/加拿大组; 效果: 主订阅 87 节点/15 地区组/🇫🇷 01 格式/0 违规词 |
| 2026-09-02 | 实施 v2.6.2 P14 subconverter 外部配置加载根因修复（双重重罪）：① config 参数 file:// 前缀错误 (P9.3 引入)——实测 subconverter v0.9.0 + 源码: loadExternalConfig() 用 fetchFile(path) 读取, file:// scheme 无法识别为本地文件, Linux 上 isInScope 拒绝以 / 开头的绝对路径, external_config.ini/merged_config.ini 从未被加载; ② 外部配置 section 错误 (P6/P9.1/P13 引入)——subconverter 外部 config 标准 section 是 [custom] (settings.cpp set_isolated_items_section("custom")+enter_section("custom")), rule= 应为 emoji=; 修复: T14.1 external_config.ini 重写为 [custom]; T14.2 merged_config.ini 重写为 [custom]; T14.3 main.py call_subconverter 去 file:// 前缀改用相对文件名 (config=external_config.ini); T14.4 fetch.yaml 启动前复制配置文件到 subconverter 运行目录; 本地实测 [custom]+emoji=+相对路径 → 9 个地区代理组全部出现; 版本号 v2.6.1→v2.6.2 | |
| 2026-08-17 | 升级至 v1.4.0：新增上游订阅融合（merge.yaml 白名单 + 拉取重试失败隔离 + 三格式解析 + [ext:] 前缀 + output/merged 独立输出含回退保护 + README 免责声明/来源表）；新增 4.14 节；同步更新第 1/2/3/4/5/7/8 节；本地 10 项单元验证通过 |

<!-- 待补充/更新标记：
- [ ] 新增文件时在此文档第 4 节添加对应小节
- [ ] 版本号变更时更新第 2 节版本历史表
- [ ] 参数调整时更新第 5 节速查表
- [ ] 遇到新的常见问题时补充第 7 节
- [ ] deploy.sh 的 commit message 需在每次部署前手动更新
-->
