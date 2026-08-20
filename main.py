#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NodeCollection Pro v1.6.0 - 订阅源采集 + 多格式转换一体化工具

架构:
  config.yaml (TG频道) + airports.yaml (机场列表) + merge.yaml (上游订阅白名单)
  → 并发爬取频道 + 探测机场公开订阅
  → 校验分类去重
  → 原始 YAML 输出 (向后兼容)
  → subconverter API 多格式转换 (Clash/V2Ray/Surge/SingBox)
  → 上游订阅融合: 白名单拉取 → 解析 → 安检 → [ext:来源] 标注 → 独立输出 output/merged/
  → GitHub Actions 自动提交

新增模块:
  - load_airports(): 加载机场列表
  - probe_airport(): 探测机场公开订阅链接
  - call_subconverter(): 调用 subconverter API 转换格式
  - generate_multi_format(): 生成多格式订阅文件
  - load_upstreams(): 加载上游订阅白名单 (merge.yaml)
  - fetch_upstream() / fetch_all_upstreams(): 上游拉取 (重试 + 失败隔离)
  - parse_upstream_text(): 解析上游订阅 (Base64 / 明文链接 / Clash YAML)
  - generate_merged_format(): 融合节点独立输出到 output/merged/
  - extract_host_port(): 从分享链接 URI 提取 host/port (协议无关)
  - measure_uri_latencies() / measure_proxy_latencies(): 并发 TCP 测速 (P1 v1.5.0)
"""

import re
import os
import sys
import time
import json
import socket
import base64
import shutil
import tempfile
import threading
import ipaddress
import datetime
import http.server
from urllib.parse import urlparse, quote, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml
import requests
from loguru import logger
from tqdm import tqdm
from retry import retry

# ============================================================
# 配置常量
# ============================================================

CONFIG_PATH = './config.yaml'
AIRPORTS_PATH = './airports.yaml'
MERGE_PATH = './merge.yaml'
SUB_DIR = 'sub'
OUTPUT_DIR = 'output'
MERGED_DIR = 'merged'

MAX_THREADS = 32
CHANNEL_THREADS = 8
AIRPORT_THREADS = 8
REQUEST_TIMEOUT = 10
CHANNEL_TIMEOUT = 15
AIRPORT_TIMEOUT = 8
RETRY_TIMES = 2
USER_AGENT = 'ClashforWindows/0.18.1'
PROTOCOL_PREFIXES = ('ss://', 'ssr://', 'vmess://', 'trojan://')

# 上游订阅融合配置 (v1.4.0)
UPSTREAM_THREADS = 8
UPSTREAM_TIMEOUT = 60
UPSTREAM_RETRIES = 3
UPSTREAM_RETRY_DELAY = 5
# 上游节点允许的协议前缀 (比自采管道宽: 兼容 vless / hysteria2 / tuic 等新协议)
UPSTREAM_PROTOCOL_PREFIXES = (
    'ss://', 'ssr://', 'vmess://', 'vless://', 'trojan://',
    'hysteria://', 'hysteria2://', 'hy2://', 'tuic://',
)
# Clash YAML 代理类型白名单 (解析上游 Clash 订阅时过滤未知类型)
CLASH_PROXY_TYPES = {
    'ss', 'ssr', 'vmess', 'vless', 'trojan', 'hysteria', 'hysteria2',
    'tuic', 'snell', 'socks5', 'http', 'mixed',
}

# 节点延迟测速配置 (P1 v1.5.0)
LATENCY_TIMEOUT = 5           # 单节点 TCP connect 超时 (秒)
LATENCY_THREADS = 32          # 并发测速线程数
LATENCY_SAMPLE_RATIO = 1.0    # 抽测比例 (1.0 = 全量测速), 可在 merge.yaml 覆盖
LATENCY_FAIL_THRESHOLD = 2   # 连续 N 个周期不可达则剔除 (与 T1.2 共用)
MERGED_MAX_NODES = 200       # 融合输出每格式总量上限 (与 T1.4 共用)

# 历史版本保留配置 (P2 v1.6.0, T2.2)
HISTORY_KEEP_COUNT = 5       # 每格式保留最近 N 版日期文件, 超出自动清理最旧版本

# 上游监控与降级配置 (P2 v1.6.0, T2.3)
UPSTREAM_DEGRADE_THRESHOLD = 3  # 连续 N 次拉取失败标记为 degraded (已降级)

# 上游拉取镜像回退: 主 URL 连续失败时按序尝试镜像前缀 (仅对 raw.githubusercontent.com 生效)
# 解决本地/国内网络直连 raw.githubusercontent.com 被阻断 (10054) 或限流 (HTTP 429) 的问题
RAW_GITHUB_HOST = 'raw.githubusercontent.com'
UPSTREAM_MIRROR_PREFIXES = (
    'https://ghfast.top/',
    'https://gh-proxy.com/',
    'https://ghproxy.net/',
)

# subconverter 配置
SUBCONVERTER_URL = os.environ.get('SUBCONVERTER_URL', 'http://127.0.0.1:25500')
SUBCONVERTER_TIMEOUT = 60
SUBCONVERTER_RETRIES = 3
SUBCONVERTER_RETRY_DELAY = 5
SUBCONVERTER_EXTERNAL_CONFIG = 'subconverter/external_config.ini'
SUBCONVERTER_MERGED_CONFIG = 'subconverter/merged_config.ini'  # T1.3: 融合订阅独立配置 (含韩英分组)

# GitHub 仓库信息 (用于生成 README 中的订阅链接)
GITHUB_OWNER = os.environ.get('GITHUB_OWNER', 'huiwin')
GITHUB_REPO = os.environ.get('GITHUB_REPO', 'NodeCollection')
GITHUB_BRANCH = os.environ.get('GITHUB_BRANCH', 'main')

# Raw 链接基础路径
RAW_BASE = f'https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_BRANCH}'

# 加速代理前缀配置: (显示名, 前缀模板)
# {url} 会被替换为原始 raw 链接
PROXY_PREFIXES = [
    ('原生', '{url}'),
    ('kkgithub', 'https://raw.kkgithub.com/{GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_BRANCH}{path}'),
    ('ghproxy.net', 'https://ghproxy.net/{url}'),
    ('gh-proxy.com', 'https://gh-proxy.com/{url}'),
    ('ghfast.top', 'https://ghfast.top/{url}'),
    ('jsdelivr', 'https://fastly.jsdelivr.net/gh/{GITHUB_OWNER}/{GITHUB_REPO}@{GITHUB_BRANCH}{path}'),
]

# 输出格式: (target参数, 输出子目录, 文件扩展名)
# T2.1 (v1.6.0): 新增 singbox 格式 (Sing-box / SagerNet / Hiddify 客户端)
OUTPUT_FORMATS = [
    ('clash', 'clash', 'yaml'),
    ('v2ray', 'v2ray', 'txt'),
    ('surge&ver=4', 'surge', 'conf'),
    ('mixed', 'mixed', 'txt'),
    ('singbox', 'singbox', 'json'),
]

URL_REGEX = re.compile(
    r'https?://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]'
)
# 机场页面中常见的订阅/节点关键词
SUB_KEYWORDS = re.compile(
    r'(subscribe|subscription|sub|api/v1/client|clash|v2ray|trojan|ssr|free|'
    r'node|节点|订阅|免费|试用|trial)',
    re.IGNORECASE
)


# ============================================================
# 初始化目录
# ============================================================

def pre_check():
    """创建 sub/YYYY/M/ 和 output/ 目录结构，返回当日输出路径。"""
    today = datetime.datetime.today()
    path_year = os.path.join(SUB_DIR, str(today.year))
    path_mon = os.path.join(path_year, str(today.month))
    path_yaml = os.path.join(path_mon, f'{today.month}-{today.day}.yaml')

    for directory in (SUB_DIR, path_year, path_mon, OUTPUT_DIR):
        if not os.path.exists(directory):
            os.makedirs(directory)

    logger.info('初始化目录完成')
    return path_yaml


# ============================================================
# YAML 读写
# ============================================================

def yaml_check(path_yaml):
    """读取已有订阅 YAML，不存在则返回空结构。"""
    if os.path.isfile(path_yaml):
        with open(path_yaml, encoding='UTF-8') as f:
            dict_url = yaml.load(f, Loader=yaml.FullLoader)
    else:
        dict_url = {
            '机场订阅': [],
            'clash订阅': [],
            'v2订阅': [],
            '开心玩耍': [],
        }
    logger.info('读取已有文件成功')
    return dict_url


def yaml_save(path_yaml, dict_url):
    """将订阅数据写入 YAML 文件。"""
    with open(path_yaml, 'w', encoding='utf-8') as f:
        yaml.dump(dict_url, f, allow_unicode=True)
    logger.info(f'写入原始 YAML: {path_yaml}')


# ============================================================
# 配置读取
# ============================================================

def get_config():
    """读取 config.yaml 中的 Telegram 频道列表。"""
    with open(CONFIG_PATH, encoding='UTF-8') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)

    list_tg = data.get('tgchannel', [])
    new_list = []
    for url in list_tg:
        channel_name = url.split('/')[-1].strip()
        if channel_name:
            new_list.append(f'https://t.me/s/{channel_name}')
    logger.info(f'读取 TG 频道配置: {len(new_list)} 个')
    return new_list


def load_airports():
    """
    读取 airports.yaml 机场列表。
    返回: list[dict] 每项含 domain, clash(可选), note(可选)
    """
    if not os.path.isfile(AIRPORTS_PATH):
        logger.info('未找到 airports.yaml，跳过机场探测')
        return []

    with open(AIRPORTS_PATH, encoding='UTF-8') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)

    airports = data.get('airports', []) if data else []
    logger.info(f'读取机场列表: {len(airports)} 个')
    return airports


# ============================================================
# 安全工具
# ============================================================

def is_safe_url(url):
    """URL 安全校验，阻止内网地址（SSRF 防护）。"""
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in ('http', 'https'):
        return False
    host = parsed.hostname
    if not host:
        return False
    try:
        ip = ipaddress.ip_address(host)
        if ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local:
            return False
    except ValueError:
        pass
    return True


# ============================================================
# 频道爬取
# ============================================================

def get_channel_http(session, channel_url):
    """请求 Telegram 频道公开页面，正则提取所有 URL。"""
    try:
        resp = session.get(channel_url, timeout=CHANNEL_TIMEOUT)
        url_list = URL_REGEX.findall(resp.text)
        logger.info(f'{channel_url}\t获取成功 ({len(url_list)} 个 URL)')
        return url_list
    except requests.Timeout:
        logger.warning(f'{channel_url}\t请求超时')
        return []
    except requests.ConnectionError:
        logger.warning(f'{channel_url}\t连接失败')
        return []
    except Exception as e:
        logger.warning(f'{channel_url}\t获取失败: {type(e).__name__}: {e}')
        return []


def crawl_all_channels(session, channel_urls):
    """并发爬取所有 Telegram 频道。"""
    all_urls = []
    with ThreadPoolExecutor(max_workers=CHANNEL_THREADS) as executor:
        futures = {
            executor.submit(get_channel_http, session, url): url
            for url in channel_urls
        }
        for future in as_completed(futures):
            result = future.result()
            if result:
                all_urls.extend(result)

    safe_urls = [u for u in all_urls if is_safe_url(u)]
    unique_urls = list(set(safe_urls))
    logger.info(
        f'频道爬取完成: 原始 {len(all_urls)}, 安全 {len(safe_urls)}, 去重 {len(unique_urls)}'
    )
    return unique_urls


# ============================================================
# 机场探测 (Phase 1: 简单版 - 探测公开页面中的订阅链接)
# ============================================================

def probe_airport(session, airport):
    """
    探测单个机场域名的公开订阅链接。

    策略:
      1. 访问 https://{domain}/ 页面
      2. 正则提取页面中所有 URL
      3. 过滤出含订阅关键词的链接
      4. 返回候选订阅 URL 列表

    注意: 此为 Phase 1 简单实现，不包含自动注册。
    Phase 2 将增加 v2board API 自动注册获取试用订阅。
    """
    domain = airport.get('domain', '').strip()
    if not domain:
        return []

    # 标准化域名为完整 URL
    if not domain.startswith('http'):
        domain = f'https://{domain}'

    if not is_safe_url(domain):
        return []

    candidate_urls = []
    try:
        resp = session.get(domain, timeout=AIRPORT_TIMEOUT, allow_redirects=True)
        page_urls = URL_REGEX.findall(resp.text)

        for url in page_urls:
            if not is_safe_url(url):
                continue
            # 检查 URL 或路径是否包含订阅关键词
            if SUB_KEYWORDS.search(url):
                candidate_urls.append(url)

        # 也检查页面文本中的 base64 编码内容 (可能包含节点链接)
        # 这里保持简单，不做 base64 解码

    except requests.Timeout:
        logger.debug(f'[airport] {domain}\t超时')
    except requests.ConnectionError:
        logger.debug(f'[airport] {domain}\t连接失败')
    except Exception as e:
        logger.debug(f'[airport] {domain}\t{type(e).__name__}: {e}')

    if candidate_urls:
        logger.info(f'[airport] {domain}\t发现 {len(candidate_urls)} 个候选订阅')
    return candidate_urls


def probe_all_airports(session, airports):
    """并发探测所有机场域名。"""
    if not airports:
        return []

    all_candidates = []
    logger.info(f'开始探测 {len(airports)} 个机场域名 ---')

    with ThreadPoolExecutor(max_workers=AIRPORT_THREADS) as executor:
        futures = {
            executor.submit(probe_airport, session, airport): airport
            for airport in airports
        }
        for future in as_completed(futures):
            result = future.result()
            if result:
                all_candidates.extend(result)

    unique_candidates = list(set(all_candidates))
    logger.info(
        f'机场探测完成: 原始 {len(all_candidates)}, 去重 {len(unique_candidates)}'
    )
    return unique_candidates


# ============================================================
# 上游订阅融合 (v1.4.0 External Sources)
# 白名单拉取 → 解析 → 安检 → [ext:来源] 标注 → 独立输出 output/merged/
# ============================================================

def load_upstreams():
    """
    读取 merge.yaml 上游订阅白名单。
    返回: list[dict] 每项含 name, url, enabled(可选), max_nodes(可选)
    配置缺失或为空时返回空列表，不影响主流程。
    """
    if not os.path.isfile(MERGE_PATH):
        logger.info('未找到 merge.yaml，跳过上游订阅融合')
        return []

    try:
        with open(MERGE_PATH, encoding='UTF-8') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
    except Exception as e:
        logger.warning(f'merge.yaml 读取失败: {e}，跳过上游订阅融合')
        return []

    upstreams = [
        u for u in (data.get('upstreams') or [])
        if isinstance(u, dict) and u.get('url') and u.get('enabled', True)
    ]
    logger.info(f'读取上游订阅白名单: {len(upstreams)} 个启用')
    return upstreams


def _upstream_fetch_candidates(url):
    """
    构造上游拉取的候选 URL 列表: [主 URL] + 镜像 URL (仅 raw.githubusercontent.com)。
    主 URL 享有完整重试次数；镜像各尝试 1 次，作为网络阻断时的回退通道。
    """
    candidates = [url]
    try:
        if urlparse(url).hostname == RAW_GITHUB_HOST:
            candidates += [m + url for m in UPSTREAM_MIRROR_PREFIXES]
    except Exception:
        pass
    return candidates


def _fetch_upstream_once(session, name, url):
    """单次拉取一个 URL。成功返回订阅文本，失败返回 None (日志由调用方统一输出)。"""
    resp = session.get(
        url, headers={'User-Agent': USER_AGENT}, timeout=UPSTREAM_TIMEOUT
    )
    if resp.status_code == 200 and resp.text.strip():
        logger.info(f'[ext:{name}] 拉取成功 ({len(resp.text)} bytes, via {urlparse(url).hostname})')
        return resp.text
    logger.warning(f'[ext:{name}] 拉取失败: HTTP {resp.status_code} (via {urlparse(url).hostname})')
    return None


def fetch_upstream(session, upstream):
    """
    拉取单个上游订阅内容。
    主 URL: 60s 超时 + 3 次重试 (间隔 5s)；全部失败后依次尝试镜像 (各 1 次)。
    任何成功即返回，彻底失败返回 None，不阻断主流程 (失败隔离)。
    """
    name = upstream.get('name', 'unknown')
    url = upstream['url']

    if not is_safe_url(url):
        logger.warning(f'[ext:{name}] URL 未通过安全校验 (SSRF 防护)，已跳过')
        return None

    candidates = _upstream_fetch_candidates(url)

    # 1. 主 URL: 完整重试
    for attempt in range(1, UPSTREAM_RETRIES + 1):
        try:
            text = _fetch_upstream_once(session, name, candidates[0])
            if text:
                return text
        except requests.Timeout:
            logger.warning(
                f'[ext:{name}] 第 {attempt}/{UPSTREAM_RETRIES} 次请求超时 '
                f'({UPSTREAM_TIMEOUT}s)'
            )
        except Exception as e:
            logger.warning(
                f'[ext:{name}] 第 {attempt}/{UPSTREAM_RETRIES} 次异常: '
                f'{type(e).__name__}: {e}'
            )

        if attempt < UPSTREAM_RETRIES:
            time.sleep(UPSTREAM_RETRY_DELAY)

    # 2. 镜像回退: 各尝试 1 次
    for mirror_url in candidates[1:]:
        try:
            text = _fetch_upstream_once(session, name, mirror_url)
            if text:
                return text
        except Exception as e:
            logger.warning(
                f'[ext:{name}] 镜像拉取异常: {urlparse(mirror_url).hostname}: '
                f'{type(e).__name__}'
            )

    logger.error(f'[ext:{name}] 主 URL + {len(candidates) - 1} 个镜像均拉取失败，已跳过')
    return None


def fetch_all_upstreams(session, upstreams):
    """并发拉取所有上游订阅。返回 {name: 订阅文本}，仅含成功的上游。"""
    results = {}
    if not upstreams:
        return results

    with ThreadPoolExecutor(max_workers=UPSTREAM_THREADS) as executor:
        futures = {executor.submit(fetch_upstream, session, u): u for u in upstreams}
        for future in as_completed(futures):
            upstream = futures[future]
            text = future.result()
            if text:
                results[upstream['name']] = text

    logger.info(f'上游拉取完成: 成功 {len(results)}/{len(upstreams)}')
    return results


def rename_uri_node(uri, prefix):
    """
    给分享链接的节点显示名加来源前缀，返回修改后的 URI。
    vmess 的名称在 Base64 JSON 的 ps 字段中；其余协议名称在 #fragment 中。
    解析失败时原样返回，不中断流程。
    """
    try:
        scheme, body = uri.split('://', 1)
        scheme = scheme.lower()

        if scheme == 'vmess':
            padded = body + '=' * (-len(body) % 4)
            info = json.loads(base64.b64decode(padded).decode('utf-8'))
            info['ps'] = f"{prefix} {info.get('ps', '')}".strip()
            payload = base64.b64encode(
                json.dumps(info, ensure_ascii=False).encode('utf-8')
            ).decode('utf-8')
            return f'vmess://{payload}'

        if '#' in body:
            body, fragment = body.rsplit('#', 1)
        else:
            fragment = ''
        name = unquote(fragment).strip() or 'node'
        return f"{scheme}://{body}#{quote(f'{prefix} {name}'.strip(), safe='[]: ')}".replace(' ', '%20')
    except Exception:
        return uri


def filter_clash_proxies(proxies, prefix):
    """
    过滤上游 Clash YAML 中的代理节点:
      1. 类型白名单 (CLASH_PROXY_TYPES)
      2. server 为内网/保留地址时剔除 (SSRF 防护)
      3. 节点名加 [ext:来源] 前缀
    """
    valid = []
    for proxy in proxies:
        if not isinstance(proxy, dict):
            continue
        ptype = str(proxy.get('type', '')).lower()
        server = str(proxy.get('server', '')).strip()
        if ptype not in CLASH_PROXY_TYPES or not server:
            continue
        try:
            ip = ipaddress.ip_address(server)
            if ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local:
                continue
        except ValueError:
            pass  # 域名节点放行，由客户端解析
        proxy['name'] = f"{prefix} {proxy.get('name', 'node')}".strip()
        valid.append(proxy)
    return valid


def parse_upstream_text(text, upstream):
    """
    解析上游订阅文本为统一节点集合。
    自动识别: Clash YAML (proxies) / 整体 Base64 / 明文分享链接列表。
    返回: (uris: list[str], clash_proxies: list[dict])
    """
    name = upstream.get('name', 'unknown')
    max_nodes = int(upstream.get('max_nodes', 0) or 0)
    prefix = f'[ext:{name}]'
    uris, clash_proxies = [], []

    stripped = text.strip()
    if 'proxies:' in stripped[:2000]:
        # Clash YAML 订阅
        try:
            data = yaml.safe_load(stripped)
            proxies = (data or {}).get('proxies') or []
            clash_proxies = filter_clash_proxies(proxies, prefix)
            logger.info(
                f'[ext:{name}] Clash YAML 解析: {len(proxies)} 个代理, '
                f'过滤后 {len(clash_proxies)}'
            )
        except Exception as e:
            logger.warning(f'[ext:{name}] Clash YAML 解析失败: {e}')
    else:
        lines = stripped.splitlines()
        # 尝试整体 Base64 解码 (v2ray 标准订阅格式)
        try:
            padded = stripped + '=' * (-len(stripped) % 4)
            decoded = base64.b64decode(padded).decode('utf-8', errors='ignore')
            if any(l.strip().startswith(UPSTREAM_PROTOCOL_PREFIXES)
                   for l in decoded.splitlines()):
                lines = decoded.splitlines()
        except Exception:
            pass
        # 明文分享链接列表 (含 Base64 解码结果)
        uris = [
            l.strip() for l in lines
            if l.strip().startswith(UPSTREAM_PROTOCOL_PREFIXES)
        ]
        uris = [rename_uri_node(u, prefix) for u in uris]
        logger.info(f'[ext:{name}] 分享链接解析: {len(uris)} 个节点')

    # T1.4: 截断逻辑已移至 generate_merged_format (测速排序后按延迟截断)
    # 此处返回全部解析节点, 由上层统一做「单源 max_nodes + 总量 MERGED_MAX_NODES」两层截断
    return uris, clash_proxies


# ============================================================
# 节点延迟测速 (P1 v1.5.0)
# 对融合节点做 TCP connect 计时，记录每节点延迟 (ms)。
# 协议无关: vmess/ss/ssr/vless/trojan/hysteria2/tuic 均通过 TCP 握手测速。
# ============================================================

def _strip_ipv6_brackets(host):
    """去掉 IPv6 地址的方括号 (如 [::1] → ::1)，非 IPv6 原样返回。"""
    if host and host.startswith('[') and host.endswith(']'):
        return host[1:-1]
    return host


def extract_host_port(uri):
    """
    从分享链接 URI 中提取 host 和 port。
    支持: vmess (Base64 JSON) / ss / ssr / vless / trojan / hysteria2 / tuic
    返回: (host, port) 或 (None, None)
    """
    try:
        scheme, body = uri.split('://', 1)
        scheme = scheme.lower()

        # --- vmess: Base64 JSON, 含 add/port 字段 ---
        if scheme == 'vmess':
            padded = body + '=' * (-len(body) % 4)
            info = json.loads(base64.b64decode(padded).decode('utf-8'))
            host = str(info.get('add', '')).strip()
            port = int(info.get('port', 0) or 0)
            if host and port > 0:
                return _strip_ipv6_brackets(host), port
            return None, None

        # --- ssr: base64(host:port:protocol:method:obfs:base64pass/?params) ---
        if scheme == 'ssr':
            padded = body + '=' * (-len(body) % 4)
            decoded = base64.b64decode(padded).decode('utf-8', errors='ignore')
            main = decoded.split('/?')[0]
            parts = main.split(':')
            if len(parts) >= 2:
                host = parts[0]
                port = int(parts[1])
                if host and port > 0:
                    return _strip_ipv6_brackets(host), port
            return None, None

        # --- ss (Shadowsocks): SIP002 或 legacy ---
        if scheme == 'ss':
            # 去掉 fragment
            body_no_frag = body.split('#', 1)[0]
            if '@' in body_no_frag:
                # SIP002: ss://base64(method:password)@host:port[#name]
                _, hostport = body_no_frag.rsplit('@', 1)
                hostport = hostport.split('?')[0]
                if ':' in hostport:
                    host, port_str = hostport.rsplit(':', 1)
                    port = int(port_str)
                    if host and port > 0:
                        return _strip_ipv6_brackets(host), port
            else:
                # Legacy: ss://base64(method:password@host:port)#name
                padded = body_no_frag + '=' * (-len(body_no_frag) % 4)
                decoded = base64.b64decode(padded).decode('utf-8', errors='ignore')
                if '@' in decoded:
                    _, hostport = decoded.rsplit('@', 1)
                    if ':' in hostport:
                        host, port_str = hostport.rsplit(':', 1)
                        port = int(port_str)
                        if host and port > 0:
                            return _strip_ipv6_brackets(host), port
            return None, None

        # --- vless / trojan / hysteria / hysteria2 / hy2 / tuic ---
        # 标准 URI 格式: scheme://userinfo@host:port?params#name
        parsed = urlparse(uri)
        host = parsed.hostname
        port = parsed.port
        if host and port:
            return _strip_ipv6_brackets(host), port
        return None, None

    except Exception:
        return None, None


def _tcp_connect_latency(host, port):
    """
    TCP connect 计时，返回延迟 (ms)，超时或连接失败返回 None。
    用 socket.create_connection 完成 TCP 握手，测量往返时间。
    """
    try:
        start = time.time()
        sock = socket.create_connection((host, port), timeout=LATENCY_TIMEOUT)
        elapsed_ms = round((time.time() - start) * 1000)
        sock.close()
        return elapsed_ms
    except Exception:
        return None


def measure_uri_latencies(uris):
    """
    并发测速分享链接 URI 列表。
    返回: {uri: latency_ms_or_None}  (None = 不可达/无法解析)
    """
    if not uris:
        return {}

    # 抽测比例 (1.0 = 全量)
    sample = uris
    if LATENCY_SAMPLE_RATIO < 1.0:
        sample_count = max(1, int(len(uris) * LATENCY_SAMPLE_RATIO))
        sample = uris[:sample_count]

    results = {}
    bar = tqdm(total=len(sample), desc='节点测速')

    def _measure(uri):
        host, port = extract_host_port(uri)
        if not host or not port:
            return uri, None
        return uri, _tcp_connect_latency(host, port)

    with ThreadPoolExecutor(max_workers=LATENCY_THREADS) as executor:
        futures = {executor.submit(_measure, uri): uri for uri in sample}
        for future in as_completed(futures):
            uri, latency = future.result()
            results[uri] = latency
            bar.update(1)
    bar.close()

    # 未抽测的 URI 标记为 None
    for uri in uris:
        results.setdefault(uri, None)

    available = sum(1 for v in results.values() if v is not None)
    logger.info(f'测速完成: {available}/{len(uris)} 个节点可达')
    return results


def measure_proxy_latencies(proxies):
    """
    并发测速 Clash YAML 代理列表。
    proxies: list[dict] 每项含 'server', 'port', 'name' 字段
    返回: {proxy_name: latency_ms_or_None}
    """
    if not proxies:
        return {}

    results = {}
    bar = tqdm(total=len(proxies), desc='代理测速')

    def _measure(proxy):
        host = str(proxy.get('server', '')).strip()
        port = int(proxy.get('port', 0) or 0)
        name = proxy.get('name', '')
        if not host or not port:
            return name, None
        return name, _tcp_connect_latency(host, port)

    with ThreadPoolExecutor(max_workers=LATENCY_THREADS) as executor:
        futures = {executor.submit(_measure, p): p for p in proxies}
        for future in as_completed(futures):
            name, latency = future.result()
            results[name] = latency
            bar.update(1)
    bar.close()

    available = sum(1 for v in results.values() if v is not None)
    logger.info(f'代理测速完成: {available}/{len(proxies)} 个代理可达')
    return results


# ============================================================
# 节点健康记录与排序剔除 (P1 v1.5.0, T1.2)
# 健康记录嵌入 index.json 的 merged.quality.node_health 字段,
# 跟随 git 提交跨周期持久化 (避免 Actions 每次全新 checkout 丢失记录)。
# 记录结构: {node_id: {fail_count, last_latency, last_seen}}
#   node_id: 分享链接用完整 URI 串, Clash 代理用 proxy['name']
# ============================================================

def load_node_health():
    """
    从 index.json 的 merged.quality.node_health 读取历史节点健康记录。
    返回: dict {node_id: {fail_count, last_latency, last_seen}}
    文件不存在或损坏时返回空 dict, 不影响主流程。
    """
    index_path = os.path.join(OUTPUT_DIR, 'index.json')
    if not os.path.isfile(index_path):
        return {}
    try:
        with open(index_path, encoding='utf-8') as f:
            data = json.load(f)
        return (data.get('merged', {})
                   .get('quality', {})
                   .get('node_health', {}) or {})
    except Exception:
        return {}


def load_upstream_health():
    """
    从 index.json 的 merged.upstream_health 读取上游拉取健康记录。
    返回: dict {name: {fail_count, last_success, degraded}}
    文件不存在或损坏时返回空 dict, 不影响主流程。
    """
    index_path = os.path.join(OUTPUT_DIR, 'index.json')
    if not os.path.isfile(index_path):
        return {}
    try:
        with open(index_path, encoding='utf-8') as f:
            data = json.load(f)
        return (data.get('merged', {})
                   .get('upstream_health', {}) or {})
    except Exception:
        return {}


def filter_and_sort_nodes(items, latencies, health_data):
    """
    根据延迟与健康记录对节点排序、剔除。

    Args:
        items: 节点标识列表 (URI 字符串 或 Clash proxy name)
        latencies: {id: latency_ms_or_None}  (None = 本次测速不可达)
        health_data: 健康记录 dict (就地更新, 跨周期累计 fail_count;
                    传入前应已用 load_node_health() 加载)

    逻辑:
        1. 可达 (latency 非 None) → fail_count 清零, 记录 last_latency
        2. 不可达 (latency None) → fail_count += 1
        3. fail_count >= LATENCY_FAIL_THRESHOLD → 剔除 (不进入输出)
        4. 存活节点按延迟升序排列 (None 排末尾)

    Returns:
        (surviving_items, excluded_count)
        surviving_items 已按延迟升序排列, 可直接用于回写 all_uris / dedup_proxies
    """
    today_str = datetime.datetime.today().strftime('%Y-%m-%d')
    surviving = []
    excluded = 0
    for nid in items:
        lat = latencies.get(nid)  # None 表示本次测速不可达
        rec = health_data.get(nid, {'fail_count': 0, 'last_latency': None, 'last_seen': ''})
        if lat is None:
            rec['fail_count'] = rec.get('fail_count', 0) + 1
        else:
            rec['fail_count'] = 0
            rec['last_latency'] = lat
        rec['last_seen'] = today_str
        health_data[nid] = rec

        if rec['fail_count'] >= LATENCY_FAIL_THRESHOLD:
            excluded += 1
            label = str(nid)
            if len(label) > 60:
                label = label[:57] + '...'
            logger.info(
                f'[health] 剔除连续 {rec["fail_count"]} 次不可达节点: {label}'
            )
            continue
        surviving.append(nid)

    # 按延迟升序 (None 排末尾)
    surviving.sort(key=lambda x: (
        1 if latencies.get(x) is None else 0,
        latencies.get(x) if latencies.get(x) is not None else float('inf'),
    ))
    return surviving, excluded


def _serve_local_files(file_paths):
    """
    在本地随机端口启动临时 HTTP 服务，返回 (server, urls)。
    subconverter 通过 http://127.0.0.1:{port}/{fname} 拉取解析后的节点文件，
    避免上游原始 URL 直连，实现安检前置。
    """
    tmp_dir = os.path.dirname(file_paths[0])
    handler = lambda *args, **kw: http.server.SimpleHTTPRequestHandler(
        *args, directory=tmp_dir, **kw
    )
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    urls = [f'http://127.0.0.1:{port}/{os.path.basename(p)}' for p in file_paths]
    return server, urls


def _ensure_merged_latest_fallback(target, latest_fname):
    """merged 转换失败且 latest 缺失时，回退复制最近的历史日期文件 (按文件名日期)。"""
    subdir_path = os.path.join(OUTPUT_DIR, MERGED_DIR)
    latest_path = os.path.join(subdir_path, latest_fname)
    if os.path.exists(latest_path):
        return

    if not os.path.isdir(subdir_path):
        return  # 目录尚未创建 (从未成功转换过)，无历史文件可回退

    def _date_key(fname):
        m = re.match(r'(\d{1,2})-(\d{1,2})\.', fname)
        return (int(m.group(1)), int(m.group(2))) if m else (0, 0)

    candidates = sorted(
        (f for f in os.listdir(subdir_path)
         if f.endswith(f'.{target}.{latest_fname.rsplit(".", 1)[-1]}')
         and f != latest_fname),
        key=_date_key,
        reverse=True,
    )
    if candidates:
        shutil.copy2(os.path.join(subdir_path, candidates[0]), latest_path)
        logger.warning(
            f'[merged/{target}] 本次转换失败，latest 已回退为历史文件: {candidates[0]}'
        )


def cleanup_old_versions(directory, pattern_suffix, keep=HISTORY_KEEP_COUNT):
    """
    清理目录中的旧版本文件，保留最近 keep 个 (按文件名中的日期排序)。

    Args:
        directory: 目录路径
        pattern_suffix: 文件名后缀匹配 (如 '.yaml' 或 '.clash.yaml')
        keep: 保留数量 (默认 HISTORY_KEEP_COUNT=5)

    说明:
        - 仅清理日期文件 (匹配 M-D 开头的文件名)，不触碰 latest.* 文件
        - 同一天多次运行只保留最新文件 (同名覆盖, 此处不处理)
        - 按 (月, 日) 降序排列, 删除超出 keep 的最旧文件
    """
    if not os.path.isdir(directory):
        return 0

    def _date_key(fname):
        m = re.match(r'(\d{1,2})-(\d{1,2})\.', fname)
        return (int(m.group(1)), int(m.group(2))) if m else (0, 0)

    candidates = sorted(
        (f for f in os.listdir(directory)
         if f.endswith(pattern_suffix)
         and not f.startswith('latest.')
         and re.match(r'\d{1,2}-\d{1,2}\.', f)),
        key=_date_key,
        reverse=True,
    )

    removed = 0
    for old_file in candidates[keep:]:
        old_path = os.path.join(directory, old_file)
        try:
            os.remove(old_path)
            removed += 1
            logger.debug(f'[history] 清理旧版本: {old_path}')
        except OSError as e:
            logger.warning(f'[history] 清理失败 {old_path}: {e}')

    if removed:
        logger.info(f'[history] {directory} 保留最近 {keep} 版, 清理 {removed} 个旧文件')
    return removed


def generate_merged_format(upstream_texts, upstreams):
    """
    上游订阅融合主入口:
      解析 → 安检 → 去重 → 测速 → 健康更新 → 剔除失效 → 按延迟排序
      → 本地临时文件 → subconverter 转换
      → 独立输出 output/merged/{M-D}.{ext} + latest.{ext} (含回退保护)
    上游节点不进入主订阅，与自采节点物理隔离。
    健康记录嵌入 index.json 的 merged.quality.node_health，跨周期持久化。
    返回融合节点总数 (无可用节点时返回 0)。
    """
    if not upstream_texts:
        logger.warning('无可用上游订阅，跳过融合输出')
        return 0
    if not check_subconverter():
        logger.warning('subconverter 不可用，跳过融合输出')
        return 0

    # 1. 解析 + 安检 + 来源标注
    all_uris, all_clash_proxies = [], []
    uri_source_map = {}    # uri → 上游来源名 (T1.4 单源截断用)
    proxy_source_map = {}  # proxy_name → 上游来源名
    for upstream in upstreams:
        text = upstream_texts.get(upstream.get('name'))
        if not text:
            continue
        source_name = upstream.get('name', 'unknown')
        uris, proxies = parse_upstream_text(text, upstream)
        for u in uris:
            uri_source_map.setdefault(u, source_name)
        for p in proxies:
            proxy_source_map.setdefault(p['name'], source_name)
        all_uris.extend(uris)
        all_clash_proxies.extend(proxies)

    # 2. 节点级去重 (URI 全串去重 / Clash 代理按名称去重)
    all_uris = list(dict.fromkeys(all_uris))
    seen_names = set()
    dedup_proxies = []
    for proxy in all_clash_proxies:
        if proxy['name'] not in seen_names:
            seen_names.add(proxy['name'])
            dedup_proxies.append(proxy)

    total_nodes = len(all_uris) + len(dedup_proxies)
    if total_nodes == 0:
        logger.warning('上游订阅解析后无有效节点，跳过融合输出')
        return 0
    logger.info(
        f'融合节点合计: 分享链接 {len(all_uris)}, Clash 代理 {len(dedup_proxies)}'
    )

    # 2.5 (T1.2) 测速 → 健康更新 → 剔除失效 → 按延迟排序
    #     健康记录跨周期持久化, 连续 LATENCY_FAIL_THRESHOLD 次不可达的节点剔除
    health_data = load_node_health()

    # T2.3: 上游拉取健康记录更新 (连续 UPSTREAM_DEGRADE_THRESHOLD 次失败标记 degraded)
    upstream_health = load_upstream_health()
    today_str = datetime.datetime.today().strftime('%Y-%m-%d')
    successful_names = set(upstream_texts.keys())
    for upstream in upstreams:
        name = upstream.get('name', 'unknown')
        rec = upstream_health.get(name, {
            'fail_count': 0, 'last_success': '', 'degraded': False,
        })
        if name in successful_names:
            rec['fail_count'] = 0
            rec['last_success'] = today_str
            rec['degraded'] = False
        else:
            rec['fail_count'] = rec.get('fail_count', 0) + 1
            if rec['fail_count'] >= UPSTREAM_DEGRADE_THRESHOLD:
                if not rec.get('degraded'):
                    logger.warning(
                        f'[upstream] {name} 连续 {rec["fail_count"]} 次拉取失败, '
                        f'已标记为 degraded'
                    )
                rec['degraded'] = True
        upstream_health[name] = rec

    uri_latencies = measure_uri_latencies(all_uris) if all_uris else {}
    uri_survivors, uri_excluded = filter_and_sort_nodes(
        all_uris, uri_latencies, health_data
    )
    all_uris = uri_survivors

    proxy_latencies = measure_proxy_latencies(dedup_proxies) if dedup_proxies else {}
    proxy_names = [p['name'] for p in dedup_proxies]
    proxy_survivors, proxy_excluded = filter_and_sort_nodes(
        proxy_names, proxy_latencies, health_data
    )
    proxy_by_name = {p['name']: p for p in dedup_proxies}
    dedup_proxies = [proxy_by_name[n] for n in proxy_survivors]

    excluded_count = uri_excluded + proxy_excluded
    post_health_count = len(all_uris) + len(dedup_proxies)

    # T1.5: 延迟统计 (基于剔除后、截断前的可达节点, 反映节点质量而非体积控制)
    _all_lats = []
    for u in all_uris:
        _lat = uri_latencies.get(u)
        if _lat is not None:
            _all_lats.append(_lat)
    for p in dedup_proxies:
        _lat = proxy_latencies.get(p['name'])
        if _lat is not None:
            _all_lats.append(_lat)
    if _all_lats:
        avg_latency = round(sum(_all_lats) / len(_all_lats))
        max_latency = max(_all_lats)
        min_latency = min(_all_lats)
    else:
        avg_latency = max_latency = min_latency = None
    availability_rate = round(post_health_count / max(total_nodes, 1) * 100, 1)

    if excluded_count:
        logger.info(
            f'[health] 已剔除 {excluded_count} 个连续不可达节点, '
            f'存活 {post_health_count}/{total_nodes} (可用率 {availability_rate}%)'
        )
    if post_health_count == 0:
        logger.warning('所有融合节点均不可达，跳过融合输出')
        return 0

    # 2.8 (T1.4) 单源截断 + 总量截断
    #   单源: 每个上游按 max_nodes 截断 (列表已按延迟升序, 取前 N)
    #   总量: URI + Clash proxy 合计 ≤ MERGED_MAX_NODES (合并按延迟升序取前 N)
    from collections import defaultdict
    source_max_map = {
        u.get('name', 'unknown'): int(u.get('max_nodes', 0) or 0)
        for u in upstreams
    }

    # 单源截断: URI
    uri_groups = defaultdict(list)
    for u in all_uris:
        uri_groups[uri_source_map.get(u, 'unknown')].append(u)
    uri_truncated = []
    for src, items in uri_groups.items():
        cap = source_max_map.get(src, 0)
        if cap > 0 and len(items) > cap:
            items = items[:cap]
        uri_truncated.extend(items)
    all_uris = uri_truncated

    # 单源截断: Clash proxy
    proxy_groups = defaultdict(list)
    for p in dedup_proxies:
        proxy_groups[proxy_source_map.get(p['name'], 'unknown')].append(p)
    proxy_truncated = []
    for src, items in proxy_groups.items():
        cap = source_max_map.get(src, 0)
        if cap > 0 and len(items) > cap:
            items = items[:cap]
        proxy_truncated.extend(items)
    dedup_proxies = proxy_truncated

    # 单源截断后重新全局排序 (分组截断打乱了全局延迟顺序)
    all_uris.sort(key=lambda u: (
        1 if uri_latencies.get(u) is None else 0,
        uri_latencies.get(u) if uri_latencies.get(u) is not None else float('inf'),
    ))
    dedup_proxies.sort(key=lambda p: (
        1 if proxy_latencies.get(p['name']) is None else 0,
        proxy_latencies.get(p['name']) if proxy_latencies.get(p['name']) is not None else float('inf'),
    ))

    # 总量截断: 合并 URI + proxy 按延迟升序取前 MERGED_MAX_NODES
    total_after_source = len(all_uris) + len(dedup_proxies)
    if total_after_source > MERGED_MAX_NODES:
        tagged = []
        for u in all_uris:
            lat = uri_latencies.get(u)
            tagged.append((lat if lat is not None else float('inf'), 0, u))
        for p in dedup_proxies:
            lat = proxy_latencies.get(p['name'])
            tagged.append((lat if lat is not None else float('inf'), 1, p))
        tagged.sort(key=lambda x: (x[0], x[1]))
        tagged = tagged[:MERGED_MAX_NODES]
        all_uris = [item for _, kind, item in tagged if kind == 0]
        dedup_proxies = [item for _, kind, item in tagged if kind == 1]

    truncated_count = total_after_source - len(all_uris) - len(dedup_proxies)
    available_count = len(all_uris) + len(dedup_proxies)
    if truncated_count:
        logger.info(
            f'[T1.4] 体积截断 {truncated_count} 个节点 '
            f'(单源上限 + 总量 {MERGED_MAX_NODES}), 最终输出 {available_count}'
        )

    # 3. 写入本地临时文件并通过本地 HTTP 服务提供给 subconverter
    tmp_dir = tempfile.mkdtemp(prefix='nodecollection_upstream_')
    server = None
    try:
        local_files = []
        if all_uris:
            nodes_file = os.path.join(tmp_dir, 'upstream_nodes.txt')
            with open(nodes_file, 'w', encoding='utf-8') as f:
                f.write(base64.b64encode(
                    '\n'.join(all_uris).encode('utf-8')
                ).decode('utf-8'))
            local_files.append(nodes_file)
        if dedup_proxies:
            clash_file = os.path.join(tmp_dir, 'upstream_clash.yaml')
            with open(clash_file, 'w', encoding='utf-8') as f:
                yaml.safe_dump(
                    {'proxies': dedup_proxies}, f,
                    allow_unicode=True, sort_keys=False,
                )
            local_files.append(clash_file)

        server, local_sub_urls = _serve_local_files(local_files)

        # 4. 多格式转换 → 独立输出 output/merged/
        #    文件名带格式 token (clash/v2ray/surge/mixed) 防止同扩展名互相覆盖
        #    (v2ray 与 mixed 均为 .txt，不带 token 会发生 latest.txt 覆盖冲突)
        today = datetime.datetime.today()
        for target, _, ext in OUTPUT_FORMATS:
            token = target.split('&')[0]
            date_fname = f'{today.month}-{today.day}.{token}.{ext}'
            output_path = os.path.join(OUTPUT_DIR, MERGED_DIR, date_fname)
            os.makedirs(os.path.join(OUTPUT_DIR, MERGED_DIR), exist_ok=True)
            # 注意: subconverter 的 target 必须是格式名 (clash/v2ray/...),
            # 不能带 'merged/' 路径前缀; 输出目录已由 MERGED_DIR 控制
            # T1.3: 融合订阅使用独立配置 merged_config.ini (含韩英分组)
            success = call_subconverter(
                target, local_sub_urls, output_path,
                config_path=SUBCONVERTER_MERGED_CONFIG,
            )

            latest_fname = f'latest.{token}.{ext}'
            latest_path = os.path.join(OUTPUT_DIR, MERGED_DIR, latest_fname)
            if success:
                shutil.copy2(output_path, latest_path)
                logger.info(f'[merged/{target}] 固定链接已更新 → {latest_path}')
            else:
                _ensure_merged_latest_fallback(token, latest_fname)

        # T2.2: 融合订阅历史版本清理 (按格式 token 分组, 每组保留最近 5 版)
        for target, _, ext in OUTPUT_FORMATS:
            token = target.split('&')[0]
            cleanup_old_versions(
                os.path.join(OUTPUT_DIR, MERGED_DIR),
                f'.{token}.{ext}',
            )

        # 5. 更新 index.json 增加 merged 段
        index_path = os.path.join(OUTPUT_DIR, 'index.json')
        index_data = {}
        if os.path.isfile(index_path):
            try:
                with open(index_path, encoding='utf-8') as f:
                    index_data = json.load(f)
            except Exception:
                index_data = {}
        merged_section = {
            'date': f'{today.year}/{today.month}/{today.month}-{today.day}',
            'formats': {},
            'latest': {},
            'total_nodes': total_nodes,
            'sources': sorted(upstream_texts.keys()),
            # T2.3: 上游拉取健康记录 (连续失败计数 + 降级标记)
            'upstream_health': upstream_health,
            # T1.2+T1.4+T1.5 质量指标: 解析数/存活数/剔除数/截断数/可用率/延迟统计/健康记录
            'quality': {
                'total_parsed': total_nodes,
                'total_available': post_health_count,   # 剔除后存活 (质量指标, 不含体积截断)
                'excluded': excluded_count,             # 连续不可达剔除
                'truncated': truncated_count,           # T1.4 体积截断
                'output_count': available_count,        # 最终输出数 (截断后)
                'availability_rate': availability_rate, # 可用率 %
                'avg_latency_ms': avg_latency,
                'max_latency_ms': max_latency,
                'min_latency_ms': min_latency,
                'node_health': health_data,
            },
        }
        for target, _, ext in OUTPUT_FORMATS:
            token = target.split('&')[0]
            merged_section['formats'][token] = \
                f'{MERGED_DIR}/{today.month}-{today.day}.{token}.{ext}'
            merged_section['latest'][token] = f'{MERGED_DIR}/latest.{token}.{ext}'
        index_data['merged'] = merged_section
        with open(index_path, 'w', encoding='utf-8') as f:
            json.dump(index_data, f, ensure_ascii=False, indent=2)
        logger.info(f'索引文件已更新 (含 merged 段): {index_path}')

        return available_count
    finally:
        if server:
            server.shutdown()
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ============================================================
# 订阅校验与分类
# ============================================================

def classify_subscription(res):
    """
    根据响应内容分类订阅类型。
    返回: (类型字符串, 信息字符串)
      类型: 'sub' | 'clash' | 'v2' | None
    """
    # 1. 检查 subscription-userinfo 流量信息（机场订阅）
    user_info = res.headers.get('subscription-userinfo')
    if user_info:
        info_nums = re.findall(r'\d+', user_info)
        if len(info_nums) >= 3:
            upload = int(info_nums[0])
            download = int(info_nums[1])
            total = int(info_nums[2])
            unused = (total - upload - download) / 1024 / 1024 / 1024
            unused_rounded = round(unused, 2)
            if unused_rounded > 0:
                return 'sub', f'可用流量: {unused_rounded} GB                    {res.url}'

    # 2. 检查 clash 格式
    if 'proxies:' in res.text:
        return 'clash', None

    # 3. 检查 v2 格式（base64 解码后含协议头）
    try:
        decoded = base64.b64decode(res.text[:64])
        decoded_str = str(decoded)
        if any(prefix in decoded_str for prefix in PROTOCOL_PREFIXES):
            return 'v2', None
    except Exception:
        pass

    return None, None


def sub_check(session, url):
    """校验单个 URL 是否为有效订阅链接。"""
    headers = {'User-Agent': USER_AGENT}

    @retry(tries=RETRY_TIMES, exceptions=requests.RequestException)
    def _do_check():
        return session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

    try:
        res = _do_check()
    except (requests.Timeout, requests.ConnectionError, requests.RequestException):
        return {'type': None, 'url': url, 'info': None}

    if res.status_code != 200:
        return {'type': None, 'url': url, 'info': None}

    sub_type, info = classify_subscription(res)
    return {'type': sub_type, 'url': url, 'info': info}


def check_all_urls(session, url_list):
    """多线程并发校验所有 URL。"""
    results = {'sub': [], 'clash': [], 'v2': [], 'play': []}
    total = len(url_list)
    if total == 0:
        logger.warning('URL 列表为空，跳过校验')
        return results

    bar = tqdm(total=total, desc='订阅筛选')
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(sub_check, session, url): url for url in url_list}
        for future in as_completed(futures):
            result = future.result()
            sub_type = result['type']
            if sub_type == 'sub':
                results['sub'].append(result['url'])
                if result['info']:
                    results['play'].append(result['info'])
            elif sub_type == 'clash':
                results['clash'].append(result['url'])
            elif sub_type == 'v2':
                results['v2'].append(result['url'])
            bar.update(1)
    bar.close()
    return results


# ============================================================
# subconverter 集成 (Phase 1 核心)
# ============================================================

def check_subconverter():
    """检查 subconverter 服务是否可用。"""
    try:
        resp = requests.get(
            f'{SUBCONVERTER_URL}/version',
            timeout=5
        )
        if resp.status_code == 200:
            logger.info(f'subconverter 服务可用: {resp.text.strip()[:50]}')
            return True
    except Exception:
        pass
    logger.warning('subconverter 服务不可用，跳过多格式转换 '
                   '(本地运行请先执行: bash update.sh local)')
    return False


def call_subconverter(target, sub_urls, output_path, config_path=None):
    """
    调用 subconverter API 转换订阅格式。

    Args:
        target: 目标格式 (clash, v2ray, surge&ver=4, mixed 等)
        sub_urls: 订阅 URL 列表
        output_path: 输出文件路径
        config_path: subconverter 外部配置文件路径 (None 时使用默认 SUBCONVERTER_EXTERNAL_CONFIG)
    """
    if not sub_urls:
        logger.warning(f'[{target}] 订阅 URL 为空，跳过')
        return False

    # 用 | 合并多个订阅 URL，然后 URL-encode
    merged_url = '|'.join(sub_urls)
    encoded_url = quote(merged_url, safe='')

    # 构造外部配置路径 (使用绝对路径)
    cfg = config_path or SUBCONVERTER_EXTERNAL_CONFIG
    abs_config = os.path.abspath(cfg)
    encoded_config = quote(f'file://{abs_config}', safe='')

    api_url = (
        f'{SUBCONVERTER_URL}/sub?'
        f'target={target}&'
        f'url={encoded_url}&'
        f'config={encoded_config}&'
        f'emoji=true&'
        f'udp=true&'
        f'tfo=false&'
        f'expand=true&'
        f'append_info=true&'
        f'sort=false'
    )

    for attempt in range(1, SUBCONVERTER_RETRIES + 1):
        try:
            resp = requests.get(api_url, timeout=SUBCONVERTER_TIMEOUT)
            if resp.status_code == 200 and resp.text.strip():
                # 确保输出目录存在
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(resp.text)
                logger.info(f'[{target}] 转换成功 → {output_path} ({len(resp.text)} bytes)')
                return True
            logger.warning(f'[{target}] 第 {attempt}/{SUBCONVERTER_RETRIES} 次转换失败: HTTP {resp.status_code}')
        except requests.Timeout:
            logger.warning(f'[{target}] 第 {attempt}/{SUBCONVERTER_RETRIES} 次请求超时 ({SUBCONVERTER_TIMEOUT}s)')
        except Exception as e:
            logger.warning(f'[{target}] 第 {attempt}/{SUBCONVERTER_RETRIES} 次异常: {type(e).__name__}: {e}')

        if attempt < SUBCONVERTER_RETRIES:
            time.sleep(SUBCONVERTER_RETRY_DELAY)

    logger.error(f'[{target}] 连续 {SUBCONVERTER_RETRIES} 次转换失败，已跳过')
    return False


def generate_multi_format(all_sub_urls):
    """
    将所有订阅 URL 通过 subconverter 转换为多格式输出。

    Args:
        all_sub_urls: 所有有效订阅 URL 的列表
    """
    if not check_subconverter():
        logger.warning('subconverter 不可用，仅输出原始 YAML')
        return

    today = datetime.datetime.today()
    date_str = f'{today.year}/{today.month}/{today.month}-{today.day}'

    for target, subdir, ext in OUTPUT_FORMATS:
        date_fname = f'{today.month}-{today.day}.{ext}'
        output_path = os.path.join(OUTPUT_DIR, subdir, date_fname)
        success = call_subconverter(target, all_sub_urls, output_path)

        # 同时写入 latest 固定文件 (URL 永不改变，内容随每次运行更新)
        latest_path = os.path.join(OUTPUT_DIR, subdir, f'latest.{ext}')
        if success:
            shutil.copy2(output_path, latest_path)
            logger.info(f'[{target}] 固定链接已更新 → {latest_path}')
        elif not os.path.exists(latest_path):
            # 转换失败且 latest 文件不存在时，回退复制最近的历史日期文件，
            # 保证固定链接永不 404 (内容可能是较早的快照)
            # 注意: CI 中 git checkout 会重置 mtime，因此按文件名中的日期排序
            def _date_key(fname):
                m = re.match(r'(\d{1,2})-(\d{1,2})\.', fname)
                return (int(m.group(1)), int(m.group(2))) if m else (0, 0)

            subdir_path = os.path.join(OUTPUT_DIR, subdir)
            if not os.path.isdir(subdir_path):
                os.makedirs(subdir_path, exist_ok=True)
                continue  # 目录尚未创建 (从未成功转换过)，无历史文件可回退
            candidates = sorted(
                (f for f in os.listdir(subdir_path)
                 if f.endswith(f'.{ext}') and f != f'latest.{ext}'),
                key=_date_key,
                reverse=True,
            )
            if candidates:
                fallback = os.path.join(subdir_path, candidates[0])
                shutil.copy2(fallback, latest_path)
                logger.warning(
                    f'[{target}] 本次转换失败，latest 已回退为历史文件: {candidates[0]}'
                )

    # T2.2: 历史版本清理 (每格式保留最近 HISTORY_KEEP_COUNT 版)
    for target, subdir, ext in OUTPUT_FORMATS:
        cleanup_old_versions(
            os.path.join(OUTPUT_DIR, subdir),
            f'.{ext}',
        )

    # 额外: 生成一个合并所有格式的 index.json 索引文件
    index_path = os.path.join(OUTPUT_DIR, 'index.json')
    index_data = {
        'update_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'date': date_str,
        'formats': {},
        'latest': {},
        'total_urls': len(all_sub_urls),
    }
    for target, subdir, ext in OUTPUT_FORMATS:
        date_fname = f'{today.month}-{today.day}.{ext}'
        index_data['formats'][target] = f'{subdir}/{date_fname}'
        index_data['latest'][target] = f'{subdir}/latest.{ext}'

    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(index_data, f, ensure_ascii=False, indent=2)
    logger.info(f'索引文件: {index_path}')


# ============================================================
# README 自动生成 (每次运行后更新订阅链接)
# ============================================================

def _append_proxy_table(lines, file_path):
    """向 lines 追加某个订阅文件的加速代理链接表格。"""
    lines.append('| 加速方式 | 订阅地址 |')
    lines.append('| :--- | :--- |')

    for proxy_name, proxy_template in PROXY_PREFIXES:
        raw_url = f'{RAW_BASE}/{file_path}'
        if '{path}' in proxy_template:
            # kkgithub / jsdelivr: 替换域名方式
            proxy_url = proxy_template.format(
                GITHUB_OWNER=GITHUB_OWNER,
                GITHUB_REPO=GITHUB_REPO,
                GITHUB_BRANCH=GITHUB_BRANCH,
                path=f'/{file_path}',
            )
        else:
            # ghproxy: 加前缀方式
            proxy_url = proxy_template.format(url=raw_url)

        lines.append(f'| {proxy_name} | `{proxy_url}` |')


def _append_sub_section(lines, display_name, file_path, note, github_url=None):
    """向 lines 追加一个订阅小节: 标题 + 说明 + 加速代理表格。"""
    # 标题渲染: 有 github_url 则包成 [显示名](github_url) 超链接，否则保留原样
    if github_url:
        lines.append(f'### [{display_name}]({github_url})')
    else:
        lines.append(f'### {display_name}')
    lines.append('')
    lines.append(f'<sub>{note}</sub>')
    lines.append('')
    _append_proxy_table(lines, file_path)
    lines.append('')
    lines.append('---')
    lines.append('')


# 上游订阅来源的 GitHub 仓库映射 (用于 README 来源表致谢)
UPSTREAM_REPO_MAP = {
    'freenode': ('ripaojiedian/freenode', 'https://github.com/ripaojiedian/freenode'),
    'NoMoreWalls': ('peasoft/NoMoreWalls', 'https://github.com/peasoft/NoMoreWalls'),
    'FreeNodes': ('Barabama/FreeNodes', 'https://github.com/Barabama/FreeNodes'),
}


def generate_readme(upstreams=None):
    """
    生成 README.md，仅包含最新订阅链接。
    使用 latest 固定路径，URL 永不改变，内容随每次运行自动更新。
    包含原生链接 + 多种加速代理前缀。
    upstreams: 启用的上游白名单 (merge.yaml)，用于渲染融合订阅段落与来源表。
    """
    update_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 订阅文件路径定义: (显示名, 文件路径, 格式说明, GitHub 项目链接)
    # 使用 latest 固定路径，URL 永不改变，内容随每次运行自动更新
    # 末尾的链接字段让各软件标题成为可点击的超链接，跳转到对应 GitHub 仓库
    sub_files = [
        ('Clash', 'output/clash/latest.yaml', 'Clash / Clash Meta / Mihomo',
         'https://github.com/clash-verge-rev/clash-verge-rev'),
        ('V2Ray', 'output/v2ray/latest.txt', 'V2RayN / V2RayNG / Shadowrocket (Base64)',
         'https://github.com/2dust/v2rayN'),
        ('Surge', 'output/surge/latest.conf', 'Surge 4+',
         None),
        ('Mixed', 'output/mixed/latest.txt', '混合格式 Base64 (全协议)',
         None),
        # T2.1 (v1.6.0): 新增 Sing-box 输出
        ('Sing-box', 'output/singbox/latest.json', 'Sing-box / SagerNet / Hiddify (JSON)',
         'https://github.com/SagerNet/sing-box'),
        ('原始 YAML', 'sub/latest.yaml', '向后兼容格式 (含分类)',
         None),
    ]

    # 融合订阅文件 (上游外部来源，节点名带 [ext:来源] 前缀)
    # 文件名带格式 token，与 generate_merged_format 的输出命名保持一致
    merged_files = [
        ('Clash', 'output/merged/latest.clash.yaml', '融合节点 (含外部来源标注)'),
        ('V2Ray', 'output/merged/latest.v2ray.txt', '融合节点 Base64'),
        ('Surge', 'output/merged/latest.surge.conf', '融合节点 Surge 配置'),
        ('Mixed', 'output/merged/latest.mixed.txt', '融合节点混合 Base64'),
        # T2.1 (v1.6.0): 新增 Sing-box 融合输出
        ('Sing-box', 'output/merged/latest.singbox.json', '融合节点 Sing-box JSON'),
    ]

    lines = []
    lines.append('# NodeCollection')
    lines.append('')
    lines.append(f'> 自动更新时间: {update_time}')
    lines.append('')
    lines.append('> ⚠️ **免责声明**：本项目所有节点均来自互联网公开资源，仅供学习与交流使用，'
                 '不保证节点的安全性、可用性与合法性。请勿用于任何违反所在地区法律法规的用途，'
                 '也请勿通过免费节点登录银行、邮箱等敏感账号。使用本项目产生的一切后果由使用者自行承担。')
    lines.append('')
    lines.append('## 订阅链接')
    lines.append('')
    lines.append('复制下方链接到客户端的订阅地址中即可使用。各软件标题为超链接，点击可跳转到对应 GitHub 仓库。')
    lines.append('')
    lines.append('---')
    lines.append('')

    for display_name, file_path, note, github_url in sub_files:
        _append_sub_section(lines, display_name, file_path, note, github_url)

    # 融合订阅段落 (仅当上游白名单非空时渲染)
    if upstreams:
        lines.append('## 融合订阅（外部来源）')
        lines.append('')
        lines.append('以下订阅融合了外部优质开源项目的公开免费节点，'
                     '节点名带 `[ext:来源]` 前缀便于溯源。与主订阅相互独立，任选其一使用即可。')
        lines.append('')
        lines.append('---')
        lines.append('')

        for display_name, file_path, note in merged_files:
            _append_sub_section(lines, display_name, file_path, note)

        # T1.3: 融合订阅节点分组说明
        lines.append('### 节点分组')
        lines.append('')
        lines.append('融合订阅（Clash 格式）按地区自动分组，支持以下代理组：')
        lines.append('')
        lines.append('| 代理组 | 匹配规则 |')
        lines.append('| :--- | :--- |')
        lines.append('| 🚀 节点选择 | 手动选择，含所有地区组 + 直连 |')
        lines.append('| ♻️ 自动选择 | 全部节点 URL 测速，自动选最优 |')
        lines.append('| 🇭🇰 香港节点 | 香港 / HK / Hong Kong |')
        lines.append('| 🇹🇼 台湾节点 | 台湾 / TW / Taiwan |')
        lines.append('| 🇯🇵 日本节点 | 日本 / JP / Japan |')
        lines.append('| 🇸🇬 新加坡节点 | 新加坡 / SG / Singapore |')
        lines.append('| 🇺🇸 美国节点 | 美国 / US / United States |')
        lines.append('| 🇰🇷 韩国节点 | 韩国 / KR / Korea |')
        lines.append('| 🇬🇧 英国节点 | 英国 / UK / United Kingdom |')
        lines.append('| 🔗 故障转移 | 全部节点故障转移 |')
        lines.append('| ⚖️ 负载均衡 | 全部节点负载均衡 |')
        lines.append('')
        lines.append('> 节点名带 `[ext:来源]` 前缀，地区识别基于节点名称中的地区关键词。')
        lines.append('')

        lines.append('### 上游来源 (Thanks)')
        lines.append('')
        # T2.3: 从 index.json 读取上游健康状态, 降级的来源显示 ⚠️ 标记
        upstream_health = {}
        _idx_path = os.path.join(OUTPUT_DIR, 'index.json')
        if os.path.isfile(_idx_path):
            try:
                with open(_idx_path, encoding='utf-8') as _f:
                    _idx_data = json.load(_f)
                upstream_health = _idx_data.get('merged', {}).get('upstream_health', {})
            except Exception:
                pass

        lines.append('| 来源 | 项目地址 | 状态 |')
        lines.append('| :--- | :--- | :--- |')
        seen = set()
        for upstream in upstreams:
            name = upstream.get('name', '')
            if not name or name in seen:
                continue
            seen.add(name)
            # T2.4: Credits 自动同步, 优先级: merge.yaml repo 字段 > URL 提取 > UPSTREAM_REPO_MAP 回退
            repo_field = upstream.get('repo', '').strip()
            if repo_field:
                repo = repo_field
                repo_url = f'https://github.com/{repo_field}'
            else:
                # 从 raw.githubusercontent.com URL 自动提取仓库路径
                src_url = upstream.get('url', '')
                try:
                    parsed = urlparse(src_url)
                    if parsed.hostname == RAW_GITHUB_HOST:
                        parts = parsed.path.strip('/').split('/')
                        if len(parts) >= 2:
                            repo = f'{parts[0]}/{parts[1]}'
                            repo_url = f'https://github.com/{repo}'
                        else:
                            repo, repo_url = UPSTREAM_REPO_MAP.get(name, (name, src_url))
                    else:
                        repo, repo_url = UPSTREAM_REPO_MAP.get(name, (name, src_url))
                except Exception:
                    repo, repo_url = UPSTREAM_REPO_MAP.get(name, (name, src_url))
            # T2.3: 降级状态显示
            health = upstream_health.get(name, {})
            if health.get('degraded'):
                fail_cnt = health.get('fail_count', 0)
                status = f'⚠️ 已降级 (连续 {fail_cnt} 次失败)'
            elif health.get('fail_count', 0) > 0:
                status = f'⚠️ 异常 ({health["fail_count"]} 次失败)'
            else:
                status = '✅ 正常'
            lines.append(f'| {name} | [{repo}]({repo_url}) | {status} |')
        lines.append('')
        lines.append('上游节点遵循各来源项目的许可证与分发要求，如来源项目提出异议将立即移除。')
        lines.append('')

    lines.append('## 说明')
    lines.append('')
    lines.append(f'- 每 4 小时自动更新一次 (GitHub Actions)')
    lines.append(f'- 订阅链接为固定地址，复制一次即可长期使用，内容随自动更新刷新')
    lines.append(f'- 当前更新时间: `{update_time}`')
    lines.append(f'- 加速方式按实时性排序: kkgithub/ghproxy 实时更新, jsdelivr 有缓存延迟')
    lines.append(f'- 如某加速节点不可用, 换一个即可')
    lines.append('')
    lines.append('> 项目详细信息请参阅 [ABOUT.md](ABOUT.md)')
    lines.append('')

    readme_path = 'README.md'
    with open(readme_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    logger.info(f'README.md 已更新: {readme_path}')


# ============================================================
# 主流程
# ============================================================

def main():
    start_time = time.time()

    # 1. 初始化目录
    path_yaml = pre_check()

    # 2. 读取已有数据
    dict_url = yaml_check(path_yaml)

    # 3. 读取频道配置
    channel_urls = get_config()
    if not channel_urls:
        logger.error('config.yaml 中未找到有效频道，程序退出')
        sys.exit(1)

    # 3.5 读取机场列表
    airports = load_airports()

    # 4. 创建 HTTP Session
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=MAX_THREADS,
        pool_maxsize=MAX_THREADS,
    )
    session.mount('https://', adapter)
    session.mount('http://', adapter)

    # 5. 并发爬取 TG 频道
    logger.info('=== 开始爬取 TG 频道 ===')
    tg_urls = crawl_all_channels(session, channel_urls)

    # 5.5 探测机场公开订阅 (Phase 1)
    logger.info('=== 开始探测机场列表 ===')
    airport_urls = probe_all_airports(session, airports)

    # 6. 合并所有 URL 并校验
    all_urls = list(set(tg_urls + airport_urls))
    logger.info(f'=== 开始校验订阅 (共 {len(all_urls)} 个 URL) ===')
    new_results = check_all_urls(session, all_urls)

    # 7. 合并旧数据 + 去重
    new_sub_list = list(set(new_results['sub'] + dict_url.get('机场订阅', [])))
    new_clash_list = list(set(new_results['clash'] + dict_url.get('clash订阅', [])))
    new_v2_list = list(set(new_results['v2'] + dict_url.get('v2订阅', [])))
    play_list = list(set(new_results['play'] + dict_url.get('开心玩耍', [])))

    # 8. 写入原始 YAML (向后兼容)
    dict_url.update({
        '机场订阅': new_sub_list,
        'clash订阅': new_clash_list,
        'v2订阅': new_v2_list,
        '开心玩耍': play_list,
    })
    yaml_save(path_yaml, dict_url)

    # 8.5 同时写入固定路径 sub/latest.yaml (URL 永不改变)
    latest_yaml_path = os.path.join(SUB_DIR, 'latest.yaml')
    shutil.copy2(path_yaml, latest_yaml_path)
    logger.info(f'固定链接已更新 → {latest_yaml_path}')

    # 9. subconverter 多格式转换
    logger.info('=== subconverter 多格式转换 ===')
    all_sub_urls = new_sub_list + new_clash_list + new_v2_list
    generate_multi_format(all_sub_urls)

    # 9.5 上游订阅融合 (独立输出到 output/merged/，与主订阅隔离)
    logger.info('=== 上游订阅融合 ===')
    upstreams = load_upstreams()
    upstream_texts = fetch_all_upstreams(session, upstreams)
    merged_nodes = generate_merged_format(upstream_texts, upstreams)

    # 10. 输出运行统计
    elapsed = round(time.time() - start_time, 2)

    # T1.5: 从 index.json 读取融合节点质量指标
    merged_quality = {}
    _idx_path = os.path.join(OUTPUT_DIR, 'index.json')
    if os.path.isfile(_idx_path):
        try:
            with open(_idx_path, encoding='utf-8') as _f:
                _idx_data = json.load(_f)
            merged_quality = _idx_data.get('merged', {}).get('quality', {})
        except Exception:
            pass

    logger.info(
        f'=== 运行统计 ===\n'
        f'  耗时: {elapsed}s\n'
        f'  TG频道: {len(channel_urls)}\n'
        f'  机场域名: {len(airports)}\n'
        f'  校验URL: {len(all_urls)}\n'
        f'  机场订阅: {len(new_sub_list)}\n'
        f'  clash订阅: {len(new_clash_list)}\n'
        f'  v2订阅: {len(new_v2_list)}\n'
        f'  流量信息: {len(play_list)}\n'
        f'  多格式输出: {OUTPUT_DIR}/\n'
        f'  上游来源: {len(upstream_texts)}/{len(upstreams)} 个成功\n'
        f'  融合节点: {merged_nodes} → {OUTPUT_DIR}/{MERGED_DIR}/'
    )
    if merged_quality:
        _ar = merged_quality.get('availability_rate')
        _avg = merged_quality.get('avg_latency_ms')
        _exc = merged_quality.get('excluded', 0)
        _trunc = merged_quality.get('truncated', 0)
        logger.info(
            f'  融合质量: 可用率 {_ar}%, 平均延迟 {_avg}ms, '
            f'剔除 {_exc}, 截断 {_trunc}'
        )

    # 11. 自动更新 README.md (订阅链接展示)
    generate_readme(upstreams)

    logger.info('全部任务完成')


if __name__ == '__main__':
    main()
