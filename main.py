#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NodeCollection Pro v1.3.1 - 订阅源采集 + 多格式转换一体化工具

架构:
  config.yaml (TG频道) + airports.yaml (机场列表)
  → 并发爬取频道 + 探测机场公开订阅
  → 校验分类去重
  → 原始 YAML 输出 (向后兼容)
  → subconverter API 多格式转换 (Clash/V2Ray/Surge/SingBox)
  → GitHub Actions 自动提交

新增模块:
  - load_airports(): 加载机场列表
  - probe_airport(): 探测机场公开订阅链接
  - call_subconverter(): 调用 subconverter API 转换格式
  - generate_multi_format(): 生成多格式订阅文件
"""

import re
import os
import sys
import time
import json
import base64
import shutil
import ipaddress
import datetime
from urllib.parse import urlparse, quote
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
SUB_DIR = 'sub'
OUTPUT_DIR = 'output'

MAX_THREADS = 32
CHANNEL_THREADS = 8
AIRPORT_THREADS = 8
REQUEST_TIMEOUT = 10
CHANNEL_TIMEOUT = 15
AIRPORT_TIMEOUT = 8
RETRY_TIMES = 2
USER_AGENT = 'ClashforWindows/0.18.1'
PROTOCOL_PREFIXES = ('ss://', 'ssr://', 'vmess://', 'trojan://')

# subconverter 配置
SUBCONVERTER_URL = os.environ.get('SUBCONVERTER_URL', 'http://127.0.0.1:25500')
SUBCONVERTER_TIMEOUT = 30
SUBCONVERTER_EXTERNAL_CONFIG = 'subconverter/external_config.ini'

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
OUTPUT_FORMATS = [
    ('clash', 'clash', 'yaml'),
    ('v2ray', 'v2ray', 'txt'),
    ('surge&ver=4', 'surge', 'conf'),
    ('mixed', 'mixed', 'txt'),
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
    logger.warning('subconverter 服务不可用，跳过多格式转换')
    return False


def call_subconverter(target, sub_urls, output_path):
    """
    调用 subconverter API 转换订阅格式。

    Args:
        target: 目标格式 (clash, v2ray, surge&ver=4, mixed 等)
        sub_urls: 订阅 URL 列表
        output_path: 输出文件路径
    """
    if not sub_urls:
        logger.warning(f'[{target}] 订阅 URL 为空，跳过')
        return False

    # 用 | 合并多个订阅 URL，然后 URL-encode
    merged_url = '|'.join(sub_urls)
    encoded_url = quote(merged_url, safe='')

    # 构造外部配置路径 (使用绝对路径)
    config_path = os.path.abspath(SUBCONVERTER_EXTERNAL_CONFIG)
    encoded_config = quote(f'file://{config_path}', safe='')

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

    try:
        resp = requests.get(api_url, timeout=SUBCONVERTER_TIMEOUT)
        if resp.status_code == 200 and resp.text.strip():
            # 确保输出目录存在
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(resp.text)
            logger.info(f'[{target}] 转换成功 → {output_path} ({len(resp.text)} bytes)')
            return True
        else:
            logger.warning(f'[{target}] 转换失败: HTTP {resp.status_code}')
            return False
    except requests.Timeout:
        logger.warning(f'[{target}] subconverter 请求超时')
        return False
    except Exception as e:
        logger.warning(f'[{target}] subconverter 异常: {type(e).__name__}: {e}')
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
        if success:
            latest_path = os.path.join(OUTPUT_DIR, subdir, f'latest.{ext}')
            shutil.copy2(output_path, latest_path)
            logger.info(f'[{target}] 固定链接已更新 → {latest_path}')

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

def generate_readme():
    """
    生成 README.md，仅包含最新订阅链接。
    使用 latest 固定路径，URL 永不改变，内容随每次运行自动更新。
    包含原生链接 + 多种加速代理前缀。
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
        ('原始 YAML', 'sub/latest.yaml', '向后兼容格式 (含分类)',
         None),
    ]

    lines = []
    lines.append('# NodeCollection')
    lines.append('')
    lines.append(f'> 自动更新时间: {update_time}')
    lines.append('')
    lines.append('## 订阅链接')
    lines.append('')
    lines.append('复制下方链接到客户端的订阅地址中即可使用。各软件标题为超链接，点击可跳转到对应 GitHub 仓库。')
    lines.append('')
    lines.append('---')
    lines.append('')

    for display_name, file_path, note, github_url in sub_files:
        # 标题渲染: 有 github_url 则包成 [显示名](github_url) 超链接，否则保留原样
        if github_url:
            lines.append(f'### [{display_name}]({github_url})')
        else:
            lines.append(f'### {display_name}')
        lines.append('')
        lines.append(f'<sub>{note}</sub>')
        lines.append('')
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

        lines.append('')
        lines.append('---')
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

    # 10. 输出运行统计
    elapsed = round(time.time() - start_time, 2)
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
        f'  多格式输出: {OUTPUT_DIR}/'
    )

    # 11. 自动更新 README.md (订阅链接展示)
    generate_readme()

    logger.info('全部任务完成')


if __name__ == '__main__':
    main()
