#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NodeCollection - Telegram 频道订阅源采集工具
整合稳定版 v2.0

原项目: https://github.com/huiwin/collectSub-google
 Fork: https://github.com/RenaLio/proxy-minging/

功能: 爬取 Telegram 频道公开页面，提取并校验代理订阅链接，
      按机场/clash/v2分类存储，支持 GitHub Actions 定时自动运行。

修复项:
  P0 - 全局变量多线程无锁写入 → ThreadPoolExecutor + 返回值收集
  P0 - 三层 except:pass 静默吞异常 → 线性分类逻辑 + 具体异常捕获
  P0 - 频道爬取串行无超时 → 并发爬取 + 超时控制
  P0 - URL 无安全校验 → SSRF 防护过滤
  P1 - 无连接复用 → requests.Session 连接池
  P1 - logger.error 字符串拼接 TypeError → f-string 格式化
  P1 - setDaemon 已废弃 → ThreadPoolExecutor 管理
  P1 - 日志无统计 → 运行结束时输出统计摘要
"""

import re
import os
import sys
import time
import base64
import ipaddress
import datetime
import threading
from urllib.parse import urlparse
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
SUB_DIR = 'sub'
MAX_THREADS = 32           # 订阅校验并发线程数
CHANNEL_THREADS = 8        # 频道爬取并发线程数
REQUEST_TIMEOUT = 10       # 单个 URL 校验超时（秒）
CHANNEL_TIMEOUT = 15       # 频道页面请求超时（秒）
RETRY_TIMES = 2            # 校验失败重试次数
USER_AGENT = 'ClashforWindows/0.18.1'
PROTOCOL_PREFIXES = ('ss://', 'ssr://', 'vmess://', 'trojan://')
URL_REGEX = re.compile(
    r'https?://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]'
)

# ============================================================
# 初始化目录结构（原 pre_check.py 功能）
# ============================================================

def pre_check():
    """根据当前日期创建 sub/YYYY/M/ 目录，返回当日输出文件路径。"""
    today = datetime.datetime.today()
    path_year = os.path.join(SUB_DIR, str(today.year))
    path_mon = os.path.join(path_year, str(today.month))
    path_yaml = os.path.join(
        path_mon, f'{today.month}-{today.day}.yaml'
    )

    for directory in (SUB_DIR, path_year, path_mon):
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
    logger.info('读取文件成功')
    return dict_url


def yaml_save(path_yaml, dict_url):
    """将订阅数据写入 YAML 文件。"""
    with open(path_yaml, 'w', encoding='utf-8') as f:
        yaml.dump(dict_url, f, allow_unicode=True)
    logger.info(f'写入文件成功: {path_yaml}')


# ============================================================
# 配置读取
# ============================================================

def get_config():
    """读取 config.yaml 中的 Telegram 频道列表，转换为 t.me/s/ 公开页面格式。"""
    with open(CONFIG_PATH, encoding='UTF-8') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)

    list_tg = data.get('tgchannel', [])
    new_list = []
    for url in list_tg:
        channel_name = url.split('/')[-1].strip()
        if channel_name:
            new_list.append(f'https://t.me/s/{channel_name}')
    logger.info(f'读取配置成功，共 {len(new_list)} 个频道')
    return new_list


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
        pass  # 域名，允许通过

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
    """并发爬取所有 Telegram 频道，返回去重后的 URL 列表。"""
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

    # 安全过滤 + 去重
    safe_urls = [u for u in all_urls if is_safe_url(u)]
    unique_urls = list(set(safe_urls))
    logger.info(
        f'频道爬取完成: 原始 {len(all_urls)} 个 URL, '
        f'安全过滤后 {len(safe_urls)} 个, 去重后 {len(unique_urls)} 个'
    )
    return unique_urls


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

    # 2. 检查 clash 格式（含 proxies: 关键字）
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
    """
    校验单个 URL 是否为有效订阅链接。

    返回: dict {'type': 'sub'|'clash'|'v2'|None, 'url': str, 'info': str|None}
    """
    headers = {'User-Agent': USER_AGENT}

    @retry(tries=RETRY_TIMES, exceptions=requests.RequestException)
    def _do_check():
        return session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

    try:
        res = _do_check()
    except requests.Timeout:
        return {'type': None, 'url': url, 'info': None}
    except requests.ConnectionError:
        return {'type': None, 'url': url, 'info': None}
    except requests.RequestException:
        return {'type': None, 'url': url, 'info': None}

    if res.status_code != 200:
        return {'type': None, 'url': url, 'info': None}

    sub_type, info = classify_subscription(res)
    return {'type': sub_type, 'url': url, 'info': info}


def check_all_urls(session, url_list):
    """多线程并发校验所有 URL，返回分类结果。"""
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

    # 4. 创建 HTTP Session（连接池复用）
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=MAX_THREADS,
        pool_maxsize=MAX_THREADS,
    )
    session.mount('https://', adapter)
    session.mount('http://', adapter)

    # 5. 并发爬取所有频道
    logger.info('开始爬取频道 ---')
    url_list = crawl_all_channels(session, channel_urls)

    # 6. 多线程校验订阅
    logger.info('开始筛选订阅 ---')
    new_results = check_all_urls(session, url_list)
    logger.info('筛选完成')

    # 7. 合并旧数据 + 去重
    new_sub_list = list(set(new_results['sub'] + dict_url.get('机场订阅', [])))
    new_clash_list = list(set(new_results['clash'] + dict_url.get('clash订阅', [])))
    new_v2_list = list(set(new_results['v2'] + dict_url.get('v2订阅', [])))
    play_list = list(set(new_results['play'] + dict_url.get('开心玩耍', [])))

    # 8. 更新并写入 YAML
    dict_url.update({
        '机场订阅': new_sub_list,
        'clash订阅': new_clash_list,
        'v2订阅': new_v2_list,
        '开心玩耍': play_list,
    })
    yaml_save(path_yaml, dict_url)

    # 9. 输出运行统计
    elapsed = round(time.time() - start_time, 2)
    logger.info(
        f'运行统计 | 耗时: {elapsed}s | '
        f'频道数: {len(channel_urls)} | '
        f'校验URL: {len(url_list)} | '
        f'机场订阅: {len(new_sub_list)} | '
        f'clash订阅: {len(new_clash_list)} | '
        f'v2订阅: {len(new_v2_list)} | '
        f'可用流量信息: {len(play_list)}'
    )
    logger.info('全部任务完成')


if __name__ == '__main__':
    main()
