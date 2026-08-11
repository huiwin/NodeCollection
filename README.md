# NodeCollection

Telegram 频道代理订阅源自动采集工具。

## 说明

| 文件 | 说明 |
|------|------|
| `config.yaml` | 爬取源（Telegram 频道列表） |
| `main.py` | 主程序（含目录初始化、频道爬取、订阅校验、分类存储） |
| `requirements.txt` | 依赖包 |
| `.github/workflows/fetch.yaml` | GitHub Actions 定时任务 |

## 使用方式

1. Fork 本仓库
2. 在 GitHub Actions 页面确认 workflow 已启用
3. 默认每 4 小时自动执行一次，也可手动触发（workflow_dispatch）
4. 采集结果存放在 `sub/YYYY/M/M-D.yaml`

## 本地运行

```bash
pip install -r requirements.txt
python main.py
```
