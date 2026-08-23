# NodeCollection

> 自动更新时间: 2026-08-23 20:17:06

> ⚠️ **免责声明**：本项目所有节点均来自互联网公开资源，仅供学习与交流使用，不保证节点的安全性、可用性与合法性。请勿用于任何违反所在地区法律法规的用途，也请勿通过免费节点登录银行、邮箱等敏感账号。使用本项目产生的一切后果由使用者自行承担。

## 节点质量概览

| 指标 | 数值 | 说明 |
| :--- | ---: | :--- |
| 解析节点总数 | 270 | 上游订阅解析后节点数 |
| 可用节点 | 270 | 测速后存活节点数 (剔除前) |
| 最终输出 | 196 | 体积截断后实际输出节点数 |
| 可用率 | 100.0% | 可用节点 / 解析总数 |
| 平均延迟 | 210ms | 可达节点的平均 TCP 握手延迟 |

> 📊 查看详细状态: [Web 状态页](https://huiwin.github.io/NodeCollection/status.html) （含上游贡献统计、质量指标、实时更新）

---

## 订阅链接

复制下方链接到客户端的订阅地址中即可使用。各软件标题为超链接，点击可跳转到对应 GitHub 仓库。

### 格式选择指南

| 格式 | 适用客户端 | 特点 | 协议支持 |
| :--- | :--- | :--- | :--- |
| **Clash** | Clash for Windows / ClashX / Mihomo | 完整配置含代理组+规则，最流行 | SS/SSR/VMess/VLESS/Trojan |
| **V2Ray** | V2RayN / V2RayNG / Shadowrocket | Base64 编码，兼容性最广 | SS/VMess/VLESS/Trojan |
| **Sing-box** | Sing-box / SagerNet / Hiddify | 全协议支持，性能优秀 | 全协议(含 Hysteria2/TUIC) |
| **Surge** | Surge 4+ (iOS/macOS) | 苹果生态原生，功能强大 | SS/VMess/Trojan |
| **Mixed** | 通用客户端 | 所有协议混合 Base64 | 全协议 |

> 不确定选哪个？**Windows/Android 选 Clash**，**iOS 选 Shadowrocket(V2Ray)**，**需要 Hysteria2/TUIC 选 Sing-box**。

---

### [Clash](https://github.com/clash-verge-rev/clash-verge-rev)

<sub>Clash / Clash Meta / Mihomo</sub>

> 支持协议: Shadowsocks, ShadowsocksR, VMess, VLESS, Trojan

| 加速方式 | 订阅地址 |
| :--- | :--- |
| 原生 | `https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/clash/latest.yaml` |
| kkgithub | `https://raw.kkgithub.com/huiwin/NodeCollection/main/output/clash/latest.yaml` |
| ghproxy.net | `https://ghproxy.net/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/clash/latest.yaml` |
| gh-proxy.com | `https://gh-proxy.com/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/clash/latest.yaml` |
| ghfast.top | `https://ghfast.top/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/clash/latest.yaml` |
| jsdelivr | `https://fastly.jsdelivr.net/gh/huiwin/NodeCollection@main/output/clash/latest.yaml` |

---

### [V2Ray](https://github.com/2dust/v2rayN)

<sub>V2RayN / V2RayNG / Shadowrocket (Base64)</sub>

> 支持协议: Shadowsocks, VMess, VLESS, Trojan

| 加速方式 | 订阅地址 |
| :--- | :--- |
| 原生 | `https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/v2ray/latest.txt` |
| kkgithub | `https://raw.kkgithub.com/huiwin/NodeCollection/main/output/v2ray/latest.txt` |
| ghproxy.net | `https://ghproxy.net/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/v2ray/latest.txt` |
| gh-proxy.com | `https://gh-proxy.com/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/v2ray/latest.txt` |
| ghfast.top | `https://ghfast.top/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/v2ray/latest.txt` |
| jsdelivr | `https://fastly.jsdelivr.net/gh/huiwin/NodeCollection@main/output/v2ray/latest.txt` |

---

### [Sing-box](https://github.com/SagerNet/sing-box)

<sub>Sing-box / SagerNet / Hiddify (JSON)</sub>

> 支持协议: Shadowsocks, ShadowsocksR, VMess, VLESS, Trojan, Hysteria, Hysteria2, TUIC

| 加速方式 | 订阅地址 |
| :--- | :--- |
| 原生 | `https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/singbox/latest.json` |
| kkgithub | `https://raw.kkgithub.com/huiwin/NodeCollection/main/output/singbox/latest.json` |
| ghproxy.net | `https://ghproxy.net/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/singbox/latest.json` |
| gh-proxy.com | `https://gh-proxy.com/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/singbox/latest.json` |
| ghfast.top | `https://ghfast.top/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/singbox/latest.json` |
| jsdelivr | `https://fastly.jsdelivr.net/gh/huiwin/NodeCollection@main/output/singbox/latest.json` |

---

### Surge

<sub>Surge 4+</sub>

> 支持协议: Shadowsocks, VMess, Trojan

| 加速方式 | 订阅地址 |
| :--- | :--- |
| 原生 | `https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/surge/latest.conf` |
| kkgithub | `https://raw.kkgithub.com/huiwin/NodeCollection/main/output/surge/latest.conf` |
| ghproxy.net | `https://ghproxy.net/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/surge/latest.conf` |
| gh-proxy.com | `https://gh-proxy.com/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/surge/latest.conf` |
| ghfast.top | `https://ghfast.top/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/surge/latest.conf` |
| jsdelivr | `https://fastly.jsdelivr.net/gh/huiwin/NodeCollection@main/output/surge/latest.conf` |

---

### Mixed

<sub>混合格式 Base64 (全协议)</sub>

> 支持协议: Shadowsocks, ShadowsocksR, VMess, VLESS, Trojan, Hysteria, Hysteria2, TUIC

| 加速方式 | 订阅地址 |
| :--- | :--- |
| 原生 | `https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/mixed/latest.txt` |
| kkgithub | `https://raw.kkgithub.com/huiwin/NodeCollection/main/output/mixed/latest.txt` |
| ghproxy.net | `https://ghproxy.net/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/mixed/latest.txt` |
| gh-proxy.com | `https://gh-proxy.com/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/mixed/latest.txt` |
| ghfast.top | `https://ghfast.top/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/mixed/latest.txt` |
| jsdelivr | `https://fastly.jsdelivr.net/gh/huiwin/NodeCollection@main/output/mixed/latest.txt` |

---

### 原始 YAML

<sub>向后兼容格式 (含分类, 开发者用)</sub>

| 加速方式 | 订阅地址 |
| :--- | :--- |
| 原生 | `https://raw.githubusercontent.com/huiwin/NodeCollection/main/sub/latest.yaml` |
| kkgithub | `https://raw.kkgithub.com/huiwin/NodeCollection/main/sub/latest.yaml` |
| ghproxy.net | `https://ghproxy.net/https://raw.githubusercontent.com/huiwin/NodeCollection/main/sub/latest.yaml` |
| gh-proxy.com | `https://gh-proxy.com/https://raw.githubusercontent.com/huiwin/NodeCollection/main/sub/latest.yaml` |
| ghfast.top | `https://ghfast.top/https://raw.githubusercontent.com/huiwin/NodeCollection/main/sub/latest.yaml` |
| jsdelivr | `https://fastly.jsdelivr.net/gh/huiwin/NodeCollection@main/sub/latest.yaml` |

---

## 融合订阅（外部来源）

以下订阅融合了外部优质开源项目的公开免费节点，节点名带 `[ext:来源]` 前缀便于溯源。与主订阅相互独立，任选其一使用即可。

---

### Clash

<sub>融合节点 (含外部来源标注)</sub>

> 支持协议: Shadowsocks, ShadowsocksR, VMess, VLESS, Trojan

| 加速方式 | 订阅地址 |
| :--- | :--- |
| 原生 | `https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.clash.yaml` |
| kkgithub | `https://raw.kkgithub.com/huiwin/NodeCollection/main/output/merged/latest.clash.yaml` |
| ghproxy.net | `https://ghproxy.net/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.clash.yaml` |
| gh-proxy.com | `https://gh-proxy.com/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.clash.yaml` |
| ghfast.top | `https://ghfast.top/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.clash.yaml` |
| jsdelivr | `https://fastly.jsdelivr.net/gh/huiwin/NodeCollection@main/output/merged/latest.clash.yaml` |

---

### V2Ray

<sub>融合节点 Base64</sub>

> 支持协议: Shadowsocks, VMess, VLESS, Trojan

| 加速方式 | 订阅地址 |
| :--- | :--- |
| 原生 | `https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.v2ray.txt` |
| kkgithub | `https://raw.kkgithub.com/huiwin/NodeCollection/main/output/merged/latest.v2ray.txt` |
| ghproxy.net | `https://ghproxy.net/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.v2ray.txt` |
| gh-proxy.com | `https://gh-proxy.com/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.v2ray.txt` |
| ghfast.top | `https://ghfast.top/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.v2ray.txt` |
| jsdelivr | `https://fastly.jsdelivr.net/gh/huiwin/NodeCollection@main/output/merged/latest.v2ray.txt` |

---

### Sing-box

<sub>融合节点 Sing-box JSON</sub>

> 支持协议: Shadowsocks, ShadowsocksR, VMess, VLESS, Trojan, Hysteria, Hysteria2, TUIC

| 加速方式 | 订阅地址 |
| :--- | :--- |
| 原生 | `https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.singbox.json` |
| kkgithub | `https://raw.kkgithub.com/huiwin/NodeCollection/main/output/merged/latest.singbox.json` |
| ghproxy.net | `https://ghproxy.net/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.singbox.json` |
| gh-proxy.com | `https://gh-proxy.com/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.singbox.json` |
| ghfast.top | `https://ghfast.top/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.singbox.json` |
| jsdelivr | `https://fastly.jsdelivr.net/gh/huiwin/NodeCollection@main/output/merged/latest.singbox.json` |

---

### Surge

<sub>融合节点 Surge 配置</sub>

> 支持协议: Shadowsocks, VMess, Trojan

| 加速方式 | 订阅地址 |
| :--- | :--- |
| 原生 | `https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.surge.conf` |
| kkgithub | `https://raw.kkgithub.com/huiwin/NodeCollection/main/output/merged/latest.surge.conf` |
| ghproxy.net | `https://ghproxy.net/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.surge.conf` |
| gh-proxy.com | `https://gh-proxy.com/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.surge.conf` |
| ghfast.top | `https://ghfast.top/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.surge.conf` |
| jsdelivr | `https://fastly.jsdelivr.net/gh/huiwin/NodeCollection@main/output/merged/latest.surge.conf` |

---

### Mixed

<sub>融合节点混合 Base64</sub>

> 支持协议: Shadowsocks, ShadowsocksR, VMess, VLESS, Trojan, Hysteria, Hysteria2, TUIC

| 加速方式 | 订阅地址 |
| :--- | :--- |
| 原生 | `https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.mixed.txt` |
| kkgithub | `https://raw.kkgithub.com/huiwin/NodeCollection/main/output/merged/latest.mixed.txt` |
| ghproxy.net | `https://ghproxy.net/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.mixed.txt` |
| gh-proxy.com | `https://gh-proxy.com/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.mixed.txt` |
| ghfast.top | `https://ghfast.top/https://raw.githubusercontent.com/huiwin/NodeCollection/main/output/merged/latest.mixed.txt` |
| jsdelivr | `https://fastly.jsdelivr.net/gh/huiwin/NodeCollection@main/output/merged/latest.mixed.txt` |

---

### 节点分组

融合订阅（Clash 格式）按地区自动分组，支持以下代理组：

| 代理组 | 匹配规则 |
| :--- | :--- |
| 🚀 节点选择 | 手动选择，含所有地区组 + 直连 |
| ♻️ 自动选择 | 全部节点 URL 测速，自动选最优 |
| 🇭🇰 香港节点 | 香港 / HK / Hong Kong |
| 🇹🇼 台湾节点 | 台湾 / TW / Taiwan |
| 🇯🇵 日本节点 | 日本 / JP / Japan |
| 🇸🇬 新加坡节点 | 新加坡 / SG / Singapore |
| 🇺🇸 美国节点 | 美国 / US / United States |
| 🇰🇷 韩国节点 | 韩国 / KR / Korea |
| 🇬🇧 英国节点 | 英国 / UK / United Kingdom |
| 🔗 故障转移 | 全部节点故障转移 |
| ⚖️ 负载均衡 | 全部节点负载均衡 |

> 节点名带 `[ext:来源]` 前缀，地区识别基于节点名称中的地区关键词。

### 上游来源 (Thanks)

| 来源 | 项目地址 | 解析 | 去重后 | 可达 | 可用率 | 平均延迟 | 状态 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | :--- |
| NoMoreWalls | [peasoft/NoMoreWalls](https://github.com/peasoft/NoMoreWalls) | 158 | 154 | 44 | 28.6% | 215ms | ✅ 正常 |
| FreeNodes | [Barabama/FreeNodes](https://github.com/Barabama/FreeNodes) | 54 | 47 | 25 | 53.2% | 308ms | ✅ 正常 |
| Pawdroid | [Pawdroid/Free-servers](https://github.com/Pawdroid/Free-servers) | 20 | 18 | 17 | 94.4% | 44ms | ✅ 正常 |
| Jsnzkpg | [Jsnzkpg/Jsnzkpg](https://github.com/Jsnzkpg/Jsnzkpg) | 77 | 51 | 14 | 27.5% | 218ms | ✅ 正常 |

上游节点遵循各来源项目的许可证与分发要求，如来源项目提出异议将立即移除。

## 说明

- 每 4 小时自动更新一次 (GitHub Actions)
- 订阅链接为固定地址，复制一次即可长期使用，内容随自动更新刷新
- 当前更新时间: `2026-08-23 20:17:06`
- 加速方式按实时性排序: kkgithub/ghproxy 实时更新, jsdelivr 有缓存延迟
- 如某加速节点不可用, 换一个即可

> 项目详细信息请参阅 [ABOUT.md](ABOUT.md)
