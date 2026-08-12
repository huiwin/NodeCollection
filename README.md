# NodeCollection Pro

Telegram + Airport subscription collector with multi-format output via subconverter.

## Features

- Crawl 34+ Telegram channels for proxy subscription links
- Probe 50+ airport domains for public subscriptions
- Validate and classify subscriptions (airport / clash / v2ray)
- Convert to multiple formats via subconverter:
  - Clash (with ACL4SSR rules, custom proxy groups)
  - V2Ray (base64)
  - Surge 4
  - Mixed (all protocols in base64)
- GitHub Actions auto-run every 4 hours

## Directory Structure

```
main.py                          Main script
config.yaml                      TG channel list
airports.yaml                    Airport domain list
subconverter/
  external_config.ini            subconverter external config (rules, groups, emoji)
.github/workflows/fetch.yaml     GitHub Actions workflow
sub/                             Raw collected subscriptions (YAML, by date)
output/                          Multi-format converted output
  clash/                         Clash format
  v2ray/                         V2Ray base64
  surge/                         Surge config
  mixed/                         Mixed base64
  index.json                     Index of latest outputs
```

## Usage

### Local

1. Download subconverter binary and start it
2. Run: `python main.py`

### GitHub Actions

Automatically runs every 4 hours. No manual intervention needed.

## Integration Architecture

```
config.yaml (TG channels)           airports.yaml (airport domains)
         |                                    |
         v                                    v
   crawl_all_channels()              probe_all_airports()
         |                                    |
         +---------- merge + -----------------+
                       |
                       v
              check_all_urls() (validate + classify)
                       |
            +----------+----------+
            |                     |
            v                     v
     sub/YYYY/M/D.yaml     subconverter API
     (raw, backward        (multi-format convert)
      compatible)                 |
                              v
                    output/clash/ v2ray/ surge/ mixed/
```

## Credits

- Original: [huiwin/collectSub-google](https://github.com/huiwin/collectSub-google)
- subconverter: [tindy2013/subconverter](https://github.com/tindy2013/subconverter)
- Airport list: [moneyfly1/jichangnodes](https://github.com/moneyfly1/jichangnodes)
- Rules: [ACL4SSR](https://github.com/ACL4SSR/ACL4SSR)
