# 目标
支持在 lighter mainnet 下 maker 单， 在 lighter-rh 下 taker 单

# 实现（已完成）
`hedge/hedge_mode_lighter.py`：对冲模式，maker post-only 单下在 Lighter mainnet，taker IOC 对冲单下在 lighter-rh。

- 启动方式：`python hedge_mode.py --exchange lighter --ticker BTC --size 0.003 --iter 20 --max-position 0.05`
- maker 单距 BBO 偏移由 `LIGHTER_MAKER_OFFSET_BPS` 控制（默认 3 bps），POST_ONLY time-in-force
- taker 单用 MARKET + IMMEDIATE_OR_CANCEL 穿越 rh 的价差对冲
- 双部署凭据按部署隔离：`LIGHTER_MAINNET_*` / `LIGHTER_RH_*`，未设置时回退到共享 `API_KEY_PRIVATE_KEY` / `LIGHTER_ACCOUNT_INDEX` / `LIGHTER_API_KEY_INDEX`
- 账户索引在两个部署不通用（同一钱包 mainnet 与 rh 的 account_index 不同），见 README 获取方法
- 单元测试：`tests/test_hedge_mode_lighter.py`（16 个用例）
