from __future__ import annotations

import datetime as dt
import json
import math
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd
from sqlalchemy import inspect, text

from us_options_polygon import (
    compact_date,
    DEFAULT_UNDERLYINGS as DEFAULT_US_OPTION_UNDERLYINGS,
    dte_for_trade_date,
    get_db_engine,
    get_us_option_chain_daily,
    get_us_underlying_iv_rank,
    normalize_iv_value,
    table_names,
)


DASHBOARD_UNDERLYING_PRIORITY = ("SPY", "QQQ", "DIA", "IWM")
DEFAULT_DASHBOARD_UNDERLYINGS = DASHBOARD_UNDERLYING_PRIORITY + tuple(
    sorted(symbol for symbol in DEFAULT_US_OPTION_UNDERLYINGS if symbol not in DASHBOARD_UNDERLYING_PRIORITY)
)
UNDERLYING_DISPLAY_NAMES = {
    "SPY": "标普500ETF",
    "QQQ": "纳指100ETF",
    "IWM": "罗素2000ETF",
    "GLD": "黄金ETF",
    "TLT": "20年美债ETF",
    "SLV": "白银ETF",
    "XLF": "金融板块ETF",
    "XLE": "能源板块ETF",
    "DIA": "道指ETF",
    "HYG": "高收益债ETF",
    "TSLA": "特斯拉",
    "NVDA": "英伟达",
    "AMD": "超威半导体",
    "AAPL": "苹果",
    "AMZN": "亚马逊",
    "ARM": "Arm",
    "ASML": "阿斯麦",
    "AVGO": "博通",
    "BABA": "阿里巴巴",
    "BAC": "美国银行",
    "COIN": "Coinbase",
    "CRWD": "CrowdStrike",
    "DELL": "戴尔",
    "DIS": "迪士尼",
    "DRAM": "内存ETF",
    "GOOGL": "谷歌",
    "HOOD": "Robinhood",
    "INTC": "英特尔",
    "JPM": "摩根大通",
    "MARA": "Marathon Digital",
    "META": "Meta",
    "MRVL": "Marvell",
    "MSFT": "微软",
    "MSTR": "MicroStrategy",
    "MU": "美光",
    "NFLX": "奈飞",
    "NKE": "耐克",
    "PLTR": "帕兰提尔",
    "ORCL": "甲骨文",
    "QCOM": "高通",
    "RIVN": "Rivian",
    "RKLB": "Rocket Lab",
    "SMCI": "超微电脑",
    "SOFI": "SoFi",
    "SPCX": "SpaceX",
    "TSM": "台积电",
    "UBER": "优步",
    "WMT": "沃尔玛",
}

ETF_EARNINGS_NOTE = "ETF无公司财报"
STOCK_EARNINGS_NOTE = "待日历确认"

UNDERLYING_PROFILE_CARDS = {
    "SPY": {
        "asset_type": "etf",
        "business": "追踪标普500指数，是美股大盘核心 beta，覆盖美国大型龙头公司。",
        "strength": "行业分散、流动性深，常用来观察美股整体风险偏好和机构仓位。",
        "risk": "权重集中在大型科技与消费龙头，估值和利率变化会明显影响表现。",
        "next_earnings_date": ETF_EARNINGS_NOTE,
    },
    "QQQ": {
        "asset_type": "etf",
        "business": "追踪纳斯达克100，偏科技、互联网、半导体和高成长龙头。",
        "strength": "成长属性强，AI、云计算、软件和平台经济权重高。",
        "risk": "对高估值、长久期资产和科技监管更敏感，波动通常高于大盘。",
        "next_earnings_date": ETF_EARNINGS_NOTE,
    },
    "DIA": {
        "asset_type": "etf",
        "business": "追踪道琼斯工业平均指数，偏成熟蓝筹和传统经济龙头。",
        "strength": "成分股盈利相对成熟，适合观察价值蓝筹和工业周期情绪。",
        "risk": "价格加权指数代表性有限，对新经济成长股覆盖不如 SPY/QQQ。",
        "next_earnings_date": ETF_EARNINGS_NOTE,
    },
    "IWM": {
        "asset_type": "etf",
        "business": "追踪罗素2000小盘股，代表美国本土小市值公司风险偏好。",
        "strength": "对降息、信用环境改善和美国内需修复反应更灵敏。",
        "risk": "成分股盈利质量分化大，对融资成本、经济放缓和信用压力敏感。",
        "next_earnings_date": ETF_EARNINGS_NOTE,
    },
    "AAPL": {
        "asset_type": "stock",
        "business": "苹果以 iPhone、Mac、iPad、可穿戴设备和服务生态为核心。",
        "strength": "品牌、硬件生态和服务订阅粘性强，现金流质量高。",
        "risk": "硬件换机周期、供应链集中、监管和中国市场竞争会影响估值。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "AMD": {
        "asset_type": "stock",
        "business": "AMD 提供 CPU、GPU、数据中心加速卡和嵌入式芯片。",
        "strength": "服务器 CPU 份额和 AI 加速卡带来增长弹性，产品线覆盖广。",
        "risk": "AI GPU 生态弱于龙头，半导体周期和客户资本开支波动较大。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "AMZN": {
        "asset_type": "stock",
        "business": "亚马逊覆盖电商、AWS 云、广告、会员和物流基础设施。",
        "strength": "云计算和广告利润率高，零售规模与物流网络形成壁垒。",
        "risk": "零售利润率、云增长放缓、监管和大规模资本开支会压制预期。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "ARM": {
        "asset_type": "stock",
        "business": "Arm 授权芯片架构和 IP，覆盖手机、边缘设备、汽车和服务器。",
        "strength": "授权模式轻资产，生态渗透广，AI 终端和服务器带来增量想象。",
        "risk": "估值弹性大，客户自研、授权费率和终端需求周期会影响增长。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "ASML": {
        "asset_type": "stock",
        "business": "ASML 是先进光刻设备核心供应商，EUV 是先进制程关键环节。",
        "strength": "技术壁垒极高，受益先进制程、AI 芯片和晶圆厂长期投资。",
        "risk": "出口管制、晶圆厂资本开支周期和客户集中度会影响订单节奏。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "AVGO": {
        "asset_type": "stock",
        "business": "博通覆盖网络芯片、定制 ASIC、无线连接和基础设施软件。",
        "strength": "AI 网络和定制芯片需求强，软件业务提高现金流稳定性。",
        "risk": "大客户集中、半导体周期和并购整合节奏是主要观察点。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "BABA": {
        "asset_type": "stock",
        "business": "阿里巴巴覆盖中国电商、云计算、本地生活、物流和国际电商。",
        "strength": "用户和商家生态庞大，云和国际业务仍有结构性机会。",
        "risk": "国内电商竞争、消费复苏斜率、监管和中概股情绪影响较大。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "BAC": {
        "asset_type": "stock",
        "business": "美国银行提供零售银行、财富管理、投行和企业金融服务。",
        "strength": "存款基础深，利率环境改善时净息差弹性明显。",
        "risk": "信用周期、商业地产敞口、收益率曲线和监管资本要求需关注。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "COIN": {
        "asset_type": "stock",
        "business": "Coinbase 是加密资产交易、托管和机构服务平台。",
        "strength": "合规品牌和美国市场入口优势强，交易活跃度提升时业绩弹性大。",
        "risk": "高度受加密资产价格、交易量、监管政策和费用率竞争影响。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "CRWD": {
        "asset_type": "stock",
        "business": "CrowdStrike 提供云原生终端安全、威胁情报和安全运营平台。",
        "strength": "Falcon 平台模块化强，订阅收入和客户扩展能力突出。",
        "risk": "网络安全竞争激烈，重大服务事故或客户预算收缩会冲击估值。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "DELL": {
        "asset_type": "stock",
        "business": "戴尔提供 PC、服务器、存储和企业基础设施解决方案。",
        "strength": "企业渠道强，AI 服务器需求带来收入弹性。",
        "risk": "硬件利润率偏薄，PC 周期、服务器供应链和订单可持续性需跟踪。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "DIS": {
        "asset_type": "stock",
        "business": "迪士尼覆盖影视 IP、主题乐园、体育媒体和流媒体。",
        "strength": "IP 资产和乐园体验稀缺，消费复苏时经营杠杆明显。",
        "risk": "流媒体盈利、内容成本、体育版权和可选消费疲弱是主要压力。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "DRAM": {
        "asset_type": "etf",
        "business": "存储芯片主题标的，主要跟踪 DRAM、NAND、HBM 相关产业链情绪。",
        "strength": "对 AI 服务器、HBM 供需和存储涨价周期敏感，弹性较高。",
        "risk": "存储是强周期行业，价格下行、库存和资本开支扩张会放大波动。",
        "next_earnings_date": ETF_EARNINGS_NOTE,
    },
    "GLD": {
        "asset_type": "etf",
        "business": "黄金 ETF，跟踪黄金现货价格，是贵金属避险和实际利率交易工具。",
        "strength": "适合观察美元、实际利率、央行购金和避险需求变化。",
        "risk": "不产生现金流，对实际利率上行、美元走强和风险偏好修复敏感。",
        "next_earnings_date": ETF_EARNINGS_NOTE,
    },
    "GOOGL": {
        "asset_type": "stock",
        "business": "Alphabet 以搜索广告、YouTube、Google Cloud 和 AI 技术为核心。",
        "strength": "搜索入口、广告数据和云业务规模优势明显，AI 基础能力强。",
        "risk": "AI 搜索替代、反垄断监管、广告周期和云竞争需持续跟踪。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "HOOD": {
        "asset_type": "stock",
        "business": "Robinhood 提供零佣金券商、期权、加密和现金管理服务。",
        "strength": "年轻用户渗透强，交易活跃和利息收入提升时弹性大。",
        "risk": "交易量周期、支付订单流监管、加密波动和信用产品扩张风险较高。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "HYG": {
        "asset_type": "etf",
        "business": "高收益债 ETF，代表美国信用风险偏好和企业融资环境。",
        "strength": "可观察信用利差、违约预期和风险资产流动性。",
        "risk": "经济放缓、利差走阔和降级周期会压制表现。",
        "next_earnings_date": ETF_EARNINGS_NOTE,
    },
    "INTC": {
        "asset_type": "stock",
        "business": "英特尔覆盖 PC/服务器 CPU、晶圆制造和代工业务。",
        "strength": "客户基础和制造资源深厚，政策支持与制程追赶提供反转机会。",
        "risk": "制程执行、代工亏损、AI 竞争和服务器份额压力仍是核心风险。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "JPM": {
        "asset_type": "stock",
        "business": "摩根大通覆盖零售银行、投行、交易、资管和企业金融。",
        "strength": "资产质量和风控行业领先，规模与多元收入来源稳定。",
        "risk": "信用成本、监管资本、利率路径和投行业务周期会影响利润。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "MARA": {
        "asset_type": "stock",
        "business": "Marathon Digital 是比特币矿企，收入与挖矿产量和币价相关。",
        "strength": "比特币上涨时经营和资产负债表弹性大。",
        "risk": "币价、算力难度、能源成本、减半周期和融资稀释风险很高。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "META": {
        "asset_type": "stock",
        "business": "Meta 覆盖 Facebook、Instagram、WhatsApp、广告和 AI/Reality Labs。",
        "strength": "社交流量和广告投放能力强，AI 推荐提升变现效率。",
        "risk": "监管、隐私政策、短视频竞争和元宇宙投入拖累利润。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "MRVL": {
        "asset_type": "stock",
        "business": "Marvell 提供数据中心、网络、存储、汽车和定制芯片方案。",
        "strength": "AI 互连、光模块 DSP 和定制硅方向受益云资本开支。",
        "risk": "订单兑现节奏、客户集中和传统业务周期会放大波动。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "MSFT": {
        "asset_type": "stock",
        "business": "微软覆盖企业软件、Azure 云、Office、Windows、游戏和 AI。",
        "strength": "企业客户粘性强，云和 Copilot 带来长期增长空间。",
        "risk": "AI 资本开支、云竞争、监管和高估值预期管理是关键变量。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "MSTR": {
        "asset_type": "stock",
        "business": "MicroStrategy 同时经营商业智能软件，并持有大量比特币。",
        "strength": "提供带杠杆特征的比特币敞口，资本市场关注度高。",
        "risk": "股价高度依赖比特币、融资结构、溢价收敛和波动率变化。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "MU": {
        "asset_type": "stock",
        "business": "美光生产 DRAM、NAND 和 HBM 存储芯片。",
        "strength": "HBM 和 AI 服务器需求改善时盈利弹性大。",
        "risk": "存储价格周期、库存、资本开支和供需错配会显著影响利润。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "NFLX": {
        "asset_type": "stock",
        "business": "Netflix 是全球流媒体平台，收入来自订阅和广告套餐。",
        "strength": "全球内容分发规模大，用户付费和广告层级提供增长空间。",
        "risk": "用户增长放缓、内容成本、汇率和竞争平台投入会影响估值。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "NKE": {
        "asset_type": "stock",
        "business": "耐克销售运动鞋服、装备，并经营 DTC 与批发渠道。",
        "strength": "全球品牌力强，产品创新和渠道优化可改善利润。",
        "risk": "库存、北美与中国需求、竞争品牌和营销投入效率需关注。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "NVDA": {
        "asset_type": "stock",
        "business": "英伟达提供 GPU、AI 加速平台、网络和软件生态。",
        "strength": "CUDA 生态、数据中心 GPU 和 AI 基础设施需求形成强壁垒。",
        "risk": "大客户集中、供给周期、出口限制和高增长预期回落会放大波动。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "ORCL": {
        "asset_type": "stock",
        "business": "甲骨文提供数据库、企业软件、云基础设施和 SaaS 应用。",
        "strength": "数据库客户粘性强，OCI 和 AI 云订单提升增长预期。",
        "risk": "云资本开支、迁移节奏、竞争和债务水平是主要观察点。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "PLTR": {
        "asset_type": "stock",
        "business": "Palantir 提供政府和商业数据平台、AI 应用平台与决策系统。",
        "strength": "政府客户壁垒深，AIP 商业化带来收入加速想象。",
        "risk": "合同节奏不均、估值高、商业客户扩张持续性需要验证。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "QCOM": {
        "asset_type": "stock",
        "business": "高通提供移动芯片、基带、专利授权、汽车和 IoT 芯片。",
        "strength": "移动通信专利壁垒深，汽车和边缘 AI 扩张提供新增长点。",
        "risk": "手机周期、客户自研、授权纠纷和苹果相关收入不确定性较高。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "RIVN": {
        "asset_type": "stock",
        "business": "Rivian 生产电动皮卡、SUV 和商用电动货车。",
        "strength": "品牌定位清晰，商用车合作和新平台降本具备看点。",
        "risk": "现金消耗、产能爬坡、毛利转正和 EV 竞争是主要风险。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "RKLB": {
        "asset_type": "stock",
        "business": "Rocket Lab 提供小型火箭发射、卫星部件和空间系统服务。",
        "strength": "发射与空间系统一体化，商业航天需求增长带来弹性。",
        "risk": "发射失败、项目延期、资本开支和商业航天订单波动较大。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "SLV": {
        "asset_type": "etf",
        "business": "白银 ETF，跟踪白银现货，兼具贵金属和工业金属属性。",
        "strength": "受益避险、通胀、光伏与工业需求，弹性通常高于黄金。",
        "risk": "白银波动大，对美元、实际利率和工业需求变化都很敏感。",
        "next_earnings_date": ETF_EARNINGS_NOTE,
    },
    "SMCI": {
        "asset_type": "stock",
        "business": "超微电脑提供 AI 服务器、整机柜、存储和液冷解决方案。",
        "strength": "产品迭代快，贴近 GPU 供应链，AI 服务器需求高时弹性强。",
        "risk": "硬件利润率、客户集中、供应链和财务披露可信度是核心风险。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "SOFI": {
        "asset_type": "stock",
        "business": "SoFi 提供数字银行、贷款、经纪、支付和金融科技平台服务。",
        "strength": "银行牌照、会员增长和交叉销售提升长期收入潜力。",
        "risk": "信用周期、利率环境、获客成本和消费金融监管会影响估值。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "SPCX": {
        "asset_type": "etf",
        "business": "SPAC/新上市主题 ETF，偏高风险成长和事件驱动资产。",
        "strength": "适合观察新股、SPAC、商业航天等高 beta 主题风险偏好。",
        "risk": "不等同于 SpaceX 私募股权，成分和主题暴露需以基金文件为准。",
        "next_earnings_date": ETF_EARNINGS_NOTE,
    },
    "TLT": {
        "asset_type": "etf",
        "business": "20年以上美国国债 ETF，是长久期利率交易和避险工具。",
        "strength": "对降息预期、经济放缓和避险需求敏感，常用于观察利率方向。",
        "risk": "久期很长，通胀反复或长端利率上行会带来较大回撤。",
        "next_earnings_date": ETF_EARNINGS_NOTE,
    },
    "TSLA": {
        "asset_type": "stock",
        "business": "特斯拉覆盖电动车、储能、充电网络、自动驾驶和机器人方向。",
        "strength": "品牌、制造规模、软件数据和能源业务形成多条成长线。",
        "risk": "EV 价格战、毛利率、自动驾驶兑现、监管和 CEO 风险影响较大。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "TSM": {
        "asset_type": "stock",
        "business": "台积电是全球领先晶圆代工厂，服务 AI、手机和高性能计算客户。",
        "strength": "先进制程和客户信任壁垒深，AI 芯片需求支撑产能利用率。",
        "risk": "台湾地缘风险、客户集中、资本开支周期和汇率需关注。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "UBER": {
        "asset_type": "stock",
        "business": "Uber 经营网约车、外卖配送、货运和本地服务平台。",
        "strength": "双边网络规模大，出行和配送协同提高变现效率。",
        "risk": "司机监管、补贴竞争、消费放缓和保险成本会影响利润率。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "WMT": {
        "asset_type": "stock",
        "business": "沃尔玛经营美国和国际零售、山姆会员店、电商和广告业务。",
        "strength": "供应链、低价心智和会员体系强，防御属性突出。",
        "risk": "低利润率、工资成本、消费结构变化和电商投入会压缩利润。",
        "next_earnings_date": STOCK_EARNINGS_NOTE,
    },
    "XLE": {
        "asset_type": "etf",
        "business": "能源板块 ETF，主要覆盖美国大型油气生产、炼化和能源服务公司。",
        "strength": "与油气价格、现金分红和能源资本开支周期相关性高。",
        "risk": "受油价、OPEC 政策、地缘事件和能源转型预期影响明显。",
        "next_earnings_date": ETF_EARNINGS_NOTE,
    },
    "XLF": {
        "asset_type": "etf",
        "business": "金融板块 ETF，覆盖银行、保险、券商、支付和资管公司。",
        "strength": "适合观察利率、信贷周期、资本市场活跃度和金融监管变化。",
        "risk": "信用成本、收益率曲线倒挂、监管资本和系统性风险会压制表现。",
        "next_earnings_date": ETF_EARNINGS_NOTE,
    },
}

STOCK_DAILY_COLUMNS = ["date", "symbol", "open", "high", "low", "close", "volume", "adjClose"]
OPTION_CHAIN_COLUMNS = [
    "trade_date",
    "option_ticker",
    "underlying",
    "call_put",
    "strike",
    "expiration_date",
    "expiration_type",
    "settlement_type",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "transactions",
    "open_interest",
    "provider_iv",
    "computed_iv",
    "iv_source",
    "underlying_price",
    "dte",
    "cycle_label",
    "iv",
    "iv_pct",
    "moneyness_pct",
]
VOLATILITY_CONE_TARGETS = (7, 14, 21, 30, 45, 60, 90)
VOLATILITY_CONE_COLUMNS = ["dte_target", "p10", "p25", "p50", "p75", "p90", "sample_count"]
VOLATILITY_CONE_LINE_COLUMNS = ["dte_target", "dte", "expiration_date", "iv_pct", "sample_count"]
VOLATILITY_CONE_MIN_CACHE_DAYS = 20
VOLATILITY_CONE_DAILY_CACHE_TABLE = "us_option_volatility_cone_daily"
VOLATILITY_CONE_DAILY_CACHE_COLUMNS = [
    "trade_date",
    "underlying",
    "dte_target",
    "dte",
    "expiration_date",
    "iv_pct",
    "sample_count",
]
OTM_VOLATILITY_CURVE_GRID = (
    -8.0,
    -7.0,
    -6.0,
    -5.0,
    -4.0,
    -3.0,
    -2.0,
    -1.0,
    1.0,
    2.0,
    3.0,
    4.0,
    5.0,
    6.0,
    7.0,
    8.0,
)
OTM_VOLATILITY_CURVE_MIN_SIDE_POINTS = 3
OTM_VOLATILITY_CURVE_COLUMNS = [
    "moneyness_pct",
    "iv_pct",
    "call_put",
    "expiration_date",
    "dte",
    "point_count",
    "expiration_count",
    "quality",
]
OI_DEFENSE_COLUMNS = [
    "trade_date",
    "date",
    "underlying",
    "underlying_close",
    "call_strike",
    "call_oi",
    "call_distance_pct",
    "call_expiration",
    "put_strike",
    "put_oi",
    "put_distance_pct",
    "put_expiration",
    "total_call_oi",
    "total_put_oi",
    "put_call_oi",
]
OI_DEFENSE_CACHE_TABLE = "us_option_oi_defense_daily"
OPTION_ANOMALY_SCAN_CACHE_TABLE = "us_option_anomaly_scan_daily"
UNDERLYING_PROFILE_CACHE_TABLE = "us_option_underlying_profile_daily"
UNDERLYING_PROFILE_CACHE_COLUMNS = [
    "as_of_date",
    "underlying",
    "earnings_date",
    "earnings_time",
    "earnings_source",
    "recent_catalyst",
    "recent_risk",
    "dynamic_note",
    "source_refs_json",
]
PROFILE_NEWS_MAX_ITEMS_DEFAULT = 6
PROFILE_WEB_SEARCH_MAX_QUERIES_DEFAULT = 2
PROFILE_DISPLAY_TZ = dt.timezone(dt.timedelta(hours=8), "Asia/Shanghai")
MARKET_METRICS_COLUMNS = [
    "trade_date",
    "underlying",
    "atm_iv_pct",
    "iv_change_1d",
    "rv20_pct",
    "rv60_pct",
    "iv_rv20_spread",
    "iv_30d",
    "iv_60d",
    "term_slope_30_60",
    "term_state",
    "skew_expiration",
    "put_skew_5pct",
    "call_skew_5pct",
    "put_call_oi",
    "put_call_volume",
    "zero_dte_volume_share_pct",
    "top_oi_strike",
    "top_oi",
    "top5_oi_share_pct",
    "total_open_interest",
    "total_volume",
    "monthly_contract_count",
    "short_cycle_contract_count",
    "provider_iv_rows",
    "computed_iv_rows",
    "open_interest_rows",
    "source",
    "updated_at",
]
OPTION_ANOMALY_SCAN_COLUMNS = [
    "trade_date",
    "underlying",
    "option_ticker",
    "signal_family",
    "call_put",
    "strike",
    "expiration_date",
    "dte",
    "moneyness_pct",
    "underlying_price",
    "close",
    "vwap",
    "volume",
    "open_interest",
    "oi_prev",
    "oi_change",
    "oi_change_pct",
    "volume_oi_ratio",
    "premium_est",
    "iv_pct",
    "iv_change_1d",
    "history_days",
    "historical_avg_oi",
    "historical_max_oi",
    "historical_avg_oi_change",
    "historical_max_oi_change",
    "historical_positive_oi_change_days",
    "oi_change_multiple",
    "anomaly_score",
    "tags_json",
    "data_gap",
]
ANOMALY_SIGNAL_FAMILY_LABELS = {
    "oi_build": "OI增仓埋伏",
    "volume_oi": "成交/OI异动",
    "premium": "大额权利金",
}


def oi_defense_y_axis_range(
    defense_df: pd.DataFrame | None,
    *,
    padding_ratio: float = 0.12,
    min_padding: float = 1.0,
) -> list[float] | None:
    if defense_df is None or defense_df.empty:
        return None

    value_series = []
    for col in ("underlying_close", "call_strike", "put_strike"):
        if col in defense_df.columns:
            values = pd.to_numeric(defense_df[col], errors="coerce").dropna()
            if not values.empty:
                value_series.append(values)
    if not value_series:
        return None

    all_values = pd.concat(value_series, ignore_index=True)
    low = float(all_values.min())
    high = float(all_values.max())
    if not math.isfinite(low) or not math.isfinite(high):
        return None

    span = high - low
    if span <= 0:
        padding = max(float(min_padding), abs(high or 1.0) * float(padding_ratio))
    else:
        padding = max(float(min_padding), span * float(padding_ratio))
    return [low - padding, high + padding]
MARKET_METRIC_NUMERIC_COLUMNS = [
    "atm_iv_pct",
    "iv_change_1d",
    "rv20_pct",
    "rv60_pct",
    "iv_rv20_spread",
    "iv_30d",
    "iv_60d",
    "term_slope_30_60",
    "put_skew_5pct",
    "call_skew_5pct",
    "put_call_oi",
    "put_call_volume",
    "zero_dte_volume_share_pct",
    "top_oi_strike",
    "top_oi",
    "top5_oi_share_pct",
    "total_open_interest",
    "total_volume",
    "monthly_contract_count",
    "short_cycle_contract_count",
    "provider_iv_rows",
    "computed_iv_rows",
    "open_interest_rows",
]
MARKET_CLIMATE_COLUMNS = [
    "indicator_code",
    "as_of_date",
    "value",
    "secondary_value",
    "unit",
    "source",
    "payload_json",
    "updated_at",
]
MARKET_CLIMATE_CARD_ORDER = [
    "VIX期限",
    "利率曲线",
    "实际利率",
    "政策预期",
    "AAII情绪",
    "VIX净仓",
    "供应链压力",
    "信用利差",
]
MARKET_CLIMATE_CACHE_CODES = [
    "VIX_TERM",
    "FEDWATCH",
    "AAII_BULL_BEAR",
    "CFTC_VIX_LEV_NET",
    "GSCPI",
]
MARKET_CLIMATE_MACRO_CODES = [
    "DGS10",
    "T10Y3M",
    "DFII10",
    "BAMLH0A0HYM2",
    "SOFR",
    "FEDFUNDS",
]
MARKET_CLIMATE_FRESHNESS_DAYS = {
    "VIX_TERM": 7,
    "FEDWATCH": 7,
    "AAII_BULL_BEAR": 14,
    "CFTC_VIX_LEV_NET": 14,
    "GSCPI": 60,
    "DGS10": 7,
    "T10Y3M": 7,
    "DFII10": 7,
    "BAMLH0A0HYM2": 7,
    "SOFR": 7,
    "FEDFUNDS": 90,
}
MARKET_CLIMATE_HINTS = {
    "VIX期限": "读法：VIX9D减VIX3M。>0代表短期恐慌高于中期，事件压力高，偏看空或防守；-5到0较常见；<-5说明短期压力低，偏利好风险偏好。",
    "利率曲线": "读法：10年美债减3个月利率。<0是倒挂，越深越担心经济放慢，偏压股市；0到1%算修复区；>1.5%可能是长端利率太高，也会压估值。",
    "实际利率": "读法：扣掉通胀后的10年真实利率。>2%资金成本偏高，压股票估值；1到2%中性偏紧；<1%较宽松。上行偏空，下行偏多。",
    "政策预期": "读法：市场认为美联储下次FOMC最可能的动作。维持或降息概率高，通常说明政策压力没有加大，偏利好；加息概率升，或高利率更久，偏压股市。",
    "AAII情绪": "读法：散户看多比例减看空比例。>+20pp很乐观，容易拥挤，要防追高；<-20pp很悲观，反而常有反弹土壤；-10到+10大致中性。",
    "VIX净仓": "读法：杠杆基金VIX净仓占未平仓比例。>+10%说明防波动的人多，市场紧张；<-10%说明押平静的人多，短线利好风险偏好，但坏消息来时波动会放大。",
    "供应链压力": "读法：0附近算正常。>1说明供应链紧、成本和通胀压力高，偏压估值；<-1说明供应链很顺，偏利多。看趋势：上行偏空，下行偏多。",
    "信用利差": "读法：高收益债比国债多给的利差。<3%风险偏好好，偏利多；3到5%是警戒区；>5%信用压力大，偏看空；>8%通常是明显风险事件。",
}
HISTORICAL_PERCENTILE_FIELDS = {
    "iv_change_1d": "iv_change_1d_percentile",
    "iv_rv20_spread": "iv_rv20_percentile",
    "term_slope_30_60": "term_slope_percentile",
    "put_skew_5pct": "put_skew_5pct_percentile",
    "call_skew_5pct": "call_skew_5pct_percentile",
    "put_call_skew_5pct": "put_call_skew_5pct_percentile",
    "put_call_oi": "put_call_oi_percentile",
    "put_call_volume": "put_call_volume_percentile",
    "zero_dte_volume_share_pct": "zero_dte_volume_share_percentile",
    "top5_oi_share_pct": "top5_oi_share_percentile",
    "total_open_interest": "total_open_interest_percentile",
    "total_volume": "total_volume_percentile",
}


def dashboard_engine():
    return get_db_engine()


def normalize_underlying(underlying: str) -> str:
    return str(underlying or "").strip().upper()


def get_underlying_profile(underlying: str) -> dict[str, str]:
    code = normalize_underlying(underlying)
    profile = dict(UNDERLYING_PROFILE_CARDS.get(code, {}))
    asset_type = str(profile.get("asset_type") or "stock").strip().lower()
    name = str(UNDERLYING_DISPLAY_NAMES.get(code) or code)
    is_etf = asset_type == "etf"
    default_business = (
        f"{name} 的ETF特色资料暂未维护。"
        if is_etf
        else f"{name} 的主营业务资料暂未维护。"
    )
    profile.setdefault("asset_type", asset_type or "stock")
    profile.setdefault("business", default_business)
    profile.setdefault("strength", "可先结合价格趋势、IV位置和期权异动判断市场关注点。")
    profile.setdefault("risk", "请以后续公告、基金文件或公司财报更新为准。")
    profile.setdefault("next_earnings_date", ETF_EARNINGS_NOTE if is_etf else STOCK_EARNINGS_NOTE)
    profile["symbol"] = code
    profile["name"] = name
    return {key: str(value or "") for key, value in profile.items()}


def estimate_next_earnings_window(today: dt.date | None = None) -> str:
    today = today or dt.date.today()
    windows = (
        (dt.date(today.year, 1, 15), dt.date(today.year, 2, 15)),
        (dt.date(today.year, 4, 15), dt.date(today.year, 5, 15)),
        (dt.date(today.year, 7, 15), dt.date(today.year, 8, 15)),
        (dt.date(today.year, 10, 15), dt.date(today.year, 11, 15)),
        (dt.date(today.year + 1, 1, 15), dt.date(today.year + 1, 2, 15)),
    )
    for start, end in windows:
        if today <= end:
            return f"估算 {start:%Y/%m/%d}-{end:%m/%d}"
    start, end = windows[-1]
    return f"估算 {start:%Y/%m/%d}-{end:%m/%d}"


def _nasdaq_earnings_headers() -> dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
        ),
    }


def _nasdaq_earnings_rows_for_date(day: dt.date, timeout: float = 6.0) -> list[dict[str, Any]]:
    try:
        from curl_cffi import requests as curl_requests
    except Exception:
        return []

    session = curl_requests.Session(impersonate="chrome")
    session.trust_env = False
    # Local Windows certificate stores are sometimes incomplete in this app runtime.
    # The endpoint is a public read-only calendar feed; failing closed still returns no rows.
    session.verify = False
    url = f"https://api.nasdaq.com/api/calendar/earnings?date={day:%Y-%m-%d}"
    try:
        resp = session.get(url, headers=_nasdaq_earnings_headers(), timeout=timeout)
        if int(getattr(resp, "status_code", 0) or 0) != 200:
            return []
        payload = resp.json()
    except Exception:
        return []
    data = payload.get("data") if isinstance(payload, dict) else {}
    rows = data.get("rows") if isinstance(data, dict) else []
    return rows if isinstance(rows, list) else []


def _nasdaq_time_label(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw == "time-pre-market":
        return "盘前"
    if raw == "time-after-hours":
        return "盘后"
    if raw == "time-not-supplied":
        return "时间未定"
    return str(value or "").strip()


def _nasdaq_earnings_payload(row: dict[str, Any], day: dt.date) -> dict[str, str]:
    time_label = _nasdaq_time_label(row.get("time"))
    fiscal_quarter = str(row.get("fiscalQuarterEnding") or "").strip()
    eps_forecast = str(row.get("epsForecast") or "").strip()
    parts = [part for part in (time_label, fiscal_quarter, f"EPS预期 {eps_forecast}" if eps_forecast else "") if part]
    return {
        "date": f"{day:%Y/%m/%d}",
        "source": "Nasdaq",
        "detail": " · ".join(parts),
        "is_estimate": "0",
    }


def fetch_nasdaq_next_earnings_dates(
    underlyings: list[str] | tuple[str, ...],
    *,
    today: dt.date | None = None,
    lookahead_days: int = 90,
    timeout: float = 6.0,
    max_workers: int = 8,
    batch_days: int = 12,
) -> dict[str, dict[str, str]]:
    targets = {
        normalize_underlying(symbol)
        for symbol in underlyings
        if normalize_underlying(symbol)
    }
    targets = {
        symbol
        for symbol in targets
        if get_underlying_profile(symbol).get("asset_type") != "etf"
    }
    if not targets:
        return {}

    today = today or dt.date.today()
    max_days = max(1, int(lookahead_days or 1))
    candidate_days = [
        today + dt.timedelta(days=offset)
        for offset in range(max_days + 1)
        if (today + dt.timedelta(days=offset)).weekday() < 5
    ]
    results: dict[str, dict[str, str]] = {}
    worker_count = max(1, min(int(max_workers or 1), 12))
    batch_size = max(1, int(batch_days or 1))
    for start in range(0, len(candidate_days), batch_size):
        batch = candidate_days[start : start + batch_size]
        with ThreadPoolExecutor(max_workers=min(worker_count, len(batch))) as executor:
            futures = {
                executor.submit(_nasdaq_earnings_rows_for_date, day, timeout): day
                for day in batch
            }
            for future in as_completed(futures):
                day = futures[future]
                try:
                    rows = future.result()
                except Exception:
                    rows = []
                for row in rows:
                    symbol = normalize_underlying(row.get("symbol"))
                    if symbol in targets and symbol not in results:
                        results[symbol] = _nasdaq_earnings_payload(row, day)
                if targets <= set(results):
                    break
        if targets <= set(results):
            break
    return results


def underlying_profile_cache_table(use_test_tables: bool = False) -> str:
    suffix = "_test" if use_test_tables else ""
    return f"{UNDERLYING_PROFILE_CACHE_TABLE}{suffix}"


def ensure_underlying_profile_cache_table(engine, use_test_tables: bool = False) -> None:
    if engine is None:
        return
    table_name = safe_table_name(underlying_profile_cache_table(use_test_tables))
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    as_of_date VARCHAR(8) NOT NULL,
                    underlying VARCHAR(32) NOT NULL,
                    earnings_date VARCHAR(32),
                    earnings_time VARCHAR(64),
                    earnings_source VARCHAR(64),
                    recent_catalyst TEXT,
                    recent_risk TEXT,
                    dynamic_note TEXT,
                    source_refs_json TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (as_of_date, underlying)
                )
                """
            )
        )
    cache_key = (id(engine), table_name)
    _TABLE_COLUMNS_CACHE.pop(cache_key, None)
    existing_columns = table_columns(engine, table_name)
    column_types = {
        "as_of_date": "VARCHAR(8)",
        "underlying": "VARCHAR(32)",
        "earnings_date": "VARCHAR(32)",
        "earnings_time": "VARCHAR(64)",
        "earnings_source": "VARCHAR(64)",
        "recent_catalyst": "TEXT",
        "recent_risk": "TEXT",
        "dynamic_note": "TEXT",
        "source_refs_json": "TEXT",
        "updated_at": "TIMESTAMP",
    }
    expected_columns = list(UNDERLYING_PROFILE_CACHE_COLUMNS) + ["updated_at"]
    missing_columns = [col for col in expected_columns if col not in existing_columns]
    if missing_columns:
        with engine.begin() as conn:
            for col in missing_columns:
                conn.execute(
                    text(
                        f"ALTER TABLE {table_name} ADD COLUMN {safe_table_name(col)} "
                        f"{column_types.get(col, 'TEXT')}"
                    )
                )
        _TABLE_COLUMNS_CACHE.pop(cache_key, None)
    _TABLE_EXISTS_CACHE[(id(engine), table_name)] = True
    _TABLE_COLUMNS_CACHE[(id(engine), table_name)] = set(expected_columns)


def _source_refs_json(refs: list[dict[str, Any]]) -> str:
    clean_refs: list[dict[str, str]] = []
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        clean: dict[str, str] = {}
        for key in ("source", "title", "date", "url", "kind", "side", "summary", "confidence"):
            value = str(ref.get(key) or "").strip()
            if value:
                clean[key] = value
        if clean:
            clean_refs.append(clean)
    return json.dumps(clean_refs, ensure_ascii=False)


def _parse_source_refs_json(value: Any) -> list[dict[str, str]]:
    if isinstance(value, list):
        raw_items = value
    else:
        try:
            raw_items = json.loads(str(value or "[]"))
        except Exception:
            raw_items = []
    out: list[dict[str, str]] = []
    for item in raw_items if isinstance(raw_items, list) else []:
        if not isinstance(item, dict):
            continue
        clean = {str(key): str(val) for key, val in item.items() if val is not None}
        if clean:
            out.append(clean)
    return out


def _profile_env_int(name: str, default: int, *, min_value: int = 0, max_value: int = 100) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return min(max(value, min_value), max_value)


def _profile_env_enabled(name: str, default: str = "1") -> bool:
    return str(os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}


def _source_ref_key(ref: dict[str, Any]) -> str:
    title = re.sub(r"\s+", " ", str(ref.get("title") or "").strip().lower())
    url = str(ref.get("url") or "").strip().lower()
    return url or title


def _dedupe_source_refs(refs: list[dict[str, Any]], limit: int | None = None) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        key = _source_ref_key(ref)
        if not key or key in seen:
            continue
        seen.add(key)
        clean: dict[str, str] = {}
        for field in ("source", "title", "date", "url", "kind", "side", "summary", "confidence"):
            value = str(ref.get(field) or "").strip()
            if value:
                clean[field] = value[:500]
        if clean.get("title") or clean.get("summary"):
            out.append(clean)
        if limit is not None and len(out) >= limit:
            break
    return out


def _first_sentence(text_value: str, max_len: int = 72) -> str:
    text_value = re.sub(r"\s+", " ", str(text_value or "")).strip()
    if not text_value:
        return ""
    match = re.search(r"[。.!?？]", text_value)
    if match:
        return text_value[: match.end()].strip()
    return text_value[:max_len].strip()


_PROFILE_ANALYST_PATTERNS = (
    "analyst",
    "upgrade",
    "downgrade",
    "price target",
    "target price",
    "initiates",
    "reiterates",
    "maintains",
    "raises target",
    "cuts target",
    "评级",
    "目标价",
    "上调",
    "下调",
    "买入",
    "增持",
    "中性",
    "跑赢",
    "跑输",
)
_PROFILE_RISK_PATTERNS = (
    "downgrade",
    "cuts target",
    "lawsuit",
    "probe",
    "investigation",
    "regulatory",
    "antitrust",
    "delay",
    "weak",
    "miss",
    "pressure",
    "competition",
    "tariff",
    "recall",
    "ban",
    "decline",
    "下调",
    "降级",
    "诉讼",
    "调查",
    "监管",
    "反垄断",
    "延迟",
    "疲软",
    "竞争",
    "成本",
    "关税",
    "召回",
    "下滑",
    "风险",
)
_PROFILE_CATALYST_PATTERNS = (
    "upgrade",
    "raises target",
    "beat",
    "beats",
    "raises guidance",
    "launch",
    "partnership",
    "approval",
    "demand",
    "record",
    "buyback",
    "dividend",
    "ai",
    "上调",
    "获批",
    "发布",
    "合作",
    "需求",
    "增长",
    "盈利",
    "回购",
    "分红",
    "超预期",
)


def _classify_profile_source_ref(ref: dict[str, Any]) -> dict[str, str]:
    title = str(ref.get("title") or "")
    summary = str(ref.get("summary") or "")
    text_value = f"{title} {summary}".lower()
    kind = "analyst" if any(token in text_value for token in _PROFILE_ANALYST_PATTERNS) else str(ref.get("kind") or "news")
    has_risk = any(token in text_value for token in _PROFILE_RISK_PATTERNS)
    has_catalyst = any(token in text_value for token in _PROFILE_CATALYST_PATTERNS)
    if has_risk and not has_catalyst:
        side = "risk"
    elif has_catalyst and not has_risk:
        side = "catalyst"
    elif has_catalyst and has_risk:
        side = "mixed"
    else:
        side = str(ref.get("side") or "neutral")
    out = dict(ref)
    out["kind"] = kind
    out["side"] = side
    return {str(key): str(value) for key, value in out.items() if value is not None}


def _latest_profile_metric_snapshot(
    underlying: str,
    *,
    engine=None,
    use_test_tables: bool = False,
) -> dict[str, Any]:
    try:
        df = load_market_metrics_history(
            underlying,
            window=2,
            use_test_tables=use_test_tables,
            engine=engine,
        )
    except Exception:
        return {}
    if df is None or df.empty:
        return {}
    row = df.sort_values("trade_date").iloc[-1].to_dict()
    return {str(key): value for key, value in row.items()}


def _format_metric_sentence(metrics: dict[str, Any]) -> str:
    if not metrics:
        return "本地期权指标暂无最新样本。"
    parts: list[str] = []
    atm_iv = _clean_number(metrics.get("atm_iv_pct"))
    iv_change = _clean_number(metrics.get("iv_change_1d"))
    put_call_oi = _clean_number(metrics.get("put_call_oi"))
    zero_dte = _clean_number(metrics.get("zero_dte_volume_share_pct"))
    if atm_iv is not None:
        parts.append(f"ATM IV约{atm_iv:.1f}%")
    if iv_change is not None:
        if abs(iv_change) < 0.05:
            parts.append("隐波日变动不大")
        else:
            direction = "抬升" if iv_change > 0 else "回落"
            parts.append(f"隐波较前日{direction}{abs(iv_change):.1f}点")
    if put_call_oi is not None:
        if put_call_oi >= 1.2:
            parts.append(f"Put/Call OI {put_call_oi:.2f}，保护需求偏高")
        elif put_call_oi <= 0.8:
            parts.append(f"Put/Call OI {put_call_oi:.2f}，上行动能关注度更高")
        else:
            parts.append(f"Put/Call OI {put_call_oi:.2f}，仓位相对均衡")
    if zero_dte is not None and zero_dte > 0:
        parts.append(f"0DTE成交占比约{zero_dte:.1f}%")
    return "；".join(parts[:3]) + "。" if parts else "本地期权指标暂无最新样本。"


def _fetch_recent_profile_news_refs(
    underlying: str,
    *,
    lookback_days: int = 30,
    timeout: float = 4.0,
    max_items: int = 3,
) -> list[dict[str, str]]:
    if str(os.getenv("US_OPTIONS_PROFILE_NEWS_ENABLED", "1")).strip() != "1":
        return []
    code = normalize_underlying(underlying)
    if not code:
        return []
    try:
        from email.utils import parsedate_to_datetime
        from urllib.parse import quote
        from urllib.request import Request, urlopen
        import xml.etree.ElementTree as ET
    except Exception:
        return []

    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote(code)}&region=US&lang=en-US"
    req = Request(url, headers={"User-Agent": _nasdaq_earnings_headers()["User-Agent"]})
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read(400_000)
        root = ET.fromstring(raw)
    except Exception:
        return []

    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=max(int(lookback_days or 30), 1))
    refs: list[dict[str, str]] = []
    for item in root.findall(".//item"):
        title = "".join(item.findtext("title") or "").strip()
        link = "".join(item.findtext("link") or "").strip()
        pub_raw = "".join(item.findtext("pubDate") or "").strip()
        pub_dt: dt.datetime | None = None
        if pub_raw:
            try:
                pub_dt = parsedate_to_datetime(pub_raw)
                if pub_dt.tzinfo is None:
                    pub_dt = pub_dt.replace(tzinfo=dt.timezone.utc)
            except Exception:
                pub_dt = None
        if pub_dt is not None and pub_dt < since:
            continue
        if title:
            refs.append(
                {
                    "source": "Yahoo Finance News",
                    "title": title[:180],
                    "date": pub_dt.strftime("%Y/%m/%d") if pub_dt else "",
                    "url": link,
                }
            )
        if len(refs) >= max(int(max_items or 3), 1):
            break
    return refs


def _load_local_profile_news_refs(
    underlying: str,
    profile: dict[str, str],
    *,
    engine=None,
    max_items: int = PROFILE_NEWS_MAX_ITEMS_DEFAULT,
) -> list[dict[str, str]]:
    engine = engine or dashboard_engine()
    if engine is None or not table_exists(engine, "stock_news"):
        return []
    columns = table_columns(engine, "stock_news")
    if not {"title"}.issubset(columns):
        return []
    selected = [
        _select_expr(columns, "title"),
        _select_expr(columns, "description", "summary"),
        _select_expr(columns, "publishedDate", "date"),
        _select_expr(columns, "source"),
        _select_expr(columns, "url"),
        _select_expr(columns, "tickers"),
    ]
    code = normalize_underlying(underlying)
    name = str(profile.get("name") or code)
    params = {
        "code": code,
        "name": name,
        "like_code": f"%{code}%",
        "like_name": f"%{name}%",
        "limit": max(1, min(int(max_items or PROFILE_NEWS_MAX_ITEMS_DEFAULT), 20)),
    }
    where_parts = ["title LIKE :like_code", "title LIKE :like_name"]
    if "tickers" in columns:
        where_parts.extend(["tickers = :code", "tickers = :name"])
    sql = text(
        f"""
        SELECT {", ".join(selected)}
        FROM stock_news
        WHERE {" OR ".join(where_parts)}
        ORDER BY date DESC
        LIMIT :limit
        """
    )
    try:
        df = pd.read_sql(sql, engine, params=params)
    except Exception:
        return []
    refs: list[dict[str, str]] = []
    for row in df.to_dict(orient="records"):
        title = str(row.get("title") or "").strip()
        summary = str(row.get("summary") or "").strip()
        if not title and not summary:
            continue
        refs.append(
            _classify_profile_source_ref(
                {
                    "source": str(row.get("source") or "本地新闻库"),
                    "title": title[:180],
                    "summary": summary[:260],
                    "date": str(row.get("date") or "")[:16],
                    "url": str(row.get("url") or ""),
                    "kind": "news",
                }
            )
        )
    return refs


def _collect_profile_news_context(
    underlying: str,
    profile: dict[str, str],
    *,
    lookback_days: int,
    engine=None,
) -> list[dict[str, str]]:
    max_items = _profile_env_int(
        "US_OPTIONS_PROFILE_NEWS_MAX_ITEMS",
        PROFILE_NEWS_MAX_ITEMS_DEFAULT,
        min_value=1,
        max_value=20,
    )
    refs = []
    refs.extend(
        _fetch_recent_profile_news_refs(
            underlying,
            lookback_days=lookback_days,
            max_items=max_items,
        )
    )
    refs.extend(
        _load_local_profile_news_refs(
            underlying,
            profile,
            engine=engine,
            max_items=max_items,
        )
    )
    return [_classify_profile_source_ref(ref) for ref in _dedupe_source_refs(refs, limit=max_items)]


def _profile_web_search_queries(profile: dict[str, str], *, lookback_days: int) -> list[str]:
    code = str(profile.get("symbol") or "").upper()
    name = str(profile.get("name") or code)
    is_etf = str(profile.get("asset_type") or "").lower() == "etf"
    if is_etf:
        return [
            f"{code} {name} ETF latest sector macro flow volatility news last {lookback_days} days",
            f"{code} {name} ETF holdings sector risk latest news",
        ]
    return [
        f"{code} {name} latest news analyst upgrade downgrade price target earnings guidance last {lookback_days} days",
        f"{code} {name} recent catalyst risk earnings analyst says",
    ]


def _collect_profile_web_search_context(
    profile: dict[str, str],
    *,
    lookback_days: int,
) -> list[dict[str, str]]:
    if not _profile_env_enabled("US_OPTIONS_PROFILE_WEB_SEARCH_ENABLED", "1"):
        return []
    if not os.getenv("ZHIPUAI_API_KEY"):
        return []
    max_queries = _profile_env_int(
        "US_OPTIONS_PROFILE_WEB_SEARCH_MAX_QUERIES",
        PROFILE_WEB_SEARCH_MAX_QUERIES_DEFAULT,
        min_value=0,
        max_value=5,
    )
    if max_queries <= 0:
        return []
    try:
        import search_tools
    except Exception:
        return []
    refs: list[dict[str, str]] = []
    for query in _profile_web_search_queries(profile, lookback_days=lookback_days)[:max_queries]:
        try:
            answer = search_tools._search_web_impl(query)  # type: ignore[attr-defined]
        except Exception:
            answer = ""
        answer = re.sub(r"\s+", " ", str(answer or "")).strip()
        if not answer or answer.startswith("❌") or "未搜索到相关内容" in answer:
            continue
        refs.append(
            _classify_profile_source_ref(
                {
                    "source": "Web Search",
                    "title": query,
                    "summary": answer[:420],
                    "date": dt.date.today().strftime("%Y/%m/%d"),
                    "kind": "search",
                }
            )
        )
    return _dedupe_source_refs(refs, limit=max_queries)


def _build_profile_llm_note(
    *,
    profile: dict[str, str],
    earnings_date: str,
    earnings_time: str,
    metric_sentence: str,
    news_refs: list[dict[str, str]],
) -> str:
    if str(os.getenv("US_OPTIONS_PROFILE_LLM_ENABLED", "1")).strip() != "1":
        return ""
    if not os.getenv("DASHSCOPE_API_KEY"):
        return ""
    try:
        from llm_compat import build_report_tongyi_llm

        news_titles = "；".join(str(item.get("title") or "") for item in news_refs[:3] if item.get("title"))
        prompt = (
            "请为美股期权标的资料卡写一句中文近期变化摘要，60字以内。"
            "只基于输入资料，不给买卖建议。\n"
            f"标的：{profile.get('symbol')} {profile.get('name')}，类型：{profile.get('asset_type')}\n"
            f"财报：{earnings_date} {earnings_time}\n"
            f"期权指标：{metric_sentence}\n"
            f"新闻标题：{news_titles or '无'}"
        )
        llm = build_report_tongyi_llm(
            env_prefix="US_OPTIONS_PROFILE",
            default_model=os.getenv("US_OPTIONS_PROFILE_LLM_MODEL") or "qwen-plus",
            temperature=0.1,
            request_timeout=20,
            max_retries=0,
        )
        msg = llm.invoke(prompt)
        content = getattr(msg, "content", msg)
        text_value = re.sub(r"\s+", " ", str(content or "")).strip()
        return text_value[:120]
    except Exception:
        return ""


def _profile_options_context(
    underlying: str,
    *,
    metrics: dict[str, Any] | None,
    as_of_date: str,
    engine=None,
    use_test_tables: bool = False,
) -> dict[str, Any]:
    metric_parts: list[str] = []
    metrics = metrics or {}
    atm_iv = _clean_number(metrics.get("atm_iv_pct"))
    iv_change = _clean_number(metrics.get("iv_change_1d"))
    iv_rv = _clean_number(metrics.get("iv_rv20_spread"))
    put_call_oi = _clean_number(metrics.get("put_call_oi"))
    put_call_volume = _clean_number(metrics.get("put_call_volume"))
    zero_dte = _clean_number(metrics.get("zero_dte_volume_share_pct"))
    put_skew = _clean_number(metrics.get("put_skew_5pct"))
    call_skew = _clean_number(metrics.get("call_skew_5pct"))
    if atm_iv is not None:
        metric_parts.append(f"ATM IV {atm_iv:.1f}%")
    if iv_change is not None and abs(iv_change) >= 0.05:
        metric_parts.append(f"IV较前日{'升' if iv_change > 0 else '降'}{abs(iv_change):.1f}点")
    if iv_rv is not None:
        metric_parts.append(f"IV-RV20 {iv_rv:+.1f}点")
    if put_call_oi is not None:
        if put_call_oi >= 1.2:
            metric_parts.append(f"Put/Call OI {put_call_oi:.2f}，保护需求偏高")
        elif put_call_oi <= 0.8:
            metric_parts.append(f"Put/Call OI {put_call_oi:.2f}，看涨仓位更活跃")
        else:
            metric_parts.append(f"Put/Call OI {put_call_oi:.2f}，仓位相对均衡")
    if put_call_volume is not None and put_call_volume >= 1.2:
        metric_parts.append(f"Put/Call成交 {put_call_volume:.2f}，短线避险成交增加")
    if call_skew is not None and call_skew > 0:
        metric_parts.append(f"Call Skew {call_skew:+.1f}，上方追涨溢价抬升")
    if put_skew is not None and put_skew > 0:
        metric_parts.append(f"Put Skew {put_skew:+.1f}，下方保护溢价抬升")
    if zero_dte is not None and zero_dte >= 15:
        metric_parts.append(f"0DTE占比约{zero_dte:.1f}%，盘中波动放大")

    anomaly_parts: list[str] = []
    trade_date = normalize_trade_date(metrics.get("trade_date")) or normalize_trade_date(as_of_date)
    if engine is not None and trade_date:
        try:
            scan = load_option_anomaly_scan_cache(
                trade_date,
                underlyings=[underlying],
                use_test_tables=use_test_tables,
                engine=engine,
            )
        except Exception:
            scan = _empty_df(OPTION_ANOMALY_SCAN_COLUMNS)
        if scan is not None and not scan.empty:
            for row in scan.head(3).to_dict(orient="records"):
                side = "Call" if str(row.get("call_put") or "").upper() == "C" else "Put"
                tags = []
                try:
                    tags = json.loads(str(row.get("tags_json") or "[]"))
                except Exception:
                    tags = []
                tag_text = "、".join(str(tag) for tag in tags[:2]) if isinstance(tags, list) else ""
                strike = _clean_number(row.get("strike"))
                score = _clean_number(row.get("anomaly_score"))
                label = f"{side} {strike:g}" if strike is not None else side
                if tag_text:
                    label = f"{label} {tag_text}"
                if score is not None:
                    label = f"{label} score {score:.0f}"
                anomaly_parts.append(label)

    summary_parts = [*metric_parts[:4]]
    if anomaly_parts:
        summary_parts.append("异动：" + "；".join(anomaly_parts[:2]))
    summary = "；".join(summary_parts) + "。" if summary_parts else "本地期权指标暂无最新样本。"
    return {
        "summary": summary,
        "metrics": metrics,
        "anomalies": anomaly_parts,
        "refs": [
            {
                "source": "本地期权指标",
                "title": summary,
                "date": trade_date or as_of_date,
                "kind": "options",
                "side": "mixed" if anomaly_parts else "neutral",
            }
        ],
    }


def _json_from_llm_text(value: Any) -> dict[str, Any]:
    text_value = str(value or "").strip()
    if not text_value:
        return {}
    text_value = re.sub(r"^```(?:json)?|```$", "", text_value, flags=re.I | re.M).strip()
    match = re.search(r"\{.*\}", text_value, flags=re.S)
    if match:
        text_value = match.group(0)
    try:
        data = json.loads(text_value)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _build_profile_llm_json(
    *,
    profile: dict[str, str],
    earnings_date: str,
    earnings_time: str,
    options_context: dict[str, Any],
    refs: list[dict[str, str]],
) -> dict[str, Any]:
    if not _profile_env_enabled("US_OPTIONS_PROFILE_LLM_ENABLED", "1"):
        return {}
    if not os.getenv("DASHSCOPE_API_KEY"):
        return {}
    try:
        from llm_compat import build_report_tongyi_llm

        source_lines = []
        for idx, ref in enumerate(refs[:8], start=1):
            title = str(ref.get("title") or "").strip()
            summary = str(ref.get("summary") or "").strip()
            source = str(ref.get("source") or "").strip()
            date_text = str(ref.get("date") or "").strip()
            side = str(ref.get("side") or "").strip()
            kind = str(ref.get("kind") or "").strip()
            line = f"{idx}. [{source} {date_text} {kind} {side}] {title}"
            if summary and summary != title:
                line = f"{line} - {summary[:260]}"
            source_lines.append(line)
        prompt = f"""
请为美股期权标的资料卡生成两句中文摘要，必须只基于输入资料，不给买卖建议，不编造机构名、评级或目标价。

标的：{profile.get('symbol')} {profile.get('name')}，类型：{profile.get('asset_type')}
固定业务：{profile.get('business')}
长期优势：{profile.get('strength')}
长期风险：{profile.get('risk')}
下次财报：{earnings_date} {earnings_time}
期权数据：{options_context.get('summary')}
近期来源：
{chr(10).join(source_lines) if source_lines else '无'}

输出严格 JSON，不要 Markdown：
{{
  "recent_catalyst": "一句，30-75个中文字符，结合近期事件/分析师观点/财报或期权数据，说明市场在交易什么正向催化",
  "recent_risk": "一句，30-75个中文字符，结合近期事件/分析师观点/财报或期权数据，说明主要风险或反向验证点",
  "used_refs": ["最多3个来源编号"],
  "confidence": "high|medium|low"
}}
""".strip()
        llm = build_report_tongyi_llm(
            env_prefix="US_OPTIONS_PROFILE",
            default_model=os.getenv("US_OPTIONS_PROFILE_LLM_MODEL") or "qwen-plus",
            temperature=0.1,
            request_timeout=30,
            max_retries=0,
        )
        msg = llm.invoke(prompt)
        data = _json_from_llm_text(getattr(msg, "content", msg))
    except Exception:
        return {}
    catalyst = re.sub(r"\s+", " ", str(data.get("recent_catalyst") or "")).strip()
    risk = re.sub(r"\s+", " ", str(data.get("recent_risk") or "")).strip()
    if not catalyst or not risk:
        return {}
    return {
        "recent_catalyst": catalyst[:140],
        "recent_risk": risk[:140],
        "used_refs": data.get("used_refs") if isinstance(data.get("used_refs"), list) else [],
        "confidence": str(data.get("confidence") or "medium"),
    }


def _fallback_profile_dynamic_v2(
    *,
    profile: dict[str, str],
    earnings_date: str,
    earnings_time: str,
    options_context: dict[str, Any],
    refs: list[dict[str, str]],
    lookback_days: int,
) -> dict[str, str]:
    name = str(profile.get("name") or profile.get("symbol") or "")
    is_etf = str(profile.get("asset_type") or "").lower() == "etf"
    analyst_refs = [ref for ref in refs if ref.get("kind") == "analyst"]
    catalyst_refs = [ref for ref in refs if ref.get("side") in {"catalyst", "mixed"}]
    risk_refs = [ref for ref in refs if ref.get("side") in {"risk", "mixed"}]
    option_summary = str(options_context.get("summary") or _format_metric_sentence({}))
    business_hint = _first_sentence(str(profile.get("business") or ""), max_len=60)
    risk_hint = _first_sentence(str(profile.get("risk") or ""), max_len=60)

    if is_etf:
        catalyst_base = catalyst_refs[0].get("title") if catalyst_refs else ""
        risk_base = risk_refs[0].get("title") if risk_refs else ""
        recent_catalyst = (
            f"近{lookback_days}天关注{catalyst_base[:38]}；{option_summary}"
            if catalyst_base
            else f"近期看{name}成分板块轮动和宏观利率变化；{option_summary}"
        )
        recent_risk = (
            f"风险看{risk_base[:40]}；ETF无单一财报催化。"
            if risk_base
            else f"{risk_hint or 'ETF风险主要来自权重行业回撤和市场beta变化'}；ETF没有单一公司财报。"
        )
    else:
        headline = ""
        if analyst_refs:
            headline = analyst_refs[0].get("title", "")
        elif catalyst_refs:
            headline = catalyst_refs[0].get("title", "")
        risk_headline = risk_refs[0].get("title", "") if risk_refs else ""
        earnings_hint = "财报日历已确认" if "估算" not in earnings_date and earnings_date else "财报窗口待确认"
        recent_catalyst = (
            f"公开报道/分析师线索显示{headline[:42]}；{option_summary}"
            if headline
            else f"近期看{name}的{earnings_hint}、{business_hint or '业务主线'}；{option_summary}"
        )
        recent_risk = (
            f"反向风险看{risk_headline[:42]}；需观察期权定价是否回落。"
            if risk_headline
            else f"{risk_hint or '风险在业绩预期、估值和行业竞争'}；财报前后留意IV事件后回落。"
        )
    return {
        "recent_catalyst": recent_catalyst[:160],
        "recent_risk": recent_risk[:160],
        "confidence": "medium" if refs else "low",
    }


def _summarize_profile_dynamic_v2(
    *,
    profile: dict[str, str],
    earnings_date: str,
    earnings_time: str,
    options_context: dict[str, Any],
    refs: list[dict[str, str]],
    lookback_days: int,
) -> dict[str, str]:
    llm_data = _build_profile_llm_json(
        profile=profile,
        earnings_date=earnings_date,
        earnings_time=earnings_time,
        options_context=options_context,
        refs=refs,
    )
    if llm_data:
        return {
            "recent_catalyst": str(llm_data.get("recent_catalyst") or ""),
            "recent_risk": str(llm_data.get("recent_risk") or ""),
            "confidence": str(llm_data.get("confidence") or "medium"),
        }
    return _fallback_profile_dynamic_v2(
        profile=profile,
        earnings_date=earnings_date,
        earnings_time=earnings_time,
        options_context=options_context,
        refs=refs,
        lookback_days=lookback_days,
    )


def _fallback_underlying_profile_dynamic(
    profile: dict[str, str],
    as_of_date: str | dt.date | dt.datetime | None = None,
) -> dict[str, str]:
    is_etf = str(profile.get("asset_type") or "").lower() == "etf"
    code = str(profile.get("symbol") or "")
    name = str(profile.get("name") or code)
    as_of_date_text = normalize_trade_date(as_of_date) if as_of_date else dt.date.today().strftime("%Y%m%d")
    try:
        today = dt.datetime.strptime(as_of_date_text, "%Y%m%d").date()
    except Exception:
        today = dt.date.today()
        as_of_date_text = today.strftime("%Y%m%d")
    business_hint = _first_sentence(str(profile.get("business") or ""))
    risk_hint = _first_sentence(str(profile.get("risk") or ""))
    if is_etf:
        earnings_date = ETF_EARNINGS_NOTE
        earnings_time = ""
        earnings_source = "ETF"
        recent_catalyst = (
            f"近期关注{name}的成分板块轮动、利率环境和风险偏好变化；"
            "可结合价格趋势、IV位置和资金风险偏好观察。"
        )
        recent_risk = (
            f"{risk_hint or 'ETF短线风险主要来自指数权重行业和市场 beta 的同步波动'}"
            " 没有单一公司财报催化。"
        )
        refs = [{"source": "固定资料", "title": "ETF无公司财报", "date": as_of_date_text}]
    else:
        earnings_date = estimate_next_earnings_window(today)
        earnings_time = "待日历确认"
        earnings_source = "估算"
        recent_catalyst = (
            f"近期关注{name}的财报窗口、业务主线和期权定价变化；"
            f"{business_hint or '可结合价格趋势和IV位置观察市场关注点'}"
        )
        recent_risk = (
            f"{risk_hint or '需关注业绩预期、估值和行业竞争变化'}"
            " 财报前后留意隐含波动率抬升和事件后回落。"
        )
        refs = [{"source": "估算", "title": "季度财报窗口估算 + 固定资料", "date": earnings_date}]
    return {
        "as_of_date": as_of_date_text,
        "underlying": code,
        "earnings_date": earnings_date,
        "earnings_time": earnings_time,
        "earnings_source": earnings_source,
        "recent_catalyst": recent_catalyst,
        "recent_risk": recent_risk,
        "dynamic_note": "日更缓存尚未生成，当前使用固定资料和规则兜底。",
        "source_refs_json": _source_refs_json(refs),
        "updated_at": "",
    }


def _build_underlying_profile_dynamic_row(
    underlying: str,
    *,
    as_of_date: str,
    lookback_days: int,
    earnings_payload: dict[str, str] | None,
    metrics: dict[str, Any] | None,
    news_refs: list[dict[str, str]] | None,
    options_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = get_underlying_profile(underlying)
    code = str(profile.get("symbol") or underlying).upper()
    is_etf = str(profile.get("asset_type") or "").lower() == "etf"
    options_context = options_context or {"summary": _format_metric_sentence(metrics or {}), "refs": []}
    refs: list[dict[str, Any]] = []
    refs.extend(options_context.get("refs") or [])
    refs.extend(news_refs or [])

    if is_etf:
        earnings_date = ETF_EARNINGS_NOTE
        earnings_time = ""
        earnings_source = "ETF"
        refs.append(
            {
                "source": "基金/指数属性",
                "title": "ETF无公司财报",
                "date": as_of_date,
                "kind": "fund_profile",
                "side": "neutral",
            }
        )
    else:
        payload = earnings_payload or {}
        earnings_date = str(payload.get("date") or estimate_next_earnings_window()).strip()
        earnings_time = str(payload.get("detail") or "").strip()
        earnings_source = str(payload.get("source") or "估算").strip()
        source_title = "Nasdaq earnings calendar" if earnings_source == "Nasdaq" else "季度财报窗口估算"
        refs.append(
            {
                "source": earnings_source or "估算",
                "title": source_title,
                "date": earnings_date,
                "kind": "earnings",
                "side": "neutral",
            }
        )

    refs = _dedupe_source_refs(refs, limit=12)
    summary = _summarize_profile_dynamic_v2(
        profile=profile,
        earnings_date=earnings_date,
        earnings_time=earnings_time,
        options_context=options_context,
        refs=refs,
        lookback_days=lookback_days,
    )
    recent_catalyst = summary.get("recent_catalyst") or "近期变化待更新"
    recent_risk = summary.get("recent_risk") or "近期变化待更新"
    confidence = summary.get("confidence") or "medium"
    source_names = sorted({str(ref.get("source") or "") for ref in refs if ref.get("source")})
    dynamic_note = f"V2 {confidence}：近{lookback_days}天公开来源 + 本地期权指标；来源 {' + '.join(source_names[:4]) or '规则兜底'}。"

    return {
        "as_of_date": as_of_date,
        "underlying": code,
        "earnings_date": earnings_date,
        "earnings_time": earnings_time,
        "earnings_source": earnings_source,
        "recent_catalyst": recent_catalyst,
        "recent_risk": recent_risk,
        "dynamic_note": dynamic_note,
        "source_refs_json": _source_refs_json(refs),
    }


def replace_underlying_profile_cache(
    rows: list[dict[str, Any]],
    *,
    as_of_date: str | dt.date | dt.datetime,
    underlyings: list[str] | tuple[str, ...],
    use_test_tables: bool = False,
    engine=None,
) -> int:
    engine = engine or dashboard_engine()
    if engine is None:
        return 0
    ensure_underlying_profile_cache_table(engine, use_test_tables)
    table_name = safe_table_name(underlying_profile_cache_table(use_test_tables))
    target_underlyings = _scan_underlyings(list(underlyings))
    if not target_underlyings:
        return 0
    placeholders, params = _named_in_clause("underlying", target_underlyings)
    params["as_of_date"] = normalize_trade_date(as_of_date)
    clean_rows = []
    for row in rows:
        item = {col: row.get(col) for col in UNDERLYING_PROFILE_CACHE_COLUMNS}
        item["as_of_date"] = normalize_trade_date(item.get("as_of_date")) or params["as_of_date"]
        item["underlying"] = normalize_underlying(item.get("underlying"))
        if item["underlying"]:
            clean_rows.append(item)
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                DELETE FROM {table_name}
                WHERE as_of_date = :as_of_date
                  AND underlying IN ({placeholders})
                """
            ),
            params,
        )
        if clean_rows:
            column_sql = ", ".join(safe_table_name(col) for col in UNDERLYING_PROFILE_CACHE_COLUMNS)
            value_sql = ", ".join(f":{col}" for col in UNDERLYING_PROFILE_CACHE_COLUMNS)
            conn.execute(
                text(
                    f"""
                    INSERT INTO {table_name} ({column_sql})
                    VALUES ({value_sql})
                    """
                ),
                clean_rows,
            )
    return len(clean_rows)


def rebuild_underlying_profile_cache(
    underlyings: list[str] | tuple[str, ...] | None = None,
    *,
    as_of_date: str | dt.date | dt.datetime | None = None,
    lookback_days: int = 30,
    apply: bool = False,
    use_test_tables: bool = False,
    engine=None,
) -> dict[str, Any]:
    engine = engine or dashboard_engine()
    if engine is None:
        return {"status": "missing_engine", "as_of_date": None, "rows": 0}
    as_of_date_text = normalize_trade_date(as_of_date) if as_of_date else dt.date.today().strftime("%Y%m%d")
    try:
        today = dt.datetime.strptime(as_of_date_text, "%Y%m%d").date()
    except Exception:
        today = dt.date.today()
        as_of_date_text = today.strftime("%Y%m%d")
    target_underlyings = _scan_underlyings(underlyings)
    if not target_underlyings:
        return {"status": "no_underlyings", "as_of_date": as_of_date_text, "rows": 0}

    stock_underlyings = [
        symbol
        for symbol in target_underlyings
        if get_underlying_profile(symbol).get("asset_type") != "etf"
    ]
    try:
        earnings_by_symbol = fetch_nasdaq_next_earnings_dates(
            stock_underlyings,
            today=today,
            lookahead_days=90,
            timeout=3.0,
            max_workers=12,
            batch_days=28,
        )
    except Exception:
        earnings_by_symbol = {}

    rows: list[dict[str, Any]] = []
    live_earnings = 0
    news_ref_count = 0
    web_ref_count = 0
    for symbol in target_underlyings:
        profile = get_underlying_profile(symbol)
        is_etf = profile.get("asset_type") == "etf"
        earnings_payload = None
        if not is_etf:
            earnings_payload = earnings_by_symbol.get(symbol)
            if earnings_payload:
                live_earnings += 1
            else:
                earnings_payload = {
                    "date": estimate_next_earnings_window(today),
                    "source": "估算",
                    "detail": "日历未确认",
                    "is_estimate": "1",
                }
        metrics = _latest_profile_metric_snapshot(
            symbol,
            engine=engine,
            use_test_tables=use_test_tables,
        )
        news_refs = _collect_profile_news_context(
            symbol,
            profile,
            lookback_days=max(int(lookback_days or 30), 1),
            engine=engine,
        )
        web_refs = _collect_profile_web_search_context(
            profile,
            lookback_days=max(int(lookback_days or 30), 1),
        )
        news_ref_count += len(news_refs)
        web_ref_count += len(web_refs)
        options_context = _profile_options_context(
            symbol,
            metrics=metrics,
            as_of_date=as_of_date_text,
            engine=engine,
            use_test_tables=use_test_tables,
        )
        rows.append(
            _build_underlying_profile_dynamic_row(
                symbol,
                as_of_date=as_of_date_text,
                lookback_days=max(int(lookback_days or 30), 1),
                earnings_payload=earnings_payload,
                metrics=metrics,
                news_refs=[*news_refs, *web_refs],
                options_context=options_context,
            )
        )

    written = 0
    if apply:
        written = replace_underlying_profile_cache(
            rows,
            as_of_date=as_of_date_text,
            underlyings=target_underlyings,
            use_test_tables=use_test_tables,
            engine=engine,
        )
    return {
        "status": "updated" if apply else "dry_run",
        "as_of_date": as_of_date_text,
        "underlyings": target_underlyings,
        "rows": len(rows),
        "written": written,
        "live_earnings": live_earnings,
        "news_refs": news_ref_count,
        "web_refs": web_ref_count,
    }


def load_underlying_profile_dynamic(
    underlying: str,
    engine=None,
    *,
    as_of_date: str | dt.date | dt.datetime | None = None,
    use_test_tables: bool = False,
) -> dict[str, str]:
    profile = get_underlying_profile(underlying)
    fallback = _fallback_underlying_profile_dynamic(profile, as_of_date)
    engine = engine or dashboard_engine()
    if engine is None:
        return fallback
    table_name = safe_table_name(underlying_profile_cache_table(use_test_tables))
    if not table_exists(engine, table_name):
        return fallback
    columns = table_columns(engine, table_name)
    if not {"as_of_date", "underlying"}.issubset(columns):
        return fallback
    selected = [_select_expr(columns, col) for col in UNDERLYING_PROFILE_CACHE_COLUMNS]
    selected.append(_select_expr(columns, "updated_at"))
    code = normalize_underlying(underlying)
    target_date = normalize_trade_date(as_of_date) if as_of_date else "99991231"
    sql = text(
        f"""
        SELECT {", ".join(selected)}
        FROM {table_name}
        WHERE underlying = :underlying
          AND as_of_date <= :as_of_date
        ORDER BY as_of_date DESC
        LIMIT 1
        """
    )
    try:
        df = pd.read_sql(sql, engine, params={"underlying": code, "as_of_date": target_date})
    except Exception:
        return fallback
    if df.empty:
        return fallback
    row = df.iloc[0].to_dict()
    out = dict(fallback)
    for key in list(UNDERLYING_PROFILE_CACHE_COLUMNS) + ["updated_at"]:
        value = row.get(key)
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except Exception:
            pass
        out[key] = str(value)
    return out


def build_underlying_profile_card(
    underlying: str,
    engine=None,
    *,
    as_of_date: str | dt.date | dt.datetime | None = None,
    use_test_tables: bool = False,
) -> dict[str, Any]:
    profile = get_underlying_profile(underlying)
    dynamic = load_underlying_profile_dynamic(
        profile.get("symbol", underlying),
        engine=engine,
        as_of_date=as_of_date,
        use_test_tables=use_test_tables,
    )
    card: dict[str, Any] = dict(profile)
    card.update(
        {
            "earnings_date": dynamic.get("earnings_date") or profile.get("next_earnings_date") or "",
            "earnings_time": dynamic.get("earnings_time") or "",
            "earnings_source": dynamic.get("earnings_source") or "",
            "recent_catalyst": dynamic.get("recent_catalyst") or "近期变化待更新",
            "recent_risk": dynamic.get("recent_risk") or "近期变化待更新",
            "dynamic_note": dynamic.get("dynamic_note") or "",
            "dynamic_as_of_date": dynamic.get("as_of_date") or "",
            "dynamic_updated_at": dynamic.get("updated_at") or "",
            "dynamic_source_refs": _parse_source_refs_json(dynamic.get("source_refs_json")),
        }
    )
    return card


def format_profile_updated_at_beijing(
    value: str | dt.date | dt.datetime | pd.Timestamp | None,
    as_of_date: str | dt.date | dt.datetime | None = None,
) -> str:
    raw = str(value or "").strip()
    if raw:
        parsed = pd.to_datetime(raw, errors="coerce")
        if not pd.isna(parsed):
            try:
                timestamp = pd.Timestamp(parsed)
                if timestamp.tzinfo is None:
                    timestamp = timestamp.tz_localize(dt.timezone.utc)
                else:
                    timestamp = timestamp.tz_convert(dt.timezone.utc)
                return timestamp.tz_convert(PROFILE_DISPLAY_TZ).strftime("%m/%d %H:%M")
            except Exception:
                return raw[:16]
        return raw[:16]

    compact = compact_date(as_of_date)
    if len(compact) == 8:
        return f"{compact[:4]}/{compact[4:6]}/{compact[6:8]}"
    return "待更新"


def normalize_trade_date(value: str | dt.date | dt.datetime | None) -> str:
    return compact_date(value)


def safe_table_name(name: str) -> str:
    if not re.match(r"^[A-Za-z0-9_]+$", name or ""):
        raise ValueError(f"Invalid table name: {name!r}")
    return name


def _mysql_force_index(engine, index_name: str) -> str:
    dialect_name = getattr(getattr(engine, "dialect", None), "name", "")
    if dialect_name != "mysql":
        return ""
    safe_name = safe_table_name(index_name)
    return f" FORCE INDEX ({safe_name})"


def _named_in_clause(prefix: str, values: list[str]) -> tuple[str, dict[str, str]]:
    params = {f"{prefix}_{idx}": value for idx, value in enumerate(values)}
    placeholders = ", ".join(f":{key}" for key in params)
    return placeholders, params


def option_table_names(use_test_tables: bool = False) -> dict[str, str]:
    return table_names(use_test_tables)


def _empty_df(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


_TABLE_EXISTS_CACHE: dict[tuple[int, str], bool] = {}
_TABLE_COLUMNS_CACHE: dict[tuple[int, str], set[str]] = {}


def _schema_cache_enabled(engine) -> bool:
    return getattr(getattr(engine, "dialect", None), "name", "") != "sqlite"


def table_exists(engine, table_name: str) -> bool:
    if engine is None:
        return False
    safe_name = safe_table_name(table_name)
    cache_key = (id(engine), safe_name)
    if _schema_cache_enabled(engine) and cache_key in _TABLE_EXISTS_CACHE:
        return _TABLE_EXISTS_CACHE[cache_key]
    try:
        exists = bool(inspect(engine).has_table(safe_name))
    except Exception:
        return False
    if _schema_cache_enabled(engine):
        _TABLE_EXISTS_CACHE[cache_key] = exists
    return exists


def table_columns(engine, table_name: str) -> set[str]:
    if engine is None:
        return set()
    safe_name = safe_table_name(table_name)
    cache_key = (id(engine), safe_name)
    if _schema_cache_enabled(engine) and cache_key in _TABLE_COLUMNS_CACHE:
        return set(_TABLE_COLUMNS_CACHE[cache_key])
    try:
        columns = {str(col["name"]) for col in inspect(engine).get_columns(safe_name)}
    except Exception:
        return set()
    if _schema_cache_enabled(engine):
        _TABLE_EXISTS_CACHE[cache_key] = True
        _TABLE_COLUMNS_CACHE[cache_key] = set(columns)
    return columns


def _select_expr(columns: set[str], column: str, alias: str | None = None) -> str:
    alias = alias or column
    if column in columns:
        return f"{safe_table_name(column)} AS {safe_table_name(alias)}"
    return f"NULL AS {safe_table_name(alias)}"


def _scalar(engine, sql, params: dict[str, Any] | None = None) -> Any:
    try:
        with engine.connect() as conn:
            return conn.execute(sql, params or {}).scalar()
    except Exception:
        return None


def _clean_number(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _as_date(value: Any) -> dt.date | None:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _date_detail(value: Any) -> str:
    as_of = _as_date(value)
    if as_of is None:
        return ""
    return as_of.strftime("%m/%d")


def _format_plain_number(value: Any, digits: int = 1) -> str:
    number = _clean_number(value)
    if number is None:
        return "--"
    return f"{number:,.{digits}f}"


def _format_pct_card(value: Any, digits: int = 1, *, signed: bool = False) -> str:
    number = _clean_number(value)
    if number is None:
        return "--"
    prefix = "+" if signed and number > 0 else ""
    return f"{prefix}{number:.{digits}f}%"


def _format_pp_card(value: Any, digits: int = 1, *, signed: bool = True) -> str:
    number = _clean_number(value)
    if number is None:
        return "--"
    prefix = "+" if signed and number > 0 else ""
    return f"{prefix}{number:.{digits}f}pp"


def _format_signed_value(value: Any, digits: int = 1, suffix: str = "") -> str:
    number = _clean_number(value)
    if number is None:
        return "--"
    prefix = "+" if number > 0 else ""
    return f"{prefix}{number:.{digits}f}{suffix}"


def _format_bp_change(value: Any, *, signed: bool = True) -> str:
    number = _clean_number(value)
    if number is None:
        return "--"
    bps = number * 100
    prefix = "+" if signed and bps > 0 else ""
    return f"{prefix}{bps:.0f}bp"


def _payload_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _freshness_status(as_of: Any, code: str, today: dt.date | None = None) -> tuple[str, int | None]:
    as_of_date = _as_date(as_of)
    if as_of_date is None:
        return "missing", None
    today = today or dt.datetime.now().date()
    age = (today - as_of_date).days
    max_age = MARKET_CLIMATE_FRESHNESS_DAYS.get(code, 14)
    return ("stale" if age > max_age else "fresh"), age


def _detail_with_date(detail: str, as_of: Any, code: str, today: dt.date | None) -> str:
    parts = [part for part in [detail, _date_detail(as_of)] if part]
    status, age = _freshness_status(as_of, code, today)
    if status == "stale" and age is not None:
        parts.append(f"旧{age}天")
    return " · ".join(parts) if parts else "暂无缓存"


def _market_climate_card(
    label: str,
    value: str = "--",
    detail: str = "暂无缓存",
    color: str = "#94a3b8",
    *,
    as_of: Any = None,
    code: str = "",
    today: dt.date | None = None,
) -> dict[str, Any]:
    status, age = _freshness_status(as_of, code, today) if code else ("missing", None)
    return {
        "label": label,
        "value": value or "--",
        "detail": detail or "暂无缓存",
        "color": color,
        "as_of": _as_date(as_of).isoformat() if _as_date(as_of) else None,
        "freshness": status,
        "age_days": age,
        "hint": MARKET_CLIMATE_HINTS.get(label, ""),
    }


def _empty_market_climate_card(label: str) -> dict[str, Any]:
    return _market_climate_card(label)


def _load_latest_market_climate_rows(engine, codes: list[str]) -> dict[str, dict[str, Any]]:
    if engine is None:
        return {}
    columns = table_columns(engine, "market_climate_daily")
    if not {"indicator_code", "as_of_date", "value"}.issubset(columns):
        return {}

    selected = [_select_expr(columns, col) for col in MARKET_CLIMATE_COLUMNS]
    codes = [str(code).strip() for code in codes if str(code or "").strip()]
    if not codes:
        return {}
    placeholders, params = _named_in_clause("code", codes)
    sql = text(
        f"""
        SELECT {", ".join(selected)}
        FROM market_climate_daily
        WHERE indicator_code IN ({placeholders})
        ORDER BY indicator_code, as_of_date DESC
        """
    )
    out: dict[str, dict[str, Any]] = {}
    try:
        df = pd.read_sql(sql, engine, params=params)
    except Exception:
        return {}
    if df.empty:
        return {}
    df = df.drop_duplicates(subset=["indicator_code"], keep="first")
    for row_dict in df.to_dict(orient="records"):
        code = str(row_dict.get("indicator_code") or "")
        if not code:
            continue
        row = dict(row_dict)
        row["value"] = _clean_number(row.get("value"))
        row["secondary_value"] = _clean_number(row.get("secondary_value"))
        row["payload"] = _payload_dict(row.get("payload_json"))
        out[code] = row
    return out


def _load_macro_history_rows(engine, codes: list[str], limit: int = 90) -> dict[str, pd.DataFrame]:
    if engine is None:
        return {}
    columns = table_columns(engine, "macro_daily")
    if not {"trade_date", "indicator_code", "close_value"}.issubset(columns):
        return {}

    selected = [
        _select_expr(columns, "trade_date"),
        _select_expr(columns, "indicator_code"),
        _select_expr(columns, "indicator_name"),
        _select_expr(columns, "close_value"),
        _select_expr(columns, "change_value"),
        _select_expr(columns, "change_pct"),
    ]
    limit = min(max(int(limit or 90), 2), 500)
    codes = [str(code).strip() for code in codes if str(code or "").strip()]
    if not codes:
        return {}
    placeholders, params = _named_in_clause("code", codes)
    params["limit"] = limit
    sql = text(
        f"""
        SELECT trade_date, indicator_code, indicator_name, close_value, change_value, change_pct
        FROM (
            SELECT {", ".join(selected)},
                   ROW_NUMBER() OVER (PARTITION BY indicator_code ORDER BY trade_date DESC) AS rn
            FROM macro_daily
            WHERE indicator_code IN ({placeholders})
        ) scoped
        WHERE rn <= :limit
        ORDER BY indicator_code, trade_date DESC
        """
    )
    try:
        all_rows = pd.read_sql(sql, engine, params=params)
    except Exception:
        fallback_sql = text(
            f"""
            SELECT {", ".join(selected)}
            FROM macro_daily
            WHERE indicator_code IN ({placeholders})
            ORDER BY indicator_code, trade_date DESC
            """
        )
        try:
            all_rows = pd.read_sql(fallback_sql, engine, params={key: value for key, value in params.items() if key != "limit"})
        except Exception:
            return {}
    if all_rows.empty:
        return {}

    out: dict[str, pd.DataFrame] = {}
    for code, df in all_rows.groupby("indicator_code", dropna=False):
        code_text = str(code or "")
        if not code_text:
            continue
        df = df.head(limit).copy()
        if df.empty:
            continue
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        for col in ("close_value", "change_value", "change_pct"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["trade_date", "close_value"]).sort_values("trade_date").reset_index(drop=True)
        if not df.empty:
            out[code_text] = df
    return out


def _latest_macro_row(macro_rows: dict[str, pd.DataFrame], code: str) -> pd.Series | None:
    df = macro_rows.get(code)
    if df is None or df.empty:
        return None
    return df.iloc[-1]


def _macro_change_since(macro_rows: dict[str, pd.DataFrame], code: str, days: int) -> float | None:
    df = macro_rows.get(code)
    if df is None or len(df) < 2:
        return None
    latest = df.iloc[-1]
    latest_date = _as_date(latest.get("trade_date"))
    latest_value = _clean_number(latest.get("close_value"))
    if latest_date is None or latest_value is None:
        return None
    target_date = latest_date - dt.timedelta(days=max(int(days or 1), 1))
    eligible = df[df["trade_date"].dt.date <= target_date]
    if eligible.empty:
        eligible = df.iloc[:-1]
    if eligible.empty:
        return None
    base = _clean_number(eligible.iloc[-1].get("close_value"))
    if base is None:
        return None
    return latest_value - base


def _rate_curve_card(macro_rows: dict[str, pd.DataFrame], today: dt.date | None) -> dict[str, Any]:
    ten_year = _latest_macro_row(macro_rows, "DGS10")
    curve = _latest_macro_row(macro_rows, "T10Y3M")
    if ten_year is None:
        return _empty_market_climate_card("利率曲线")
    ten_value = _clean_number(ten_year.get("close_value"))
    curve_value = _clean_number(curve.get("close_value")) if curve is not None else None
    color = "#dc2626" if curve_value is not None and curve_value < 0 else "#2563eb"
    detail = "10Y-3M " + (_format_pp_card(curve_value, 2) if curve_value is not None else "--")
    return _market_climate_card(
        "利率曲线",
        _format_pct_card(ten_value, 2),
        _detail_with_date(detail, ten_year.get("trade_date"), "DGS10", today),
        color,
        as_of=ten_year.get("trade_date"),
        code="DGS10",
        today=today,
    )


def _real_yield_card(macro_rows: dict[str, pd.DataFrame], today: dt.date | None) -> dict[str, Any]:
    row = _latest_macro_row(macro_rows, "DFII10")
    if row is None:
        return _empty_market_climate_card("实际利率")
    change = _macro_change_since(macro_rows, "DFII10", 5)
    value = _clean_number(row.get("close_value"))
    color = "#dc2626" if value is not None and value >= 2.0 else "#2563eb"
    detail = "5日 " + (_format_bp_change(change) if change is not None else "--")
    return _market_climate_card(
        "实际利率",
        _format_pct_card(value, 2),
        _detail_with_date(detail, row.get("trade_date"), "DFII10", today),
        color,
        as_of=row.get("trade_date"),
        code="DFII10",
        today=today,
    )


def _credit_spread_card(macro_rows: dict[str, pd.DataFrame], today: dt.date | None) -> dict[str, Any]:
    row = _latest_macro_row(macro_rows, "BAMLH0A0HYM2")
    if row is None:
        return _empty_market_climate_card("信用利差")
    change = _macro_change_since(macro_rows, "BAMLH0A0HYM2", 30)
    value = _clean_number(row.get("close_value"))
    color = "#dc2626" if change is not None and change > 0 else "#059669"
    detail = "1M " + (_format_bp_change(change) if change is not None else "--")
    return _market_climate_card(
        "信用利差",
        _format_pct_card(value, 2),
        _detail_with_date(detail, row.get("trade_date"), "BAMLH0A0HYM2", today),
        color,
        as_of=row.get("trade_date"),
        code="BAMLH0A0HYM2",
        today=today,
    )


def _vix_term_card(climate_rows: dict[str, dict[str, Any]], today: dt.date | None) -> dict[str, Any]:
    row = climate_rows.get("VIX_TERM")
    if not row:
        return _empty_market_climate_card("VIX期限")
    spread = _clean_number(row.get("value"))
    payload = row.get("payload") or {}
    state = "近端倒挂" if spread is not None and spread > 0 else "远端更高"
    detail = state
    vix = _clean_number(payload.get("vix"))
    if vix is not None:
        detail = f"VIX {_format_plain_number(vix, 1)}"
    color = "#dc2626" if spread is not None and spread > 0 else "#059669"
    return _market_climate_card(
        "VIX期限",
        _format_signed_value(spread, 1, "点"),
        _detail_with_date(detail, row.get("as_of_date"), "VIX_TERM", today),
        color,
        as_of=row.get("as_of_date"),
        code="VIX_TERM",
        today=today,
    )


def _policy_rate_fallback_card(macro_rows: dict[str, pd.DataFrame], today: dt.date | None) -> dict[str, Any]:
    sofr = _latest_macro_row(macro_rows, "SOFR")
    fedfunds = _latest_macro_row(macro_rows, "FEDFUNDS")
    row = sofr if sofr is not None else fedfunds
    if row is None:
        return _empty_market_climate_card("政策预期")
    value = _clean_number(row.get("close_value"))
    source_label = "SOFR" if sofr is not None else "Fed Funds"
    detail = f"{source_label}替代"
    if sofr is not None and fedfunds is not None:
        sofr_value = _clean_number(sofr.get("close_value"))
        fedfunds_value = _clean_number(fedfunds.get("close_value"))
        spread = sofr_value - fedfunds_value if sofr_value is not None and fedfunds_value is not None else None
        detail = "SOFR-Fed " + (_format_bp_change(spread) if spread is not None else "--")
    return _market_climate_card(
        "政策预期",
        _format_pct_card(value, 2),
        _detail_with_date(detail, row.get("trade_date"), str(row.get("indicator_code") or "SOFR"), today),
        "#7c3aed",
        as_of=row.get("trade_date"),
        code=str(row.get("indicator_code") or "SOFR"),
        today=today,
    )


def _fedwatch_card(
    climate_rows: dict[str, dict[str, Any]],
    macro_rows: dict[str, pd.DataFrame],
    today: dt.date | None,
) -> dict[str, Any]:
    row = climate_rows.get("FEDWATCH")
    if not row:
        return _policy_rate_fallback_card(macro_rows, today)
    payload = row.get("payload") or {}
    probability = _clean_number(row.get("value"))
    action_label = str(payload.get("action_label") or payload.get("action") or "最高概率")
    meeting_date = payload.get("meeting_date") or payload.get("event_date")
    detail = f"会议 {_date_detail(meeting_date)}" if meeting_date else "下次会议"
    return _market_climate_card(
        "政策预期",
        f"{action_label} {_format_pct_card(probability, 0)}",
        _detail_with_date(detail, row.get("as_of_date"), "FEDWATCH", today),
        "#7c3aed",
        as_of=row.get("as_of_date"),
        code="FEDWATCH",
        today=today,
    )


def _aaii_card(climate_rows: dict[str, dict[str, Any]], today: dt.date | None) -> dict[str, Any]:
    row = climate_rows.get("AAII_BULL_BEAR")
    if not row:
        return _empty_market_climate_card("AAII情绪")
    payload = row.get("payload") or {}
    spread = _clean_number(row.get("value"))
    bullish = _clean_number(payload.get("bullish_pct"))
    bearish = _clean_number(payload.get("bearish_pct"))
    detail = "多空差"
    if bullish is not None and bearish is not None:
        detail = f"牛{bullish:.0f} 熊{bearish:.0f}"
    color = "#dc2626" if spread is not None and spread > 15 else "#2563eb"
    return _market_climate_card(
        "AAII情绪",
        _format_pp_card(spread, 1),
        _detail_with_date(detail, row.get("as_of_date"), "AAII_BULL_BEAR", today),
        color,
        as_of=row.get("as_of_date"),
        code="AAII_BULL_BEAR",
        today=today,
    )


def _cftc_vix_card(climate_rows: dict[str, dict[str, Any]], today: dt.date | None) -> dict[str, Any]:
    row = climate_rows.get("CFTC_VIX_LEV_NET")
    if not row:
        return _empty_market_climate_card("VIX净仓")
    ratio = _clean_number(row.get("value"))
    net_contracts = _clean_number(row.get("secondary_value"))
    detail = "杠杆基金/OI"
    if net_contracts is not None:
        detail = f"净{net_contracts:,.0f}张"
    color = "#dc2626" if ratio is not None and ratio > 0 else "#2563eb"
    return _market_climate_card(
        "VIX净仓",
        _format_pct_card(ratio, 1, signed=True),
        _detail_with_date(detail, row.get("as_of_date"), "CFTC_VIX_LEV_NET", today),
        color,
        as_of=row.get("as_of_date"),
        code="CFTC_VIX_LEV_NET",
        today=today,
    )


def _gscpi_card(climate_rows: dict[str, dict[str, Any]], today: dt.date | None) -> dict[str, Any]:
    row = climate_rows.get("GSCPI")
    if not row:
        return _empty_market_climate_card("供应链压力")
    value = _clean_number(row.get("value"))
    change_3m = _clean_number(row.get("secondary_value"))
    color = "#dc2626" if value is not None and value > 1 else "#059669"
    detail = "3M " + (_format_signed_value(change_3m, 2) if change_3m is not None else "--")
    return _market_climate_card(
        "供应链压力",
        _format_plain_number(value, 2),
        _detail_with_date(detail, row.get("as_of_date"), "GSCPI", today),
        color,
        as_of=row.get("as_of_date"),
        code="GSCPI",
        today=today,
    )


def load_market_climate_strip(engine=None, today: dt.date | None = None) -> list[dict[str, Any]]:
    """Return eight cached market-climate cards for the US options dashboard.

    This function deliberately performs only local database reads. External
    market data is refreshed by update_market_climate_daily.py and cached in
    market_climate_daily so the Streamlit first paint stays fast.
    """
    engine = engine or dashboard_engine()
    climate_rows = _load_latest_market_climate_rows(engine, MARKET_CLIMATE_CACHE_CODES)
    macro_rows = _load_macro_history_rows(engine, MARKET_CLIMATE_MACRO_CODES)
    cards = [
        _vix_term_card(climate_rows, today),
        _rate_curve_card(macro_rows, today),
        _real_yield_card(macro_rows, today),
        _fedwatch_card(climate_rows, macro_rows, today),
        _aaii_card(climate_rows, today),
        _cftc_vix_card(climate_rows, today),
        _gscpi_card(climate_rows, today),
        _credit_spread_card(macro_rows, today),
    ]
    by_label = {card["label"]: card for card in cards}
    return [by_label.get(label, _empty_market_climate_card(label)) for label in MARKET_CLIMATE_CARD_ORDER]


def load_stock_daily(symbol: str, limit: int = 420, engine=None) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(STOCK_DAILY_COLUMNS)

    columns = table_columns(engine, "stock_prices")
    if "symbol" not in columns or "date" not in columns:
        return _empty_df(STOCK_DAILY_COLUMNS)

    limit = min(max(int(limit or 1), 1), 5000)
    selected = [
        _select_expr(columns, "date"),
        _select_expr(columns, "symbol"),
        _select_expr(columns, "open"),
        _select_expr(columns, "high"),
        _select_expr(columns, "low"),
        _select_expr(columns, "close"),
        _select_expr(columns, "volume"),
        _select_expr(columns, "adjClose"),
    ]
    sql = text(
        f"""
        SELECT {", ".join(selected)}
        FROM stock_prices
        WHERE symbol = :symbol
        ORDER BY date DESC
        LIMIT {limit}
        """
    )
    try:
        df = pd.read_sql(sql, engine, params={"symbol": normalize_underlying(symbol)})
    except Exception:
        return _empty_df(STOCK_DAILY_COLUMNS)

    if df.empty:
        return _empty_df(STOCK_DAILY_COLUMNS)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    for col in ("open", "high", "low", "close", "volume", "adjClose"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[STOCK_DAILY_COLUMNS]


def load_latest_option_trade_date(
    underlying: str,
    *,
    use_test_tables: bool = True,
    engine=None,
) -> str | None:
    engine = engine or dashboard_engine()
    if engine is None:
        return None

    names = option_table_names(use_test_tables)
    underlying = normalize_underlying(underlying)
    candidates: list[str] = []
    for logical_name in ("daily", "iv"):
        table_name = safe_table_name(names[logical_name])
        if not table_exists(engine, table_name):
            continue
        columns = table_columns(engine, table_name)
        if not {"trade_date", "underlying"}.issubset(columns):
            continue
        value = _scalar(
            engine,
            text(
                f"""
                SELECT MAX(trade_date)
                FROM {table_name}{_mysql_force_index(engine, "idx_underlying_date")}
                WHERE underlying = :underlying
                """
            ),
            {"underlying": underlying},
        )
        if value:
            candidates.append(str(value))
    if not candidates:
        return None
    return normalize_trade_date(max(candidates))


def load_available_option_trade_dates(
    underlying: str,
    *,
    use_test_tables: bool = True,
    limit: int = 260,
    engine=None,
) -> list[str]:
    engine = engine or dashboard_engine()
    if engine is None:
        return []

    names = option_table_names(use_test_tables)
    table_name = safe_table_name(names["daily"])
    if not table_exists(engine, table_name):
        return []
    columns = table_columns(engine, table_name)
    if not {"trade_date", "underlying"}.issubset(columns):
        return []

    limit = min(max(int(limit or 1), 1), 5000)
    sql = text(
        f"""
        SELECT trade_date
        FROM {table_name}{_mysql_force_index(engine, "idx_underlying_date")}
        WHERE underlying = :underlying
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT {limit}
        """
    )
    try:
        rows = pd.read_sql(sql, engine, params={"underlying": normalize_underlying(underlying)})
    except Exception:
        return []
    if rows.empty or "trade_date" not in rows.columns:
        return []
    dates = [normalize_trade_date(value) for value in rows["trade_date"].tolist()]
    return [value for value in dates if len(value) == 8]


def _underlying_price_by_trade_date(
    symbol: str,
    start_date: str,
    end_date: str,
    *,
    use_test_tables: bool = True,
    engine=None,
) -> dict[str, float]:
    prices: dict[str, float] = {}
    stock_df = load_stock_daily(symbol, limit=5000, engine=engine)
    if not stock_df.empty:
        scoped = stock_df.copy()
        scoped["trade_date"] = scoped["date"].apply(normalize_trade_date)
        scoped["close"] = pd.to_numeric(scoped["close"], errors="coerce")
        scoped = scoped[(scoped["trade_date"] >= start_date) & (scoped["trade_date"] <= end_date)]
        prices.update(
            {
                str(row.trade_date): float(row.close)
                for row in scoped.itertuples(index=False)
                if pd.notna(row.close)
            }
        )

    names = option_table_names(use_test_tables)
    iv_table = safe_table_name(names["iv"])
    if engine is None or not table_exists(engine, iv_table):
        return prices
    columns = table_columns(engine, iv_table)
    if not {"trade_date", "underlying", "underlying_price"}.issubset(columns):
        return prices
    sql = text(
        f"""
        SELECT trade_date, AVG(underlying_price) AS underlying_price
        FROM {iv_table}
        WHERE underlying = :underlying
          AND trade_date >= :start_date
          AND trade_date <= :end_date
          AND underlying_price IS NOT NULL
        GROUP BY trade_date
        """
    )
    try:
        iv_prices = pd.read_sql(
            sql,
            engine,
            params={
                "underlying": normalize_underlying(symbol),
                "start_date": start_date,
                "end_date": end_date,
            },
        )
    except Exception:
        return prices
    for row in iv_prices.itertuples(index=False):
        trade_date = normalize_trade_date(row.trade_date)
        if trade_date and trade_date not in prices and pd.notna(row.underlying_price):
            prices[trade_date] = float(row.underlying_price)
    return prices


def _load_cached_oi_defense_history(
    underlying: str,
    end_date: str,
    *,
    window: int,
    engine=None,
) -> pd.DataFrame:
    if engine is None:
        return _empty_df(OI_DEFENSE_COLUMNS)
    columns = table_columns(engine, OI_DEFENSE_CACHE_TABLE)
    required = {"trade_date", "underlying", "call_strike", "put_strike"}
    if not required.issubset(columns):
        return _empty_df(OI_DEFENSE_COLUMNS)

    selected = [_select_expr(columns, col) for col in OI_DEFENSE_COLUMNS]
    sql = text(
        f"""
        SELECT {", ".join(selected)}
        FROM {OI_DEFENSE_CACHE_TABLE}
        WHERE underlying = :underlying
          AND trade_date <= :end_date
        ORDER BY trade_date DESC
        LIMIT {window}
        """
    )
    try:
        df = pd.read_sql(sql, engine, params={"underlying": underlying, "end_date": end_date})
    except Exception:
        return _empty_df(OI_DEFENSE_COLUMNS)
    if df.empty:
        return _empty_df(OI_DEFENSE_COLUMNS)

    df["trade_date"] = df["trade_date"].apply(normalize_trade_date)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    else:
        df["date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d", errors="coerce")
    for col in [
        "underlying_close",
        "call_strike",
        "call_oi",
        "call_distance_pct",
        "put_strike",
        "put_oi",
        "put_distance_pct",
        "total_call_oi",
        "total_put_oi",
        "put_call_oi",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[OI_DEFENSE_COLUMNS].sort_values("trade_date").reset_index(drop=True)


def load_oi_defense_history(
    underlying: str,
    end_date: str | dt.date | dt.datetime,
    *,
    window: int = 20,
    use_test_tables: bool = True,
    prefer_cache: bool = True,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(OI_DEFENSE_COLUMNS)

    underlying = normalize_underlying(underlying)
    end_text = normalize_trade_date(end_date)
    if not end_text:
        return _empty_df(OI_DEFENSE_COLUMNS)

    window = min(max(int(window or 20), 1), 260)
    if prefer_cache:
        cached = _load_cached_oi_defense_history(underlying, end_text, window=window, engine=engine)
        if not cached.empty:
            latest_cached_date = (
                cached["trade_date"]
                .dropna()
                .astype(str)
                .loc[lambda series: series.str.len() > 0]
                .max()
            )
            if latest_cached_date >= end_text:
                return cached

    names = option_table_names(use_test_tables)
    daily_table = safe_table_name(names["daily"])
    contracts_table = safe_table_name(names["contracts"])
    if not table_exists(engine, daily_table) or not table_exists(engine, contracts_table):
        return _empty_df(OI_DEFENSE_COLUMNS)

    daily_columns = table_columns(engine, daily_table)
    contract_columns = table_columns(engine, contracts_table)
    required_daily = {"trade_date", "underlying", "option_ticker", "open_interest"}
    required_contracts = {"option_ticker", "call_put", "strike", "expiration_date"}
    if not required_daily.issubset(daily_columns) or not required_contracts.issubset(contract_columns):
        return _empty_df(OI_DEFENSE_COLUMNS)

    date_limit = min(max(window * 5, window), 1300)
    dates_sql = text(
        f"""
        SELECT trade_date
        FROM {daily_table}{_mysql_force_index(engine, "idx_underlying_date")}
        WHERE underlying = :underlying
          AND trade_date <= :end_date
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT {date_limit}
        """
    )
    try:
        date_df = pd.read_sql(dates_sql, engine, params={"underlying": underlying, "end_date": end_text})
    except Exception:
        return _empty_df(OI_DEFENSE_COLUMNS)
    if date_df.empty:
        return _empty_df(OI_DEFENSE_COLUMNS)

    selected_dates = (
        date_df["trade_date"]
        .apply(normalize_trade_date)
        .dropna()
        .astype(str)
        .loc[lambda series: series.str.len() > 0]
        .drop_duplicates()
        .head(window)
        .sort_values()
        .tolist()
    )
    if not selected_dates:
        return _empty_df(OI_DEFENSE_COLUMNS)

    start_text = selected_dates[0]
    price_map = _underlying_price_by_trade_date(
        underlying,
        start_text,
        end_text,
        use_test_tables=use_test_tables,
        engine=engine,
    )
    rows_sql = text(
        f"""
        SELECT
            d.trade_date AS trade_date,
            d.option_ticker AS option_ticker,
            d.underlying AS underlying,
            d.open_interest AS open_interest,
            c.call_put AS call_put,
            c.strike AS strike,
            c.expiration_date AS expiration_date
        FROM {daily_table} d{_mysql_force_index(engine, "idx_underlying_date")}
        JOIN {contracts_table} c ON d.option_ticker = c.option_ticker
        WHERE d.underlying = :underlying
          AND d.trade_date >= :start_date
          AND d.trade_date <= :end_date
        """
    )
    try:
        df = pd.read_sql(
            rows_sql,
            engine,
            params={"underlying": underlying, "start_date": start_text, "end_date": end_text},
        )
    except Exception:
        return _empty_df(OI_DEFENSE_COLUMNS)
    if df.empty:
        return _empty_df(OI_DEFENSE_COLUMNS)

    selected_set = set(selected_dates)
    df["trade_date"] = df["trade_date"].apply(normalize_trade_date)
    df = df[df["trade_date"].isin(selected_set)].copy()
    df["call_put"] = df["call_put"].astype(str).str.upper().str.slice(0, 1)
    df["open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce")
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df = df[df["call_put"].isin(["C", "P"])]
    df = df[(df["open_interest"] > 0) & df["strike"].notna()]
    if df.empty:
        return _empty_df(OI_DEFENSE_COLUMNS)

    df["dte"] = df.apply(lambda row: dte_for_trade_date(row["expiration_date"], row["trade_date"]), axis=1)
    df["dte"] = pd.to_numeric(df["dte"], errors="coerce")
    df = df[df["dte"].between(0, 90)]
    if df.empty:
        return _empty_df(OI_DEFENSE_COLUMNS)

    df["underlying_close"] = df["trade_date"].map(price_map)
    has_price = pd.to_numeric(df["underlying_close"], errors="coerce") > 0
    df["distance_pct"] = None
    df.loc[has_price, "distance_pct"] = (
        (df.loc[has_price, "strike"] - df.loc[has_price, "underlying_close"])
        / df.loc[has_price, "underlying_close"]
        * 100
    )
    df = df[df["distance_pct"].isna() | (pd.to_numeric(df["distance_pct"], errors="coerce").abs() <= 25)]
    if df.empty:
        return _empty_df(OI_DEFENSE_COLUMNS)

    output_rows: list[dict[str, Any]] = []
    for trade_date in selected_dates:
        day = df[df["trade_date"] == trade_date].copy()
        if day.empty:
            continue
        row: dict[str, Any] = {
            "trade_date": trade_date,
            "date": pd.to_datetime(trade_date, format="%Y%m%d", errors="coerce"),
            "underlying": underlying,
            "underlying_close": price_map.get(trade_date),
            "call_strike": None,
            "call_oi": None,
            "call_distance_pct": None,
            "call_expiration": None,
            "put_strike": None,
            "put_oi": None,
            "put_distance_pct": None,
            "put_expiration": None,
        }
        total_call_oi = float(day.loc[day["call_put"] == "C", "open_interest"].sum())
        total_put_oi = float(day.loc[day["call_put"] == "P", "open_interest"].sum())
        row["total_call_oi"] = total_call_oi if total_call_oi > 0 else None
        row["total_put_oi"] = total_put_oi if total_put_oi > 0 else None
        row["put_call_oi"] = total_put_oi / total_call_oi if total_call_oi > 0 else None

        for side, prefix in (("C", "call"), ("P", "put")):
            side_df = day[day["call_put"] == side]
            if side_df.empty:
                continue
            by_strike = side_df.groupby("strike", dropna=True)["open_interest"].sum().sort_values(ascending=False)
            if by_strike.empty:
                continue
            top_strike = float(by_strike.index[0])
            top_oi = float(by_strike.iloc[0])
            top_rows = side_df[side_df["strike"] == top_strike]
            by_expiration = (
                top_rows.groupby("expiration_date", dropna=False)["open_interest"].sum().sort_values(ascending=False)
            )
            expiration = str(by_expiration.index[0]) if not by_expiration.empty else None
            close_price = row.get("underlying_close")
            distance_pct = (top_strike - float(close_price)) / float(close_price) * 100 if close_price else None
            row[f"{prefix}_strike"] = top_strike
            row[f"{prefix}_oi"] = top_oi
            row[f"{prefix}_distance_pct"] = distance_pct
            row[f"{prefix}_expiration"] = expiration

        if row.get("call_strike") is not None or row.get("put_strike") is not None:
            output_rows.append(row)

    if not output_rows:
        return _empty_df(OI_DEFENSE_COLUMNS)
    out = pd.DataFrame(output_rows).sort_values("trade_date").tail(window).reset_index(drop=True)
    for col in [
        "underlying_close",
        "call_strike",
        "call_oi",
        "call_distance_pct",
        "put_strike",
        "put_oi",
        "put_distance_pct",
        "total_call_oi",
        "total_put_oi",
        "put_call_oi",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out[OI_DEFENSE_COLUMNS]


def selected_underlying_price(stock_daily: pd.DataFrame, trade_date: str) -> float | None:
    if stock_daily is None or stock_daily.empty or "date" not in stock_daily.columns:
        return None
    target = normalize_trade_date(trade_date)
    df = stock_daily.copy()
    df["trade_date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y%m%d")
    exact = df[df["trade_date"] == target]
    row = exact.iloc[-1] if not exact.empty else df.iloc[-1]
    try:
        close = float(row.get("close"))
        return close if close > 0 else None
    except Exception:
        return None


def _cycle_label(expiration_type: Any, dte: Any) -> str:
    try:
        dte_int = int(dte)
    except Exception:
        dte_int = 999999
    if dte_int <= 0:
        return "0DTE"
    if dte_int == 1:
        return "1DTE"
    exp_type = str(expiration_type or "").strip()
    return exp_type or "unknown"


def load_option_chain_daily(
    underlying: str,
    trade_date: str | dt.date | dt.datetime,
    *,
    include_short_cycle: bool = True,
    use_test_tables: bool = True,
    underlying_price: float | None = None,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(OPTION_CHAIN_COLUMNS)

    df = get_us_option_chain_daily(
        normalize_underlying(underlying),
        normalize_trade_date(trade_date),
        include_short_cycle=include_short_cycle,
        use_test_tables=use_test_tables,
        engine=engine,
    )
    if df is None or df.empty:
        return _empty_df(OPTION_CHAIN_COLUMNS)

    return _finalize_option_chain_frame(df, trade_date, underlying_price=underlying_price)


def _finalize_option_chain_frame(
    df: pd.DataFrame,
    trade_date: str | dt.date | dt.datetime,
    *,
    underlying_price: float | None = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return _empty_df(OPTION_CHAIN_COLUMNS)

    df = df.copy()
    for col in OPTION_CHAIN_COLUMNS:
        if col not in df.columns:
            df[col] = None

    numeric_cols = [
        "strike",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "transactions",
        "open_interest",
        "provider_iv",
        "computed_iv",
        "underlying_price",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if underlying_price is not None:
        df["underlying_price"] = df["underlying_price"].fillna(float(underlying_price))

    trade_date_text = normalize_trade_date(trade_date)
    df["dte"] = df["expiration_date"].apply(
        lambda value: dte_for_trade_date(value, trade_date_text) if str(value or "").strip() else None
    )
    df["cycle_label"] = df.apply(lambda row: _cycle_label(row.get("expiration_type"), row.get("dte")), axis=1)
    df["iv"] = df.apply(
        lambda row: normalize_iv_value(row.get("provider_iv")) or normalize_iv_value(row.get("computed_iv")),
        axis=1,
    )
    df["iv_pct"] = df["iv"].apply(lambda value: value * 100 if value is not None and pd.notna(value) else None)

    price = pd.to_numeric(df["underlying_price"], errors="coerce")
    strike = pd.to_numeric(df["strike"], errors="coerce")
    df["moneyness_pct"] = ((strike - price) / price * 100).where(price > 0)

    sort_cols = ["expiration_date", "strike", "call_put", "option_ticker"]
    return df[OPTION_CHAIN_COLUMNS].sort_values(sort_cols).reset_index(drop=True)


def load_option_surface_snapshot(
    underlying: str,
    trade_date: str | dt.date | dt.datetime,
    *,
    include_short_cycle: bool = True,
    use_test_tables: bool = True,
    underlying_price: float | None = None,
    moneyness_range: float = 10.0,
    max_dte: int = 135,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(OPTION_CHAIN_COLUMNS)

    names = option_table_names(use_test_tables)
    contracts = safe_table_name(names["contracts"])
    daily = safe_table_name(names["daily"])
    iv = safe_table_name(names["iv"])
    if not table_exists(engine, contracts) or not table_exists(engine, daily) or not table_exists(engine, iv):
        return _empty_df(OPTION_CHAIN_COLUMNS)

    trade_date_text = normalize_trade_date(trade_date)
    trade_dt = pd.to_datetime(trade_date_text, format="%Y%m%d", errors="coerce")
    if pd.isna(trade_dt):
        return _empty_df(OPTION_CHAIN_COLUMNS)

    price_param = float(underlying_price) if underlying_price is not None and pd.notna(underlying_price) else None
    price_expr = "COALESCE(h.underlying_price, :underlying_price)"
    where_cycle = "" if include_short_cycle else "AND c.expiration_type = 'monthly'"
    sql = text(
        f"""
        SELECT d.trade_date, d.option_ticker, d.underlying, c.call_put, c.strike,
               c.expiration_date, c.expiration_type, c.settlement_type,
               d.open, d.high, d.low, d.close, d.volume, d.vwap, d.transactions,
               d.open_interest, h.provider_iv, h.computed_iv, h.iv_source,
               {price_expr} AS underlying_price
        FROM {daily} d{_mysql_force_index(engine, "idx_underlying_date")}
        JOIN {contracts} c ON d.option_ticker = c.option_ticker
        LEFT JOIN {iv} h ON d.trade_date = h.trade_date AND d.option_ticker = h.option_ticker
        WHERE d.underlying = :underlying
          AND d.trade_date = :trade_date
          AND c.expiration_date >= :expiration_start
          AND c.expiration_date <= :expiration_end
          AND COALESCE(h.provider_iv, h.computed_iv) IS NOT NULL
          AND {price_expr} > 0
          AND ABS(c.strike - {price_expr}) / {price_expr} <= :moneyness_limit
          {where_cycle}
        ORDER BY c.expiration_date ASC, c.strike ASC, c.call_put ASC
        """
    )
    params = {
        "underlying": normalize_underlying(underlying),
        "trade_date": trade_date_text,
        "underlying_price": price_param,
        "expiration_start": trade_dt.strftime("%Y-%m-%d"),
        "expiration_end": (trade_dt + pd.Timedelta(days=max(int(max_dte or 135), 1))).strftime("%Y-%m-%d"),
        "moneyness_limit": max(float(moneyness_range or 10.0), 0.1) / 100.0,
    }
    try:
        df = pd.read_sql(sql, engine, params=params)
    except Exception:
        return _empty_df(OPTION_CHAIN_COLUMNS)
    return _finalize_option_chain_frame(df, trade_date_text, underlying_price=underlying_price)


def _valid_dte_targets(dte_targets: tuple[int, ...] | list[int] | None = None) -> list[int]:
    values = dte_targets or VOLATILITY_CONE_TARGETS
    out: list[int] = []
    for value in values:
        try:
            target = int(value)
        except Exception:
            continue
        if target > 0 and target not in out:
            out.append(target)
    return sorted(out)


def build_volatility_cone_line(
    chain: pd.DataFrame,
    *,
    dte_targets: tuple[int, ...] | list[int] | None = None,
    moneyness_band: float = 2.5,
) -> pd.DataFrame:
    targets = _valid_dte_targets(dte_targets)
    if chain is None or chain.empty or not targets:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    df = chain.copy()
    for col in ("iv_pct", "open_interest", "moneyness_pct", "dte"):
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if "expiration_date" not in df.columns:
        df["expiration_date"] = ""

    band = max(float(moneyness_band or 2.5), 0.1)
    df = df.dropna(subset=["iv_pct", "moneyness_pct", "dte"])
    df = df[(df["dte"] > 0) & (df["moneyness_pct"].abs() <= band)]
    if df.empty:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    rows: list[dict[str, Any]] = []
    for (expiration, dte), group in df.groupby(["expiration_date", "dte"], dropna=False):
        iv_pct = _weighted_average(group["iv_pct"], group.get("open_interest"))
        if iv_pct is None:
            continue
        rows.append(
            {
                "expiration_date": str(expiration or ""),
                "dte": float(dte),
                "iv_pct": iv_pct,
                "sample_count": int(len(group)),
            }
        )
    expiry_iv = pd.DataFrame(rows)
    if expiry_iv.empty:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    line_rows: list[dict[str, Any]] = []
    for target in targets:
        scoped = expiry_iv.assign(dte_distance=(expiry_iv["dte"] - target).abs())
        selected = scoped.sort_values(["dte_distance", "dte", "expiration_date"]).iloc[0]
        line_rows.append(
            {
                "dte_target": int(target),
                "dte": float(selected["dte"]),
                "expiration_date": str(selected["expiration_date"]),
                "iv_pct": float(selected["iv_pct"]),
                "sample_count": int(selected["sample_count"]),
            }
        )
    return pd.DataFrame(line_rows, columns=VOLATILITY_CONE_LINE_COLUMNS)


def build_otm_volatility_curve(
    chain: pd.DataFrame,
    *,
    target_dte: int = 30,
    dte_min: int = 20,
    dte_max: int = 45,
    moneyness_range: float = 10.0,
    min_abs_moneyness: float = 0.5,
) -> pd.DataFrame:
    return build_binned_otm_volatility_curve(
        chain,
        target_dte=target_dte,
        dte_min=dte_min,
        dte_max=dte_max,
        moneyness_range=moneyness_range,
        min_abs_moneyness=min_abs_moneyness,
    )


def _curve_side_point_counts(curve: pd.DataFrame) -> dict[str, int]:
    if curve is None or curve.empty or "call_put" not in curve.columns:
        return {"P": 0, "C": 0}
    counts = curve["call_put"].fillna("").astype(str).str.upper().value_counts()
    return {"P": int(counts.get("P", 0)), "C": int(counts.get("C", 0))}


def _curve_quality_from_side_counts(curve: pd.DataFrame) -> pd.Series:
    if curve is None or curve.empty or "call_put" not in curve.columns:
        return pd.Series(dtype=object)
    side_counts = curve.groupby("call_put")["moneyness_pct"].transform("count")
    return pd.Series(
        ["sparse" if count < OTM_VOLATILITY_CURVE_MIN_SIDE_POINTS else "ok" for count in side_counts],
        index=curve.index,
    )


def _raw_otm_curve_from_candidates(
    df: pd.DataFrame,
    *,
    target_dte: int,
    max_points_per_side: int = 9,
) -> pd.DataFrame:
    if df is None or df.empty:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)
    raw = df.copy()
    raw["base_weight"] = pd.to_numeric(raw.get("open_interest"), errors="coerce").fillna(0)
    raw.loc[raw["base_weight"] <= 0, "base_weight"] = 1.0
    raw["dte_weight"] = 1.0 / (1.0 + (pd.to_numeric(raw["dte"], errors="coerce") - int(target_dte)).abs())
    raw["curve_weight"] = raw["base_weight"] * raw["dte_weight"].fillna(0)
    raw.loc[raw["curve_weight"] <= 0, "curve_weight"] = raw["base_weight"]
    raw["sample_count"] = pd.to_numeric(raw.get("sample_count"), errors="coerce").fillna(1)

    rows: list[dict[str, Any]] = []
    for (moneyness, call_put), group in raw.groupby(["moneyness_pct", "call_put"], dropna=False):
        iv_pct = _weighted_average(group["iv_pct"], group.get("curve_weight"))
        if iv_pct is None:
            continue
        expirations_text = ",".join(sorted(str(value) for value in group["expiration_date"].dropna().astype(str).unique() if value))
        rows.append(
            {
                "moneyness_pct": float(moneyness),
                "iv_pct": iv_pct,
                "call_put": str(call_put or ""),
                "expiration_date": expirations_text,
                "dte": float(_weighted_average(group["dte"], group.get("curve_weight")) or pd.to_numeric(group["dte"], errors="coerce").median()),
                "point_count": int(pd.to_numeric(group.get("sample_count"), errors="coerce").fillna(1).sum()),
                "expiration_count": int(group["expiration_date"].dropna().astype(str).nunique()),
                "quality": "ok",
            }
        )
    if not rows:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    out = pd.DataFrame(rows, columns=OTM_VOLATILITY_CURVE_COLUMNS).sort_values("moneyness_pct").reset_index(drop=True)
    parts: list[pd.DataFrame] = []
    for side in ("P", "C"):
        side_df = out[out["call_put"] == side].sort_values("moneyness_pct").reset_index(drop=True)
        if side_df.empty:
            continue
        max_points = max(OTM_VOLATILITY_CURVE_MIN_SIDE_POINTS, int(max_points_per_side or 9))
        if len(side_df) > max_points:
            positions = sorted({round(idx * (len(side_df) - 1) / (max_points - 1)) for idx in range(max_points)})
            side_df = side_df.iloc[positions].copy()
        parts.append(side_df)
    if not parts:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    out = pd.concat(parts, ignore_index=True).sort_values("moneyness_pct").reset_index(drop=True)
    out["quality"] = _curve_quality_from_side_counts(out)
    out.loc[out["quality"] == "ok", "quality"] = "raw"
    return out[OTM_VOLATILITY_CURVE_COLUMNS]


def _curve_score(curve: pd.DataFrame) -> int:
    if curve is None or curve.empty:
        return 0
    counts = _curve_side_point_counts(curve)
    usable_sides = sum(1 for count in counts.values() if count >= OTM_VOLATILITY_CURVE_MIN_SIDE_POINTS)
    return int(len(curve)) + usable_sides * 20


def _select_front_otm_curve_expiration(df: pd.DataFrame) -> str | None:
    if df is None or df.empty:
        return None
    stats = (
        df.copy()
        .assign(dte_numeric=pd.to_numeric(df.get("dte"), errors="coerce"))
        .dropna(subset=["expiration_date", "dte_numeric"])
    )
    if stats.empty:
        return None
    by_expiration = (
        stats.groupby("expiration_date", dropna=False)
        .agg(dte=("dte_numeric", "median"), total_count=("dte_numeric", "size"))
        .reset_index()
        .sort_values(["dte", "expiration_date"], ascending=[True, True])
    )
    if by_expiration.empty:
        return None
    return str(by_expiration.iloc[0]["expiration_date"] or "")


def build_binned_otm_volatility_curve(
    chain: pd.DataFrame,
    *,
    target_dte: int = 30,
    dte_min: int = 20,
    dte_max: int = 45,
    moneyness_range: float = 10.0,
    min_abs_moneyness: float = 0.5,
    grid: tuple[float, ...] | list[float] | None = None,
    primary_radius: float = 0.85,
    fallback_radius: float = 1.25,
) -> pd.DataFrame:
    if chain is None or chain.empty:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    df = chain.copy()
    for col in ("iv_pct", "open_interest", "moneyness_pct", "dte", "sample_count"):
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ("call_put", "expiration_date"):
        if col not in df.columns:
            df[col] = ""

    span = max(float(moneyness_range or 10.0), 0.1)
    min_abs = max(float(min_abs_moneyness or 0.0), 0.0)
    df["call_put"] = df["call_put"].astype(str).str.upper()
    df = df.dropna(subset=["iv_pct", "moneyness_pct", "dte"])
    df = df[df["moneyness_pct"].between(-span, span)]
    df = df[df["dte"].between(int(dte_min), int(dte_max))]
    df = df[
        ((df["call_put"] == "P") & (df["moneyness_pct"] < 0))
        | ((df["call_put"] == "C") & (df["moneyness_pct"] > 0))
    ]
    if min_abs > 0:
        df = df[df["moneyness_pct"].abs() >= min_abs]
    if df.empty:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    df["expiration_date"] = df["expiration_date"].fillna("").astype(str)
    expiration = _select_front_otm_curve_expiration(df)
    if expiration:
        df = df[df["expiration_date"] == expiration].copy()
    if df.empty:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    grid_values = [float(value) for value in (grid or OTM_VOLATILITY_CURVE_GRID)]
    grid_values = sorted(value for value in grid_values if abs(value) <= span and abs(value) >= min_abs and value != 0)
    if not grid_values:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    max_radius = max(float(primary_radius or 0.0), float(fallback_radius or 0.0), 0.1)
    assignments: list[pd.DataFrame] = []
    for side, sign in (("P", -1), ("C", 1)):
        side_df = df[df["call_put"] == side].copy()
        side_grid = [value for value in grid_values if value * sign > 0]
        if side_df.empty or not side_grid:
            continue
        grid_frame = pd.DataFrame({"grid_moneyness_pct": side_grid})
        joined = side_df.merge(grid_frame, how="cross")
        joined["grid_distance"] = (joined["moneyness_pct"] - joined["grid_moneyness_pct"]).abs()
        joined = joined[joined["grid_distance"] <= max_radius]
        if joined.empty:
            continue
        joined["grid_abs"] = joined["grid_moneyness_pct"].abs()
        joined = joined.sort_values(
            [
                "option_ticker" if "option_ticker" in joined.columns else "expiration_date",
                "grid_distance",
                "grid_abs",
            ],
            ascending=[True, True, False],
        )
        row_keys = ["expiration_date", "call_put", "moneyness_pct", "iv_pct", "dte"]
        if "strike" in joined.columns:
            row_keys.append("strike")
        joined = joined.drop_duplicates(subset=row_keys, keep="first")
        assignments.append(joined)
    if not assignments:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    curve = pd.concat(assignments, ignore_index=True)
    curve["base_weight"] = pd.to_numeric(curve.get("open_interest"), errors="coerce").fillna(0)
    curve.loc[curve["base_weight"] <= 0, "base_weight"] = 1.0
    curve["dte_weight"] = 1.0 / (1.0 + (pd.to_numeric(curve["dte"], errors="coerce") - int(target_dte)).abs())
    curve["curve_weight"] = curve["base_weight"] * curve["dte_weight"].fillna(0)
    curve.loc[curve["curve_weight"] <= 0, "curve_weight"] = curve["base_weight"]
    curve["sample_count"] = pd.to_numeric(curve.get("sample_count"), errors="coerce").fillna(1)

    rows: list[dict[str, Any]] = []
    for (moneyness, call_put), group in curve.groupby(["grid_moneyness_pct", "call_put"], dropna=False):
        iv_pct = _weighted_average(group["iv_pct"], group.get("curve_weight"))
        if iv_pct is None:
            continue
        expirations_text = ",".join(sorted(str(value) for value in group["expiration_date"].dropna().astype(str).unique() if value))
        point_count = int(pd.to_numeric(group.get("sample_count"), errors="coerce").fillna(1).sum())
        rows.append(
            {
                "moneyness_pct": float(moneyness),
                "iv_pct": iv_pct,
                "call_put": str(call_put or ""),
                "expiration_date": expirations_text,
                "dte": float(_weighted_average(group["dte"], group.get("curve_weight")) or pd.to_numeric(group["dte"], errors="coerce").median()),
                "point_count": point_count,
                "expiration_count": int(group["expiration_date"].dropna().astype(str).nunique()),
                "quality": "ok",
            }
        )
    if not rows:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)
    out = pd.DataFrame(rows)
    out["quality"] = _curve_quality_from_side_counts(out)
    out.loc[out["quality"] == "ok", "quality"] = "grid"
    out = out[OTM_VOLATILITY_CURVE_COLUMNS].sort_values("moneyness_pct").reset_index(drop=True)
    raw_out = _raw_otm_curve_from_candidates(df, target_dte=target_dte)
    if not raw_out.empty and (_curve_score(raw_out) > _curve_score(out) or (out["quality"] == "sparse").any()):
        return raw_out
    return out


def load_volatility_cone_line_snapshot(
    underlying: str,
    trade_date: str | dt.date | dt.datetime,
    *,
    include_short_cycle: bool = True,
    use_test_tables: bool = True,
    underlying_price: float | None = None,
    dte_targets: tuple[int, ...] | list[int] | None = None,
    moneyness_band: float = 2.5,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    targets = _valid_dte_targets(dte_targets)
    if engine is None or not targets:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    names = option_table_names(use_test_tables)
    contracts_table = safe_table_name(names["contracts"])
    iv_table = safe_table_name(names["iv"])
    if not table_exists(engine, contracts_table) or not table_exists(engine, iv_table):
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    iv_columns = table_columns(engine, iv_table)
    contract_columns = table_columns(engine, contracts_table)
    required_iv = {"trade_date", "option_ticker", "underlying", "provider_iv", "computed_iv", "underlying_price"}
    required_contracts = {"option_ticker", "strike", "expiration_date"}
    if not required_iv.issubset(iv_columns) or not required_contracts.issubset(contract_columns):
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    underlying = normalize_underlying(underlying)
    trade_date_text = normalize_trade_date(trade_date)
    trade_dt = pd.to_datetime(trade_date_text, format="%Y%m%d", errors="coerce")
    if not underlying or pd.isna(trade_dt):
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    price_param = float(underlying_price) if underlying_price is not None and pd.notna(underlying_price) else None
    price_expr = "COALESCE(h.underlying_price, :underlying_price)"
    iv_value_expr = (
        "CASE WHEN COALESCE(h.provider_iv, h.computed_iv) > 3 "
        "THEN COALESCE(h.provider_iv, h.computed_iv) / 100.0 "
        "ELSE COALESCE(h.provider_iv, h.computed_iv) END"
    )
    weight_expr = (
        "CASE WHEN h.open_interest IS NOT NULL AND h.open_interest > 0 THEN h.open_interest ELSE 1 END"
        if "open_interest" in iv_columns
        else "1"
    )
    where_cycle = "" if include_short_cycle else "AND c.expiration_type = 'monthly'"
    sql = text(
        f"""
        SELECT c.expiration_date,
               SUM(({iv_value_expr}) * ({weight_expr})) / NULLIF(SUM({weight_expr}), 0) * 100.0 AS iv_pct,
               COUNT(*) AS sample_count
        FROM {iv_table} h{_mysql_force_index(engine, "idx_underlying_date")}
        JOIN {contracts_table} c ON h.option_ticker = c.option_ticker
        WHERE h.underlying = :underlying
          AND h.trade_date = :trade_date
          AND COALESCE(h.provider_iv, h.computed_iv) IS NOT NULL
          AND {price_expr} > 0
          AND c.expiration_date >= :expiration_start
          AND c.expiration_date <= :expiration_end
          AND ABS(c.strike - {price_expr}) / {price_expr} <= :moneyness_limit
          {where_cycle}
        GROUP BY c.expiration_date
        ORDER BY c.expiration_date
        """
    )
    params = {
        "underlying": underlying,
        "trade_date": trade_date_text,
        "underlying_price": price_param,
        "expiration_start": trade_dt.strftime("%Y-%m-%d"),
        "expiration_end": (trade_dt + pd.Timedelta(days=max(targets) + 45)).strftime("%Y-%m-%d"),
        "moneyness_limit": max(float(moneyness_band or 2.5), 0.1) / 100.0,
    }
    try:
        raw = pd.read_sql(sql, engine, params=params)
    except Exception:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)
    if raw.empty:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    raw["iv_pct"] = pd.to_numeric(raw.get("iv_pct"), errors="coerce")
    raw["sample_count"] = pd.to_numeric(raw.get("sample_count"), errors="coerce").fillna(0)
    raw["dte"] = raw["expiration_date"].apply(lambda value: dte_for_trade_date(value, trade_date_text))
    raw = raw.dropna(subset=["iv_pct", "dte"])
    if raw.empty:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    rows: list[dict[str, Any]] = []
    for target in targets:
        scoped = raw.assign(dte_distance=(pd.to_numeric(raw["dte"], errors="coerce") - target).abs())
        scoped = scoped.dropna(subset=["dte_distance", "iv_pct"])
        if scoped.empty:
            continue
        selected = scoped.sort_values(["dte_distance", "dte", "expiration_date"]).iloc[0]
        rows.append(
            {
                "dte_target": int(target),
                "dte": float(selected["dte"]),
                "expiration_date": str(selected["expiration_date"] or ""),
                "iv_pct": float(selected["iv_pct"]),
                "sample_count": int(selected["sample_count"]),
            }
        )
    if not rows:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)
    return pd.DataFrame(rows, columns=VOLATILITY_CONE_LINE_COLUMNS)


def load_otm_volatility_curve_snapshot(
    underlying: str,
    trade_date: str | dt.date | dt.datetime,
    *,
    include_short_cycle: bool = True,
    use_test_tables: bool = True,
    underlying_price: float | None = None,
    target_dte: int = 30,
    dte_min: int = 7,
    dte_max: int = 60,
    moneyness_range: float = 10.0,
    min_abs_moneyness: float = 0.5,
    prefer_monthly_expiration: bool = True,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    names = option_table_names(use_test_tables)
    contracts_table = safe_table_name(names["contracts"])
    iv_table = safe_table_name(names["iv"])
    if not table_exists(engine, contracts_table) or not table_exists(engine, iv_table):
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    iv_columns = table_columns(engine, iv_table)
    contract_columns = table_columns(engine, contracts_table)
    required_iv = {"trade_date", "option_ticker", "underlying", "provider_iv", "computed_iv", "underlying_price"}
    required_contracts = {"option_ticker", "call_put", "strike", "expiration_date"}
    if not required_iv.issubset(iv_columns) or not required_contracts.issubset(contract_columns):
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    underlying = normalize_underlying(underlying)
    trade_date_text = normalize_trade_date(trade_date)
    trade_dt = pd.to_datetime(trade_date_text, format="%Y%m%d", errors="coerce")
    if not underlying or pd.isna(trade_dt):
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    price_param = float(underlying_price) if underlying_price is not None and pd.notna(underlying_price) else None
    price_expr = "COALESCE(h.underlying_price, :underlying_price)"
    iv_value_expr = (
        "CASE WHEN COALESCE(h.provider_iv, h.computed_iv) > 3 "
        "THEN COALESCE(h.provider_iv, h.computed_iv) / 100.0 "
        "ELSE COALESCE(h.provider_iv, h.computed_iv) END"
    )
    weight_expr = (
        "CASE WHEN h.open_interest IS NOT NULL AND h.open_interest > 0 THEN h.open_interest ELSE 1 END"
        if "open_interest" in iv_columns
        else "1"
    )
    has_expiration_type = "expiration_type" in contract_columns
    expiration_type_expr = "c.expiration_type" if has_expiration_type else "NULL"
    if has_expiration_type and (prefer_monthly_expiration or not include_short_cycle):
        where_cycle = "AND c.expiration_type = 'monthly'"
    else:
        where_cycle = ""
    dte_min_value = max(int(dte_min), 1)
    dte_max_value = max(int(dte_max), dte_min_value)
    span = max(float(moneyness_range or 10.0), 0.1)
    sql = text(
        f"""
        SELECT c.call_put,
               c.strike,
               c.expiration_date,
               {expiration_type_expr} AS expiration_type,
               {price_expr} AS underlying_price,
               SUM(({iv_value_expr}) * ({weight_expr})) / NULLIF(SUM({weight_expr}), 0) * 100.0 AS iv_pct,
               SUM({weight_expr}) AS open_interest,
               COUNT(*) AS sample_count
        FROM {iv_table} h{_mysql_force_index(engine, "idx_underlying_date")}
        JOIN {contracts_table} c ON h.option_ticker = c.option_ticker
        WHERE h.underlying = :underlying
          AND h.trade_date = :trade_date
          AND COALESCE(h.provider_iv, h.computed_iv) IS NOT NULL
          AND {price_expr} > 0
          AND c.expiration_date >= :expiration_start
          AND c.expiration_date <= :expiration_end
          AND ABS(c.strike - {price_expr}) / {price_expr} <= :moneyness_limit
          {where_cycle}
        GROUP BY c.expiration_date, c.call_put, c.strike, {price_expr}
        ORDER BY c.expiration_date, c.strike, c.call_put
        """
    )
    params = {
        "underlying": underlying,
        "trade_date": trade_date_text,
        "underlying_price": price_param,
        "expiration_start": (trade_dt + pd.Timedelta(days=dte_min_value)).strftime("%Y-%m-%d"),
        "expiration_end": (trade_dt + pd.Timedelta(days=dte_max_value)).strftime("%Y-%m-%d"),
        "moneyness_limit": span / 100.0,
    }
    try:
        raw = pd.read_sql(sql, engine, params=params)
    except Exception:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)
    if raw.empty:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    raw["call_put"] = raw.get("call_put", "").astype(str).str.upper()
    for col in ("strike", "underlying_price", "iv_pct", "open_interest", "sample_count"):
        raw[col] = pd.to_numeric(raw.get(col), errors="coerce")
    raw["dte"] = raw["expiration_date"].apply(lambda value: dte_for_trade_date(value, trade_date_text))
    raw["moneyness_pct"] = ((raw["strike"] - raw["underlying_price"]) / raw["underlying_price"] * 100).where(
        raw["underlying_price"] > 0
    )
    raw = raw.dropna(subset=["iv_pct", "moneyness_pct", "dte"])
    raw = raw[raw["dte"].between(dte_min_value, dte_max_value)]
    if raw.empty:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    return build_binned_otm_volatility_curve(
        raw,
        target_dte=target_dte,
        dte_min=dte_min_value,
        dte_max=dte_max_value,
        moneyness_range=span,
        min_abs_moneyness=min_abs_moneyness,
    )


def _normalize_cone_source_frame(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or raw.empty:
        return _empty_df(OPTION_CHAIN_COLUMNS)
    df = raw.copy()
    df["trade_date"] = df["trade_date"].apply(normalize_trade_date)
    for col in ("provider_iv", "computed_iv", "open_interest", "underlying_price", "strike"):
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["iv"] = df.apply(
        lambda row: normalize_iv_value(row.get("provider_iv")) or normalize_iv_value(row.get("computed_iv")),
        axis=1,
    )
    df["iv_pct"] = df["iv"].apply(lambda value: value * 100 if value is not None and pd.notna(value) else None)
    df["dte"] = df.apply(lambda row: dte_for_trade_date(row.get("expiration_date"), row.get("trade_date")), axis=1)
    price = pd.to_numeric(df["underlying_price"], errors="coerce")
    strike = pd.to_numeric(df["strike"], errors="coerce")
    df["moneyness_pct"] = ((strike - price) / price * 100).where(price > 0)
    return df


def _percentile_cone_from_daily_rows(history: pd.DataFrame) -> pd.DataFrame:
    if history is None or history.empty:
        return _empty_df(VOLATILITY_CONE_COLUMNS)
    source = history.copy()
    source["dte_target"] = pd.to_numeric(source.get("dte_target"), errors="coerce")
    source["iv_pct"] = pd.to_numeric(source.get("iv_pct"), errors="coerce")
    source = source.dropna(subset=["dte_target", "iv_pct"])
    if source.empty:
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    rows: list[dict[str, Any]] = []
    for target in sorted(source["dte_target"].dropna().astype(int).unique().tolist()):
        values = pd.to_numeric(source.loc[source["dte_target"] == target, "iv_pct"], errors="coerce").dropna()
        if values.empty:
            continue
        rows.append(
            {
                "dte_target": int(target),
                "p10": float(values.quantile(0.10)),
                "p25": float(values.quantile(0.25)),
                "p50": float(values.quantile(0.50)),
                "p75": float(values.quantile(0.75)),
                "p90": float(values.quantile(0.90)),
                "sample_count": int(len(values)),
            }
        )
    if not rows:
        return _empty_df(VOLATILITY_CONE_COLUMNS)
    return pd.DataFrame(rows, columns=VOLATILITY_CONE_COLUMNS).sort_values("dte_target").reset_index(drop=True)


def _load_cached_volatility_cone_history(
    underlying: str,
    end_date: str,
    *,
    window: int,
    dte_targets: tuple[int, ...] | list[int] | None = None,
    engine=None,
) -> pd.DataFrame:
    if engine is None or not table_exists(engine, VOLATILITY_CONE_DAILY_CACHE_TABLE):
        return _empty_df(VOLATILITY_CONE_COLUMNS)
    columns = table_columns(engine, VOLATILITY_CONE_DAILY_CACHE_TABLE)
    required = {"trade_date", "underlying", "dte_target", "iv_pct"}
    if not required.issubset(columns):
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    targets = _valid_dte_targets(dte_targets)
    date_limit = min(max(int(window or 252), 2), 500)
    target_clause = ""
    params: dict[str, Any] = {"underlying": underlying, "end_date": end_date}
    if targets:
        placeholders, target_params = _named_in_clause("target", targets)
        params.update(target_params)
        target_clause = f"AND dte_target IN ({placeholders})"

    dates_sql = text(
        f"""
        SELECT trade_date
        FROM {VOLATILITY_CONE_DAILY_CACHE_TABLE}
        WHERE underlying = :underlying
          AND trade_date <= :end_date
          {target_clause}
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT {date_limit}
        """
    )
    try:
        dates_df = pd.read_sql(dates_sql, engine, params=params)
    except Exception:
        return _empty_df(VOLATILITY_CONE_COLUMNS)
    if dates_df.empty:
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    date_values = [normalize_trade_date(value) for value in dates_df["trade_date"].tolist()]
    date_values = [value for value in date_values if value]
    if not date_values or max(date_values) < end_date:
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    date_placeholders, date_params = _named_in_clause("cache_date", date_values)
    read_params = {"underlying": underlying, **date_params}
    if targets:
        target_placeholders, target_params = _named_in_clause("cache_target", targets)
        read_params.update(target_params)
        read_target_clause = f"AND dte_target IN ({target_placeholders})"
    else:
        read_target_clause = ""
    sql = text(
        f"""
        SELECT trade_date, dte_target, iv_pct
        FROM {VOLATILITY_CONE_DAILY_CACHE_TABLE}
        WHERE underlying = :underlying
          AND trade_date IN ({date_placeholders})
          {read_target_clause}
        ORDER BY trade_date, dte_target
        """
    )
    try:
        history = pd.read_sql(sql, engine, params=read_params)
    except Exception:
        return _empty_df(VOLATILITY_CONE_COLUMNS)
    return _percentile_cone_from_daily_rows(history)


def _cone_history_sample_days(cone: pd.DataFrame) -> int:
    if cone is None or cone.empty or "sample_count" not in cone.columns:
        return 0
    counts = pd.to_numeric(cone["sample_count"], errors="coerce").dropna()
    counts = counts[counts > 0]
    if counts.empty:
        return 0
    return int(counts.min())


def load_volatility_cone_history(
    underlying: str,
    end_date: str | dt.date | dt.datetime,
    *,
    window: int = 252,
    dte_targets: tuple[int, ...] | list[int] | None = None,
    moneyness_band: float = 2.5,
    use_test_tables: bool = True,
    prefer_cache: bool = True,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    targets = _valid_dte_targets(dte_targets)
    if engine is None or not targets:
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    underlying = normalize_underlying(underlying)
    end_text = normalize_trade_date(end_date)
    if not underlying or not end_text:
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    date_limit = min(max(int(window or 252), 2), 500)
    cached = _empty_df(VOLATILITY_CONE_COLUMNS)
    if prefer_cache:
        cached = _load_cached_volatility_cone_history(
            underlying,
            end_text,
            window=date_limit,
            dte_targets=targets,
            engine=engine,
        )
        min_cache_days = min(VOLATILITY_CONE_MIN_CACHE_DAYS, date_limit)
        if not cached.empty and _cone_history_sample_days(cached) >= min_cache_days:
            return cached

    names = option_table_names(use_test_tables)
    iv_table = safe_table_name(names["iv"])
    contracts_table = safe_table_name(names["contracts"])
    if not table_exists(engine, iv_table) or not table_exists(engine, contracts_table):
        return cached if not cached.empty else _empty_df(VOLATILITY_CONE_COLUMNS)

    iv_columns = table_columns(engine, iv_table)
    contract_columns = table_columns(engine, contracts_table)
    required_iv = {"trade_date", "option_ticker", "underlying", "provider_iv", "computed_iv", "underlying_price"}
    required_contracts = {"option_ticker", "strike", "expiration_date"}
    if not required_iv.issubset(iv_columns) or not required_contracts.issubset(contract_columns):
        return cached if not cached.empty else _empty_df(VOLATILITY_CONE_COLUMNS)

    dates_sql = text(
        f"""
        SELECT trade_date
        FROM {iv_table}{_mysql_force_index(engine, "idx_underlying_date")}
        WHERE underlying = :underlying
          AND trade_date <= :end_date
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT {date_limit}
        """
    )
    try:
        dates_df = pd.read_sql(dates_sql, engine, params={"underlying": underlying, "end_date": end_text})
    except Exception:
        return cached if not cached.empty else _empty_df(VOLATILITY_CONE_COLUMNS)
    if dates_df.empty:
        return cached if not cached.empty else _empty_df(VOLATILITY_CONE_COLUMNS)

    date_values = [normalize_trade_date(value) for value in dates_df["trade_date"].tolist()]
    date_values = [value for value in date_values if value]
    if not date_values:
        return cached if not cached.empty else _empty_df(VOLATILITY_CONE_COLUMNS)

    start_date = min(date_values)
    end_dt = pd.to_datetime(end_text, format="%Y%m%d", errors="coerce")
    start_dt = pd.to_datetime(start_date, format="%Y%m%d", errors="coerce")
    if pd.isna(end_dt) or pd.isna(start_dt):
        return cached if not cached.empty else _empty_df(VOLATILITY_CONE_COLUMNS)
    expiration_start = start_dt.strftime("%Y-%m-%d")
    expiration_end = (end_dt + pd.Timedelta(days=max(targets) + 45)).strftime("%Y-%m-%d")
    placeholders, params = _named_in_clause("date", date_values)
    params.update(
        {
            "underlying": underlying,
            "expiration_start": expiration_start,
            "expiration_end": expiration_end,
            "moneyness_limit": max(float(moneyness_band or 2.5), 0.1) / 100.0,
        }
    )
    iv_value_expr = (
        "CASE WHEN COALESCE(h.provider_iv, h.computed_iv) > 3 "
        "THEN COALESCE(h.provider_iv, h.computed_iv) / 100.0 "
        "ELSE COALESCE(h.provider_iv, h.computed_iv) END"
    )
    weight_expr = (
        "CASE WHEN h.open_interest IS NOT NULL AND h.open_interest > 0 THEN h.open_interest ELSE 1 END"
        if "open_interest" in iv_columns
        else "1"
    )
    sql = text(
        f"""
        SELECT h.trade_date,
               c.expiration_date,
               SUM(({iv_value_expr}) * ({weight_expr})) / NULLIF(SUM({weight_expr}), 0) * 100.0 AS iv_pct,
               COUNT(*) AS sample_count
        FROM {iv_table} h{_mysql_force_index(engine, "idx_underlying_date")}
        JOIN {contracts_table} c ON h.option_ticker = c.option_ticker
        WHERE h.underlying = :underlying
          AND h.trade_date IN ({placeholders})
          AND COALESCE(h.provider_iv, h.computed_iv) IS NOT NULL
          AND h.underlying_price > 0
          AND c.expiration_date >= :expiration_start
          AND c.expiration_date <= :expiration_end
          AND ABS(c.strike - h.underlying_price) / h.underlying_price <= :moneyness_limit
        GROUP BY h.trade_date, c.expiration_date
        ORDER BY h.trade_date, c.expiration_date
        """
    )
    try:
        raw = pd.read_sql(sql, engine, params=params)
    except Exception:
        return cached if not cached.empty else _empty_df(VOLATILITY_CONE_COLUMNS)
    if raw.empty:
        return cached if not cached.empty else _empty_df(VOLATILITY_CONE_COLUMNS)

    source = raw.copy()
    source["trade_date"] = source["trade_date"].apply(normalize_trade_date)
    source["iv_pct"] = pd.to_numeric(source.get("iv_pct"), errors="coerce")
    source["sample_count"] = pd.to_numeric(source.get("sample_count"), errors="coerce").fillna(0)
    source["dte"] = source.apply(lambda row: dte_for_trade_date(row.get("expiration_date"), row.get("trade_date")), axis=1)
    source = source.dropna(subset=["trade_date", "iv_pct", "dte"])
    if source.empty:
        return cached if not cached.empty else _empty_df(VOLATILITY_CONE_COLUMNS)

    line_rows: list[pd.DataFrame] = []
    for trade_date, day in source.groupby("trade_date", dropna=False):
        day = day.copy()
        for target in targets:
            scoped = day.assign(dte_distance=(pd.to_numeric(day["dte"], errors="coerce") - target).abs())
            scoped = scoped.dropna(subset=["dte_distance", "iv_pct"])
            if scoped.empty:
                continue
            selected = scoped.sort_values(["dte_distance", "dte", "expiration_date"]).iloc[0]
            line_rows.append(
                pd.DataFrame(
                    [
                        {
                            "trade_date": str(trade_date),
                            "dte_target": int(target),
                            "iv_pct": float(selected["iv_pct"]),
                        }
                    ]
                )
            )
    if not line_rows:
        return cached if not cached.empty else _empty_df(VOLATILITY_CONE_COLUMNS)

    dynamic = _percentile_cone_from_daily_rows(pd.concat(line_rows, ignore_index=True))
    return dynamic if not dynamic.empty else cached


def summarize_option_chain(chain: pd.DataFrame) -> dict[str, Any]:
    if chain is None or chain.empty:
        return {
            "rows": 0,
            "monthly": 0,
            "short_cycle": 0,
            "zero_dte": 0,
            "one_dte": 0,
            "expirations": 0,
            "provider_iv_rows": 0,
            "computed_iv_rows": 0,
            "open_interest_rows": 0,
        }
    expiration_type = chain.get("expiration_type", pd.Series(dtype=object)).astype(str)
    dte = pd.to_numeric(chain.get("dte", pd.Series(dtype=float)), errors="coerce")
    return {
        "rows": int(len(chain)),
        "monthly": int((expiration_type == "monthly").sum()),
        "short_cycle": int((expiration_type != "monthly").sum()),
        "zero_dte": int((dte <= 0).sum()),
        "one_dte": int((dte == 1).sum()),
        "expirations": int(chain.get("expiration_date", pd.Series(dtype=object)).nunique()),
        "provider_iv_rows": int(chain.get("provider_iv", pd.Series(dtype=float)).notna().sum()),
        "computed_iv_rows": int(chain.get("computed_iv", pd.Series(dtype=float)).notna().sum()),
        "open_interest_rows": int(chain.get("open_interest", pd.Series(dtype=float)).notna().sum()),
    }


def option_chain_empty_summary() -> dict[str, int]:
    return {
        "rows": 0,
        "monthly": 0,
        "short_cycle": 0,
        "zero_dte": 0,
        "one_dte": 0,
        "expirations": 0,
        "provider_iv_rows": 0,
        "computed_iv_rows": 0,
        "open_interest_rows": 0,
    }


def load_option_chain_summary(
    underlying: str,
    trade_date: str | dt.date | dt.datetime,
    *,
    include_short_cycle: bool = True,
    include_iv_counts: bool = True,
    use_test_tables: bool = True,
    engine=None,
) -> dict[str, int]:
    engine = engine or dashboard_engine()
    if engine is None:
        return option_chain_empty_summary()

    names = option_table_names(use_test_tables)
    daily = safe_table_name(names["daily"])
    contracts = safe_table_name(names["contracts"])
    iv = safe_table_name(names["iv"])
    if not table_exists(engine, daily) or not table_exists(engine, contracts):
        return option_chain_empty_summary()

    daily_columns = table_columns(engine, daily)
    contract_columns = table_columns(engine, contracts)
    if not {"trade_date", "underlying", "option_ticker", "open_interest"}.issubset(daily_columns):
        return option_chain_empty_summary()
    if not {"option_ticker", "expiration_date", "expiration_type"}.issubset(contract_columns):
        return option_chain_empty_summary()

    trade_date_text = normalize_trade_date(trade_date)
    underlying = normalize_underlying(underlying)
    short_cycle_clause = "" if include_short_cycle else "AND c.expiration_type = 'monthly'"
    iv_join = ""
    provider_expr = "0"
    computed_expr = "0"
    if include_iv_counts and table_exists(engine, iv):
        iv_columns = table_columns(engine, iv)
        if {"trade_date", "option_ticker", "provider_iv", "computed_iv"}.issubset(iv_columns):
            iv_join = (
                f"LEFT JOIN {iv} h "
                "ON h.trade_date = d.trade_date AND h.option_ticker = d.option_ticker"
            )
            provider_expr = "CASE WHEN h.provider_iv IS NOT NULL THEN 1 ELSE 0 END"
            computed_expr = "CASE WHEN h.computed_iv IS NOT NULL THEN 1 ELSE 0 END"

    if getattr(getattr(engine, "dialect", None), "name", "") in {"mysql", "mariadb"}:
        dte_expr = "DATEDIFF(STR_TO_DATE(c.expiration_date, '%Y-%m-%d'), STR_TO_DATE(d.trade_date, '%Y%m%d'))"
        sql = text(
            f"""
            SELECT
                COUNT(*) AS rows_count,
                SUM(CASE WHEN c.expiration_type = 'monthly' THEN 1 ELSE 0 END) AS monthly_count,
                SUM(CASE WHEN c.expiration_type = 'monthly' THEN 0 ELSE 1 END) AS short_cycle_count,
                SUM(CASE WHEN {dte_expr} <= 0 THEN 1 ELSE 0 END) AS zero_dte_count,
                SUM(CASE WHEN {dte_expr} = 1 THEN 1 ELSE 0 END) AS one_dte_count,
                COUNT(DISTINCT c.expiration_date) AS expiration_count,
                SUM({provider_expr}) AS provider_iv_count,
                SUM({computed_expr}) AS computed_iv_count,
                SUM(CASE WHEN d.open_interest IS NOT NULL THEN 1 ELSE 0 END) AS open_interest_count
            FROM {daily} d
            JOIN {contracts} c ON d.option_ticker = c.option_ticker
            {iv_join}
            WHERE d.underlying = :underlying
              AND d.trade_date = :trade_date
              {short_cycle_clause}
            """
        )
        try:
            row = pd.read_sql(sql, engine, params={"underlying": underlying, "trade_date": trade_date_text})
            if not row.empty:
                data = row.iloc[0].to_dict()
                return {
                    "rows": int(data.get("rows_count") or 0),
                    "monthly": int(data.get("monthly_count") or 0),
                    "short_cycle": int(data.get("short_cycle_count") or 0),
                    "zero_dte": int(data.get("zero_dte_count") or 0),
                    "one_dte": int(data.get("one_dte_count") or 0),
                    "expirations": int(data.get("expiration_count") or 0),
                    "provider_iv_rows": int(data.get("provider_iv_count") or 0),
                    "computed_iv_rows": int(data.get("computed_iv_count") or 0),
                    "open_interest_rows": int(data.get("open_interest_count") or 0),
                }
        except Exception:
            pass

    selected_cols = [
        "d.trade_date AS trade_date",
        "d.open_interest AS open_interest",
        "c.expiration_date AS expiration_date",
        "c.expiration_type AS expiration_type",
    ]
    if iv_join:
        selected_cols.extend(["h.provider_iv AS provider_iv", "h.computed_iv AS computed_iv"])
    else:
        selected_cols.extend(["NULL AS provider_iv", "NULL AS computed_iv"])
    sql = text(
        f"""
        SELECT {", ".join(selected_cols)}
        FROM {daily} d
        JOIN {contracts} c ON d.option_ticker = c.option_ticker
        {iv_join}
        WHERE d.underlying = :underlying
          AND d.trade_date = :trade_date
          {short_cycle_clause}
        """
    )
    try:
        df = pd.read_sql(sql, engine, params={"underlying": underlying, "trade_date": trade_date_text})
    except Exception:
        return option_chain_empty_summary()
    if df.empty:
        return option_chain_empty_summary()

    expiration_type = df.get("expiration_type", pd.Series(dtype=object)).astype(str)
    dte = df["expiration_date"].apply(lambda value: dte_for_trade_date(value, trade_date_text))
    dte = pd.to_numeric(dte, errors="coerce")
    return {
        "rows": int(len(df)),
        "monthly": int((expiration_type == "monthly").sum()),
        "short_cycle": int((expiration_type != "monthly").sum()),
        "zero_dte": int((dte <= 0).sum()),
        "one_dte": int((dte == 1).sum()),
        "expirations": int(df.get("expiration_date", pd.Series(dtype=object)).nunique()),
        "provider_iv_rows": int(df.get("provider_iv", pd.Series(dtype=float)).notna().sum()),
        "computed_iv_rows": int(df.get("computed_iv", pd.Series(dtype=float)).notna().sum()),
        "open_interest_rows": int(df.get("open_interest", pd.Series(dtype=float)).notna().sum()),
    }


def load_underlying_iv_rank(
    underlying: str,
    *,
    window: int = 252,
    use_test_tables: bool = True,
    engine=None,
) -> dict[str, Any] | None:
    engine = engine or dashboard_engine()
    if engine is None:
        return None
    return get_us_underlying_iv_rank(
        normalize_underlying(underlying),
        window=window,
        use_test_tables=use_test_tables,
        engine=engine,
    )


def load_iv_history(
    underlying: str,
    *,
    window: int = 252,
    use_test_tables: bool = True,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"])

    names = option_table_names(use_test_tables)
    contracts = safe_table_name(names["contracts"])
    iv = safe_table_name(names["iv"])
    if not table_exists(engine, contracts) or not table_exists(engine, iv):
        return _empty_df(["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"])

    # Keep the day window aligned with get_us_underlying_iv_rank. Do not limit
    # raw option rows before filtering, because one trading day can contain
    # thousands of monthly contracts and an early row cap hides older dates.
    day_limit = max(min(int(window or 252), 1500), 1)
    row_limit = max(day_limit * 5000, 1000)
    if getattr(getattr(engine, "dialect", None), "name", "") in {"mysql", "mariadb"}:
        sql = text(
            f"""
            WITH filtered AS (
                SELECT h.trade_date, h.provider_iv, h.computed_iv, h.open_interest,
                       CASE
                           WHEN COALESCE(h.provider_iv, h.computed_iv) > 3
                               THEN COALESCE(h.provider_iv, h.computed_iv) / 100
                           ELSE COALESCE(h.provider_iv, h.computed_iv)
                       END AS iv_value
                FROM {iv} h
                JOIN {contracts} c ON h.option_ticker = c.option_ticker
                WHERE h.underlying = :underlying
                  AND c.expiration_type = 'monthly'
                  AND COALESCE(h.provider_iv, h.computed_iv) IS NOT NULL
                  AND h.underlying_price > 0
                  AND DATEDIFF(STR_TO_DATE(c.expiration_date, '%Y-%m-%d'), STR_TO_DATE(h.trade_date, '%Y%m%d')) BETWEEN 20 AND 90
                  AND ABS(c.strike - h.underlying_price) / h.underlying_price <= 0.10
                  AND (h.open_interest IS NULL OR h.open_interest > 0)
            )
            SELECT trade_date,
                   CASE
                       WHEN SUM(COALESCE(open_interest, 0)) > 0
                           THEN SUM(iv_value * COALESCE(open_interest, 0)) / SUM(COALESCE(open_interest, 0))
                       ELSE AVG(iv_value)
                   END AS iv,
                   COUNT(*) AS source_rows,
                   SUM(CASE WHEN provider_iv IS NOT NULL THEN 1 ELSE 0 END) AS provider_rows,
                   SUM(CASE WHEN computed_iv IS NOT NULL THEN 1 ELSE 0 END) AS computed_rows
            FROM filtered
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT :day_limit
            """
        )
        try:
            out = pd.read_sql(
                sql,
                engine,
                params={"underlying": normalize_underlying(underlying), "day_limit": day_limit},
            )
            if not out.empty:
                for col in ("iv", "source_rows", "provider_rows", "computed_rows"):
                    out[col] = pd.to_numeric(out[col], errors="coerce")
                out = out.dropna(subset=["iv"]).sort_values("trade_date").tail(window).reset_index(drop=True)
                out["iv_pct"] = out["iv"] * 100
                return out[["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"]]
        except Exception:
            pass

    sql = text(
        f"""
        SELECT h.trade_date, h.provider_iv, h.computed_iv, h.iv_source,
               h.open_interest, h.underlying_price, c.strike, c.expiration_date,
               c.call_put, c.expiration_type
        FROM {iv} h
        JOIN {contracts} c ON h.option_ticker = c.option_ticker
        WHERE h.underlying = :underlying
          AND c.expiration_type = 'monthly'
        ORDER BY h.trade_date DESC
        LIMIT {row_limit}
        """
    )
    try:
        df = pd.read_sql(sql, engine, params={"underlying": normalize_underlying(underlying)})
    except Exception:
        return _empty_df(["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"])
    if df.empty:
        return _empty_df(["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"])

    df["iv"] = df.apply(
        lambda row: normalize_iv_value(row.get("provider_iv")) or normalize_iv_value(row.get("computed_iv")),
        axis=1,
    )
    df = df.dropna(subset=["iv"])
    if df.empty:
        return _empty_df(["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"])

    for col in ("open_interest", "underlying_price", "strike"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["dte"] = df.apply(lambda row: dte_for_trade_date(row["expiration_date"], row["trade_date"]), axis=1)
    df = df[(df["dte"] >= 20) & (df["dte"] <= 90)]
    df = df[df["underlying_price"] > 0]
    df = df[(df["strike"] - df["underlying_price"]).abs() / df["underlying_price"] <= 0.10]
    if "open_interest" in df.columns:
        df = df[df["open_interest"].isna() | (df["open_interest"] > 0)]
    if df.empty:
        return _empty_df(["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"])

    def aggregate(day: pd.DataFrame) -> pd.Series:
        weights = day["open_interest"].fillna(0).astype(float)
        if weights.sum() > 0:
            iv_value = float((day["iv"].astype(float) * weights).sum() / weights.sum())
        else:
            iv_value = float(day["iv"].astype(float).mean())
        return pd.Series(
            {
                "iv": iv_value,
                "source_rows": int(len(day)),
                "provider_rows": int(day["provider_iv"].notna().sum()),
                "computed_rows": int(day["computed_iv"].notna().sum()),
            }
        )

    out = pd.DataFrame(
        [
            {"trade_date": trade_date, **aggregate(day).to_dict()}
            for trade_date, day in df.groupby("trade_date")
        ]
    )
    out = out.sort_values("trade_date").tail(window).reset_index(drop=True)
    out["iv_pct"] = out["iv"] * 100
    return out[["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"]]


def load_market_metrics_history(
    underlying: str,
    *,
    window: int = 252,
    use_test_tables: bool = True,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(MARKET_METRICS_COLUMNS)

    names = option_table_names(use_test_tables)
    table_name = safe_table_name(names.get("metrics", ""))
    if not table_name or not table_exists(engine, table_name):
        return _empty_df(MARKET_METRICS_COLUMNS)
    columns = table_columns(engine, table_name)
    if not {"trade_date", "underlying"}.issubset(columns):
        return _empty_df(MARKET_METRICS_COLUMNS)

    limit = min(max(int(window or 252) * 4, 300), 5000)
    selected = [_select_expr(columns, col) for col in MARKET_METRICS_COLUMNS]
    sql = text(
        f"""
        SELECT {", ".join(selected)}
        FROM {table_name}
        WHERE underlying = :underlying
        ORDER BY trade_date DESC
        LIMIT {limit}
        """
    )
    try:
        df = pd.read_sql(sql, engine, params={"underlying": normalize_underlying(underlying)})
    except Exception:
        return _empty_df(MARKET_METRICS_COLUMNS)
    if df.empty:
        return _empty_df(MARKET_METRICS_COLUMNS)

    df["trade_date"] = df["trade_date"].apply(normalize_trade_date)
    for col in MARKET_METRIC_NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[MARKET_METRICS_COLUMNS].sort_values("trade_date").reset_index(drop=True)


def option_anomaly_scan_cache_table(use_test_tables: bool = False) -> str:
    suffix = "_test" if use_test_tables else ""
    return f"{OPTION_ANOMALY_SCAN_CACHE_TABLE}{suffix}"


def ensure_option_anomaly_scan_cache_table(engine, use_test_tables: bool = False) -> None:
    if engine is None:
        return
    table_name = safe_table_name(option_anomaly_scan_cache_table(use_test_tables))
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    trade_date VARCHAR(8) NOT NULL,
                    underlying VARCHAR(32) NOT NULL,
                    option_ticker VARCHAR(64) NOT NULL,
                    signal_family VARCHAR(32) NOT NULL,
                    call_put VARCHAR(1),
                    strike DOUBLE,
                    expiration_date VARCHAR(10),
                    dte INT,
                    moneyness_pct DOUBLE,
                    underlying_price DOUBLE,
                    close DOUBLE,
                    vwap DOUBLE,
                    volume DOUBLE,
                    open_interest DOUBLE,
                    oi_prev DOUBLE,
                    oi_change DOUBLE,
                    oi_change_pct DOUBLE,
                    volume_oi_ratio DOUBLE,
                    premium_est DOUBLE,
                    iv_pct DOUBLE,
                    iv_change_1d DOUBLE,
                    history_days INT,
                    historical_avg_oi DOUBLE,
                    historical_max_oi DOUBLE,
                    historical_avg_oi_change DOUBLE,
                    historical_max_oi_change DOUBLE,
                    historical_positive_oi_change_days INT,
                    oi_change_multiple DOUBLE,
                    anomaly_score DOUBLE,
                    tags_json TEXT,
                    data_gap TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (trade_date, option_ticker, signal_family)
                )
                """
            )
        )
    cache_key = (id(engine), table_name)
    _TABLE_COLUMNS_CACHE.pop(cache_key, None)
    existing_columns = table_columns(engine, table_name)
    column_types = {
        "trade_date": "VARCHAR(8)",
        "underlying": "VARCHAR(32)",
        "option_ticker": "VARCHAR(64)",
        "signal_family": "VARCHAR(32)",
        "call_put": "VARCHAR(1)",
        "expiration_date": "VARCHAR(10)",
        "dte": "INT",
        "history_days": "INT",
        "historical_positive_oi_change_days": "INT",
        "tags_json": "TEXT",
        "data_gap": "TEXT",
    }
    missing_columns = [col for col in OPTION_ANOMALY_SCAN_COLUMNS if col not in existing_columns]
    if missing_columns:
        with engine.begin() as conn:
            for col in missing_columns:
                conn.execute(
                    text(
                        f"ALTER TABLE {table_name} ADD COLUMN {safe_table_name(col)} "
                        f"{column_types.get(col, 'DOUBLE')}"
                    )
                )
        _TABLE_COLUMNS_CACHE.pop(cache_key, None)
    _TABLE_EXISTS_CACHE[(id(engine), table_name)] = True
    _TABLE_COLUMNS_CACHE[(id(engine), table_name)] = set(OPTION_ANOMALY_SCAN_COLUMNS) | {"updated_at"}


def _latest_option_trade_date_for_scan(engine, use_test_tables: bool) -> str | None:
    names = option_table_names(use_test_tables)
    daily = safe_table_name(names["daily"])
    if not table_exists(engine, daily):
        return None
    value = _scalar(engine, text(f"SELECT MAX(trade_date) FROM {daily}"))
    date_text = normalize_trade_date(value)
    return date_text if date_text else None


def _scan_underlyings(underlyings: list[str] | tuple[str, ...] | None) -> list[str]:
    if underlyings:
        values = [normalize_underlying(item) for item in underlyings if normalize_underlying(item)]
    else:
        values = list(DEFAULT_DASHBOARD_UNDERLYINGS)
    return sorted(dict.fromkeys(values))


def _iv_pct_from_values(provider_iv: Any, computed_iv: Any) -> float | None:
    iv = normalize_iv_value(provider_iv) or normalize_iv_value(computed_iv)
    return float(iv) * 100.0 if iv is not None else None


def _option_is_otm(call_put: Any, moneyness_pct: Any) -> bool:
    mny = _clean_number(moneyness_pct)
    side = str(call_put or "").upper()
    if mny is None:
        return False
    if side == "C":
        return mny > 0
    if side == "P":
        return mny < 0
    return False


def _safe_ratio(numerator: Any, denominator: Any) -> float | None:
    num = _clean_number(numerator)
    den = _clean_number(denominator)
    if num is None or den is None or den <= 0:
        return None
    return float(num) / float(den)


def _is_oi_change_anomaly(
    row: pd.Series,
    *,
    min_oi_change: float,
    min_history_days: int,
    min_change_multiple: float = 3.0,
    max_breakout_multiple: float = 1.5,
) -> bool:
    oi_change = _clean_number(row.get("oi_change")) or 0.0
    history_days = int(_clean_number(row.get("history_days")) or 0)
    oi_prev = _clean_number(row.get("oi_prev"))
    if oi_change < float(min_oi_change or 0):
        return False
    if history_days < int(min_history_days or 1):
        return oi_prev is not None and oi_prev <= 0
    change_multiple = _clean_number(row.get("oi_change_multiple"))
    historical_max_change = _clean_number(row.get("historical_max_oi_change"))
    if change_multiple is not None and change_multiple >= min_change_multiple:
        return True
    if historical_max_change is None:
        return False
    if historical_max_change <= 0:
        return True
    return oi_change >= max(float(min_oi_change or 0), historical_max_change * max_breakout_multiple)


def _option_anomaly_tags(
    row: pd.Series,
    *,
    min_oi_change: float,
    min_premium: float,
    min_history_days: int,
) -> tuple[list[str], list[str]]:
    tags: list[str] = []
    gaps: list[str] = []
    volume = _clean_number(row.get("volume")) or 0.0
    oi_now = _clean_number(row.get("open_interest"))
    oi_prev = _clean_number(row.get("oi_prev"))
    oi_change = _clean_number(row.get("oi_change")) or 0.0
    oi_change_pct = _clean_number(row.get("oi_change_pct"))
    premium = _clean_number(row.get("premium_est")) or 0.0
    volume_oi_ratio = _clean_number(row.get("volume_oi_ratio"))
    dte = _clean_number(row.get("dte"))
    history_days = int(_clean_number(row.get("history_days")) or 0)
    historical_max = _clean_number(row.get("historical_max_oi"))
    historical_max_change = _clean_number(row.get("historical_max_oi_change"))
    oi_change_multiple = _clean_number(row.get("oi_change_multiple"))
    is_oi_anomaly = _is_oi_change_anomaly(
        row,
        min_oi_change=min_oi_change,
        min_history_days=min_history_days,
    )

    if oi_now is None:
        gaps.append("missing_oi")
    if oi_prev is None:
        gaps.append("missing_prev_oi")
    if _clean_number(row.get("iv_pct")) is None:
        gaps.append("missing_iv")
    if history_days < min_history_days:
        tags.append("历史样本不足")
        gaps.append("insufficient_oi_history")

    if oi_change > 0 and (oi_change >= min_oi_change or (oi_change_pct is not None and oi_change_pct >= 0.5)):
        tags.append("OI大幅净增")
    if is_oi_anomaly:
        tags.append("OI增量异常")
        if history_days < min_history_days and oi_prev is not None and oi_prev <= 0:
            tags.append("新仓突增")
        if oi_change_multiple is not None and oi_change_multiple >= 3.0:
            tags.append("高于历史均值")
        if historical_max_change is None or historical_max_change <= 0 or oi_change >= historical_max_change * 1.5:
            tags.append("突破历史增量")
    if volume > 0 and oi_now is not None and volume > max(float(oi_now), 0.0):
        tags.append("Volume>OI")
    elif volume_oi_ratio is not None and volume_oi_ratio >= 1.0:
        tags.append("Volume>OI")
    if premium >= min_premium:
        tags.append("大额权利金")
    if oi_change > 0 and _option_is_otm(row.get("call_put"), row.get("moneyness_pct")):
        tags.append("OTM埋伏")
    if oi_change > 0 and dte is not None and dte <= 45:
        tags.append("近月增仓")
    if history_days >= min_history_days and historical_max is not None and oi_now is not None and oi_now > historical_max:
        tags.append("历史新高OI")

    return list(dict.fromkeys(tags)), list(dict.fromkeys(gaps))


def _option_anomaly_score(row: pd.Series, tags: list[str], *, min_premium: float) -> float:
    oi_change = max(_clean_number(row.get("oi_change")) or 0.0, 0.0)
    oi_change_pct = max(_clean_number(row.get("oi_change_pct")) or 0.0, 0.0)
    volume_oi_ratio = max(_clean_number(row.get("volume_oi_ratio")) or 0.0, 0.0)
    premium = max(_clean_number(row.get("premium_est")) or 0.0, 0.0)
    oi_change_multiple = max(_clean_number(row.get("oi_change_multiple")) or 0.0, 0.0)
    score = 0.0
    if oi_change > 0:
        score += min(28.0, math.log1p(oi_change) * 4.2)
        score += min(18.0, oi_change_pct * 12.0)
    if "OI增量异常" in tags:
        score += 18.0
    if oi_change_multiple > 0:
        score += min(14.0, math.log1p(oi_change_multiple) * 4.0)
    score += min(16.0, volume_oi_ratio * 5.0)
    if premium > 0 and min_premium > 0:
        score += min(20.0, premium / min_premium * 6.0)
    if "历史新高OI" in tags:
        score += 16.0
    if "突破历史增量" in tags:
        score += 10.0
    if "OTM埋伏" in tags:
        score += 10.0
    if "近月增仓" in tags:
        score += 8.0
    if "历史样本不足" in tags:
        score -= 4.0
    return round(max(score, 0.0), 3)


def _select_option_anomaly_source_rows(
    *,
    engine,
    trade_date: str,
    underlyings: list[str],
    lookback_days: int,
    use_test_tables: bool,
) -> pd.DataFrame:
    names = option_table_names(use_test_tables)
    contracts = safe_table_name(names["contracts"])
    daily = safe_table_name(names["daily"])
    iv = safe_table_name(names["iv"])
    if not table_exists(engine, contracts) or not table_exists(engine, daily):
        return _empty_df(OPTION_ANOMALY_SCAN_COLUMNS)

    placeholders, params = _named_in_clause("underlying", underlyings)
    params["trade_date"] = trade_date
    trade_dt = pd.to_datetime(trade_date, format="%Y%m%d", errors="coerce")
    if pd.isna(trade_dt):
        return _empty_df(OPTION_ANOMALY_SCAN_COLUMNS)
    calendar_days = max(int(lookback_days or 20) * 3, 10)
    params["hist_start"] = (trade_dt - pd.Timedelta(days=calendar_days)).strftime("%Y%m%d")

    iv_join = ""
    iv_columns = "NULL AS provider_iv, NULL AS computed_iv, NULL AS underlying_price, NULL AS prev_provider_iv, NULL AS prev_computed_iv"
    if table_exists(engine, iv):
        iv_join = f"""
        LEFT JOIN {iv} h ON d.trade_date = h.trade_date AND d.option_ticker = h.option_ticker
        LEFT JOIN {iv} ph ON p.prev_trade_date = ph.trade_date AND p.option_ticker = ph.option_ticker
        """
        iv_columns = """
            h.provider_iv AS provider_iv,
            h.computed_iv AS computed_iv,
            h.underlying_price AS underlying_price,
            ph.provider_iv AS prev_provider_iv,
            ph.computed_iv AS prev_computed_iv
        """

    sql = text(
        f"""
        SELECT d.trade_date,
               d.underlying,
               d.option_ticker,
               c.call_put,
               c.strike,
               c.expiration_date,
               d.close,
               d.vwap,
               d.volume,
               d.open_interest,
               p.prev_trade_date,
               p.oi_prev,
               hist.history_days,
               hist.historical_avg_oi,
               hist.historical_max_oi,
               {iv_columns}
        FROM {daily} d
        JOIN {contracts} c ON d.option_ticker = c.option_ticker
        LEFT JOIN (
            SELECT d_prev.option_ticker,
                   d_prev.trade_date AS prev_trade_date,
                   d_prev.open_interest AS oi_prev
            FROM {daily} d_prev
            JOIN (
                SELECT option_ticker, MAX(trade_date) AS prev_trade_date
                FROM {daily}
                WHERE trade_date < :trade_date
                  AND underlying IN ({placeholders})
                  AND open_interest IS NOT NULL
                GROUP BY option_ticker
            ) prev_key
              ON d_prev.option_ticker = prev_key.option_ticker
             AND d_prev.trade_date = prev_key.prev_trade_date
        ) p ON d.option_ticker = p.option_ticker
        LEFT JOIN (
            SELECT option_ticker,
                   COUNT(*) AS history_days,
                   AVG(open_interest) AS historical_avg_oi,
                   MAX(open_interest) AS historical_max_oi
            FROM {daily}
            WHERE trade_date < :trade_date
              AND trade_date >= :hist_start
              AND underlying IN ({placeholders})
              AND open_interest IS NOT NULL
            GROUP BY option_ticker
        ) hist ON d.option_ticker = hist.option_ticker
        {iv_join}
        WHERE d.trade_date = :trade_date
          AND d.underlying IN ({placeholders})
        """
    )
    try:
        return pd.read_sql(sql, engine, params=params)
    except Exception:
        return pd.DataFrame()


def _attach_option_oi_change_history(
    df: pd.DataFrame,
    *,
    engine,
    trade_date: str,
    underlyings: list[str],
    lookback_days: int,
    use_test_tables: bool,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    names = option_table_names(use_test_tables)
    daily = safe_table_name(names["daily"])
    if not table_exists(engine, daily):
        return df
    trade_dt = pd.to_datetime(trade_date, format="%Y%m%d", errors="coerce")
    if pd.isna(trade_dt):
        return df
    placeholders, params = _named_in_clause("underlying", underlyings)
    params["trade_date"] = trade_date
    calendar_days = max(int(lookback_days or 20) * 3, 10)
    params["hist_start"] = (trade_dt - pd.Timedelta(days=calendar_days)).strftime("%Y%m%d")
    sql = text(
        f"""
        SELECT trade_date, option_ticker, open_interest
        FROM {daily}
        WHERE trade_date < :trade_date
          AND trade_date >= :hist_start
          AND underlying IN ({placeholders})
          AND open_interest IS NOT NULL
        """
    )
    try:
        hist = pd.read_sql(sql, engine, params=params)
    except Exception:
        return df
    out = df.copy()
    stat_cols = [
        "historical_avg_oi_change",
        "historical_max_oi_change",
        "historical_positive_oi_change_days",
    ]
    if hist.empty:
        for col in stat_cols:
            out[col] = None
        return out
    hist["open_interest"] = pd.to_numeric(hist["open_interest"], errors="coerce")
    hist = hist.dropna(subset=["open_interest"]).sort_values(["option_ticker", "trade_date"])
    hist["oi_delta"] = hist.groupby("option_ticker")["open_interest"].diff()
    hist["positive_oi_delta"] = hist["oi_delta"].where(hist["oi_delta"] > 0)
    grouped = hist.groupby("option_ticker", as_index=False)
    stats = grouped.agg(
        historical_avg_oi_change=("positive_oi_delta", "mean"),
        historical_max_oi_change=("oi_delta", "max"),
        historical_positive_oi_change_days=("oi_delta", lambda series: int((series > 0).sum())),
    )
    out = out.drop(columns=stat_cols, errors="ignore")
    return out.merge(stats, on="option_ticker", how="left")


def compute_option_anomaly_scan(
    *,
    trade_date: str | dt.date | dt.datetime | None = None,
    underlyings: list[str] | tuple[str, ...] | None = None,
    lookback_days: int = 20,
    max_dte: int = 90,
    min_volume: float = 100,
    min_premium: float = 250_000,
    min_oi_change: float = 100,
    min_history_days: int = 5,
    use_test_tables: bool = False,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(OPTION_ANOMALY_SCAN_COLUMNS)
    trade_date_text = normalize_trade_date(trade_date) if trade_date else _latest_option_trade_date_for_scan(engine, use_test_tables)
    if not trade_date_text:
        return _empty_df(OPTION_ANOMALY_SCAN_COLUMNS)
    target_underlyings = _scan_underlyings(underlyings)
    if not target_underlyings:
        return _empty_df(OPTION_ANOMALY_SCAN_COLUMNS)

    source = _select_option_anomaly_source_rows(
        engine=engine,
        trade_date=trade_date_text,
        underlyings=target_underlyings,
        lookback_days=lookback_days,
        use_test_tables=use_test_tables,
    )
    if source is None or source.empty:
        return _empty_df(OPTION_ANOMALY_SCAN_COLUMNS)

    df = _attach_option_oi_change_history(
        source.copy(),
        engine=engine,
        trade_date=trade_date_text,
        underlyings=target_underlyings,
        lookback_days=lookback_days,
        use_test_tables=use_test_tables,
    )
    for col in (
        "strike",
        "close",
        "vwap",
        "volume",
        "open_interest",
        "oi_prev",
        "history_days",
        "historical_avg_oi",
        "historical_max_oi",
        "historical_avg_oi_change",
        "historical_max_oi_change",
        "historical_positive_oi_change_days",
        "underlying_price",
    ):
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["trade_date"] = df["trade_date"].apply(normalize_trade_date)
    df["dte"] = df.apply(lambda row: dte_for_trade_date(row.get("expiration_date"), row.get("trade_date")), axis=1)
    df["dte"] = pd.to_numeric(df["dte"], errors="coerce")
    df = df[df["dte"].notna() & (df["dte"] >= 0) & (df["dte"] <= max(int(max_dte or 90), 0))].copy()
    if df.empty:
        return _empty_df(OPTION_ANOMALY_SCAN_COLUMNS)

    price = pd.to_numeric(df["underlying_price"], errors="coerce")
    strike = pd.to_numeric(df["strike"], errors="coerce")
    df["moneyness_pct"] = ((strike - price) / price * 100.0).where(price > 0)
    df["iv_pct"] = df.apply(lambda row: _iv_pct_from_values(row.get("provider_iv"), row.get("computed_iv")), axis=1)
    df["prev_iv_pct"] = df.apply(
        lambda row: _iv_pct_from_values(row.get("prev_provider_iv"), row.get("prev_computed_iv")),
        axis=1,
    )
    df["iv_change_1d"] = pd.to_numeric(df["iv_pct"], errors="coerce") - pd.to_numeric(
        df["prev_iv_pct"], errors="coerce"
    )
    df["oi_change"] = pd.to_numeric(df["open_interest"], errors="coerce") - pd.to_numeric(
        df["oi_prev"], errors="coerce"
    )
    df["oi_change_pct"] = df.apply(lambda row: _safe_ratio(row.get("oi_change"), max(row.get("oi_prev") or 0, 1)), axis=1)
    df["oi_change_multiple"] = df.apply(
        lambda row: _safe_ratio(row.get("oi_change"), row.get("historical_avg_oi_change")),
        axis=1,
    )
    df["volume_oi_ratio"] = df.apply(
        lambda row: _safe_ratio(row.get("volume"), max(row.get("open_interest") or 0, 1)),
        axis=1,
    )
    price_for_premium = pd.to_numeric(df["vwap"], errors="coerce").fillna(pd.to_numeric(df["close"], errors="coerce"))
    df["premium_est"] = price_for_premium * pd.to_numeric(df["volume"], errors="coerce") * 100.0

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        volume = _clean_number(row.get("volume")) or 0.0
        premium = _clean_number(row.get("premium_est")) or 0.0
        oi_change = _clean_number(row.get("oi_change")) or 0.0
        oi_change_pct = _clean_number(row.get("oi_change_pct"))
        volume_oi_ratio = _clean_number(row.get("volume_oi_ratio"))
        if volume < float(min_volume or 0) and premium < float(min_premium or 0) and oi_change <= 0:
            continue
        tags, gaps = _option_anomaly_tags(
            row,
            min_oi_change=float(min_oi_change or 0),
            min_premium=float(min_premium or 0),
            min_history_days=int(min_history_days or 1),
        )
        families: list[str] = []
        if _is_oi_change_anomaly(
            row,
            min_oi_change=float(min_oi_change or 0),
            min_history_days=int(min_history_days or 1),
        ):
            families.append("oi_build")
        if volume_oi_ratio is not None and volume_oi_ratio >= 1.0:
            families.append("volume_oi")
        if premium >= float(min_premium or 0):
            families.append("premium")
        if not families:
            continue
        score = _option_anomaly_score(row, tags, min_premium=float(min_premium or 1))
        base = {col: row.get(col) for col in OPTION_ANOMALY_SCAN_COLUMNS if col not in {"signal_family", "tags_json", "data_gap", "anomaly_score"}}
        base["trade_date"] = trade_date_text
        base["underlying"] = normalize_underlying(base.get("underlying"))
        base["dte"] = int(_clean_number(base.get("dte")) or 0)
        base["history_days"] = int(_clean_number(base.get("history_days")) or 0)
        for family in families:
            item = dict(base)
            item["signal_family"] = family
            item["tags_json"] = json.dumps(tags, ensure_ascii=False)
            item["data_gap"] = ",".join(gaps)
            item["anomaly_score"] = score
            rows.append(item)

    if not rows:
        return _empty_df(OPTION_ANOMALY_SCAN_COLUMNS)
    out = pd.DataFrame(rows)
    for col in OPTION_ANOMALY_SCAN_COLUMNS:
        if col not in out.columns:
            out[col] = None
    numeric_cols = [
        "strike",
        "dte",
        "moneyness_pct",
        "underlying_price",
        "close",
        "vwap",
        "volume",
        "open_interest",
        "oi_prev",
        "oi_change",
        "oi_change_pct",
        "volume_oi_ratio",
        "premium_est",
        "iv_pct",
        "iv_change_1d",
        "history_days",
        "historical_avg_oi",
        "historical_max_oi",
        "historical_avg_oi_change",
        "historical_max_oi_change",
        "historical_positive_oi_change_days",
        "oi_change_multiple",
        "anomaly_score",
    ]
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out[OPTION_ANOMALY_SCAN_COLUMNS].sort_values(
        ["anomaly_score", "premium_est", "oi_change"], ascending=[False, False, False]
    ).reset_index(drop=True)


def _normalize_scan_cache_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return _empty_df(OPTION_ANOMALY_SCAN_COLUMNS)
    out = df.copy()
    for col in OPTION_ANOMALY_SCAN_COLUMNS:
        if col not in out.columns:
            out[col] = None
    for col in (
        "strike",
        "dte",
        "moneyness_pct",
        "underlying_price",
        "close",
        "vwap",
        "volume",
        "open_interest",
        "oi_prev",
        "oi_change",
        "oi_change_pct",
        "volume_oi_ratio",
        "premium_est",
        "iv_pct",
        "iv_change_1d",
        "history_days",
        "historical_avg_oi",
        "historical_max_oi",
        "historical_avg_oi_change",
        "historical_max_oi_change",
        "historical_positive_oi_change_days",
        "oi_change_multiple",
        "anomaly_score",
    ):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out[OPTION_ANOMALY_SCAN_COLUMNS].sort_values(
        ["anomaly_score", "premium_est", "oi_change"], ascending=[False, False, False]
    ).reset_index(drop=True)


def load_option_anomaly_scan_cache(
    trade_date: str | dt.date | dt.datetime,
    *,
    underlyings: list[str] | tuple[str, ...] | None = None,
    use_test_tables: bool = False,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(OPTION_ANOMALY_SCAN_COLUMNS)
    table_name = safe_table_name(option_anomaly_scan_cache_table(use_test_tables))
    if not table_exists(engine, table_name):
        return _empty_df(OPTION_ANOMALY_SCAN_COLUMNS)
    target_underlyings = _scan_underlyings(underlyings)
    placeholders, params = _named_in_clause("underlying", target_underlyings)
    params["trade_date"] = normalize_trade_date(trade_date)
    sql = text(
        f"""
        SELECT {", ".join(safe_table_name(col) for col in OPTION_ANOMALY_SCAN_COLUMNS)}
        FROM {table_name}
        WHERE trade_date = :trade_date
          AND underlying IN ({placeholders})
        ORDER BY anomaly_score DESC, premium_est DESC, oi_change DESC
        """
    )
    try:
        df = pd.read_sql(sql, engine, params=params)
    except Exception:
        return _empty_df(OPTION_ANOMALY_SCAN_COLUMNS)
    return _normalize_scan_cache_frame(df)


def _clean_scan_record(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def replace_option_anomaly_scan_cache(
    scan_df: pd.DataFrame,
    *,
    trade_date: str | dt.date | dt.datetime,
    underlyings: list[str] | tuple[str, ...],
    use_test_tables: bool = False,
    engine=None,
) -> int:
    engine = engine or dashboard_engine()
    if engine is None:
        return 0
    ensure_option_anomaly_scan_cache_table(engine, use_test_tables)
    table_name = safe_table_name(option_anomaly_scan_cache_table(use_test_tables))
    target_underlyings = _scan_underlyings(list(underlyings))
    if not target_underlyings:
        return 0
    placeholders, params = _named_in_clause("underlying", target_underlyings)
    params["trade_date"] = normalize_trade_date(trade_date)
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                DELETE FROM {table_name}
                WHERE trade_date = :trade_date
                  AND underlying IN ({placeholders})
                """
            ),
            params,
        )
        if scan_df is None or scan_df.empty:
            return 0
        rows = []
        for row in scan_df[OPTION_ANOMALY_SCAN_COLUMNS].to_dict(orient="records"):
            rows.append({col: _clean_scan_record(row.get(col)) for col in OPTION_ANOMALY_SCAN_COLUMNS})
        column_sql = ", ".join(safe_table_name(col) for col in OPTION_ANOMALY_SCAN_COLUMNS)
        value_sql = ", ".join(f":{col}" for col in OPTION_ANOMALY_SCAN_COLUMNS)
        conn.execute(
            text(
                f"""
                INSERT INTO {table_name} ({column_sql})
                VALUES ({value_sql})
                """
            ),
            rows,
        )
    return int(len(scan_df))


def rebuild_option_anomaly_scan_cache(
    *,
    trade_date: str | dt.date | dt.datetime | None = None,
    underlyings: list[str] | tuple[str, ...] | None = None,
    lookback_days: int = 20,
    max_dte: int = 90,
    min_volume: float = 100,
    min_premium: float = 250_000,
    min_oi_change: float = 100,
    min_history_days: int = 5,
    use_test_tables: bool = False,
    engine=None,
) -> dict[str, Any]:
    engine = engine or dashboard_engine()
    if engine is None:
        return {"status": "missing_engine", "trade_date": None, "rows": 0}
    trade_date_text = normalize_trade_date(trade_date) if trade_date else _latest_option_trade_date_for_scan(engine, use_test_tables)
    if not trade_date_text:
        return {"status": "missing_trade_date", "trade_date": None, "rows": 0}
    target_underlyings = _scan_underlyings(underlyings)
    scan = compute_option_anomaly_scan(
        trade_date=trade_date_text,
        underlyings=target_underlyings,
        lookback_days=lookback_days,
        max_dte=max_dte,
        min_volume=min_volume,
        min_premium=min_premium,
        min_oi_change=min_oi_change,
        min_history_days=min_history_days,
        use_test_tables=use_test_tables,
        engine=engine,
    )
    rows = replace_option_anomaly_scan_cache(
        scan,
        trade_date=trade_date_text,
        underlyings=target_underlyings,
        use_test_tables=use_test_tables,
        engine=engine,
    )
    return {
        "status": "updated",
        "trade_date": trade_date_text,
        "underlyings": target_underlyings,
        "rows": rows,
        "contracts": int(scan["option_ticker"].nunique()) if not scan.empty else 0,
    }


def load_option_anomaly_scan(
    *,
    trade_date: str | dt.date | dt.datetime | None = None,
    underlyings: list[str] | tuple[str, ...] | None = None,
    lookback_days: int = 20,
    max_dte: int = 90,
    min_volume: float = 100,
    min_premium: float = 250_000,
    min_oi_change: float = 100,
    min_history_days: int = 5,
    prefer_cache: bool = True,
    use_test_tables: bool = False,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(OPTION_ANOMALY_SCAN_COLUMNS)
    trade_date_text = normalize_trade_date(trade_date) if trade_date else _latest_option_trade_date_for_scan(engine, use_test_tables)
    if not trade_date_text:
        return _empty_df(OPTION_ANOMALY_SCAN_COLUMNS)
    target_underlyings = _scan_underlyings(underlyings)
    if prefer_cache:
        cached = load_option_anomaly_scan_cache(
            trade_date_text,
            underlyings=target_underlyings,
            use_test_tables=use_test_tables,
            engine=engine,
        )
        if not cached.empty:
            return cached
    return compute_option_anomaly_scan(
        trade_date=trade_date_text,
        underlyings=target_underlyings,
        lookback_days=lookback_days,
        max_dte=max_dte,
        min_volume=min_volume,
        min_premium=min_premium,
        min_oi_change=min_oi_change,
        min_history_days=min_history_days,
        use_test_tables=use_test_tables,
        engine=engine,
    )


def _weighted_average(values: pd.Series, weights: pd.Series | None = None) -> float | None:
    clean_values = pd.to_numeric(values, errors="coerce")
    if weights is not None:
        clean_weights = pd.to_numeric(weights, errors="coerce").fillna(0)
        valid = clean_values.notna() & (clean_weights > 0)
        if valid.any() and float(clean_weights[valid].sum()) > 0:
            return float((clean_values[valid] * clean_weights[valid]).sum() / clean_weights[valid].sum())
    values_only = clean_values.dropna()
    if values_only.empty:
        return None
    return float(values_only.mean())


def _filter_stock_to_trade_date(stock_df: pd.DataFrame, trade_date: str | None) -> pd.DataFrame:
    if stock_df is None or stock_df.empty or not trade_date or "date" not in stock_df.columns:
        return stock_df if stock_df is not None else pd.DataFrame()
    cutoff = pd.to_datetime(normalize_trade_date(trade_date), format="%Y%m%d", errors="coerce")
    if pd.isna(cutoff):
        return stock_df
    out = stock_df.copy()
    out["_date_for_filter"] = pd.to_datetime(out["date"], errors="coerce")
    out = out[out["_date_for_filter"] <= cutoff].drop(columns=["_date_for_filter"])
    return out


def calculate_realized_volatility(
    stock_df: pd.DataFrame,
    *,
    window: int = 20,
    trade_date: str | dt.date | dt.datetime | None = None,
) -> float | None:
    if stock_df is None or stock_df.empty or "close" not in stock_df.columns:
        return None
    scoped = _filter_stock_to_trade_date(stock_df, normalize_trade_date(trade_date) if trade_date else None)
    close = pd.to_numeric(scoped.get("close"), errors="coerce").dropna()
    if len(close) < 3:
        return None
    window = max(int(window or 1), 1)
    returns = close.pct_change().dropna().tail(window)
    if len(returns) < 2:
        return None
    return float(returns.std() * math.sqrt(252) * 100)


def _iv_history_until(iv_history: pd.DataFrame, trade_date: str | None) -> pd.DataFrame:
    if iv_history is None or iv_history.empty:
        return pd.DataFrame(columns=["trade_date", "iv_pct"])
    out = iv_history.copy()
    out["trade_date"] = out["trade_date"].apply(lambda value: normalize_trade_date(value) if value is not None else "")
    out["iv_pct"] = pd.to_numeric(out.get("iv_pct"), errors="coerce")
    out = out.dropna(subset=["iv_pct"]).sort_values("trade_date")
    if trade_date:
        out = out[out["trade_date"] <= normalize_trade_date(trade_date)]
    return out.reset_index(drop=True)


def _percentile_rank(values: pd.Series, current_value: float | None) -> float | None:
    if current_value is None:
        return None
    series = pd.to_numeric(values, errors="coerce").dropna()
    if series.empty:
        return None
    return float((series <= float(current_value)).sum() / len(series) * 100)


def _iv_rank_from_history(
    iv_history: pd.DataFrame,
    *,
    current_iv_pct: float | None,
    trade_date: str | None,
    fallback: dict[str, Any] | None = None,
) -> dict[str, float | int | None]:
    history = _iv_history_until(iv_history, trade_date)
    history["iv_change_1d"] = pd.to_numeric(history.get("iv_pct"), errors="coerce").diff()
    series = pd.to_numeric(history.get("iv_pct"), errors="coerce").dropna()
    current_value = None
    if not history.empty and trade_date:
        exact = history[history["trade_date"] == normalize_trade_date(trade_date)]
        if not exact.empty:
            current_value = float(exact["iv_pct"].iloc[-1])
    if current_value is None and not series.empty:
        current_value = float(series.iloc[-1])
    if current_value is None:
        current = pd.to_numeric(pd.Series([current_iv_pct]), errors="coerce").dropna()
        current_value = float(current.iloc[0]) if not current.empty else None
    if series.empty or current_value is None:
        return {
            "iv_rank": (fallback or {}).get("iv_rank"),
            "iv_percentile": (fallback or {}).get("iv_percentile"),
            "current_monthly_iv_pct": current_value,
            "iv_change_1d": None,
            "iv_change_1d_percentile": None,
            "iv_change_5d": None,
            "iv_change_20d": None,
            "iv_history_days": int((fallback or {}).get("days") or 0),
        }
    min_iv = float(series.min())
    max_iv = float(series.max())
    iv_rank = None if math.isclose(max_iv, min_iv) else (current_value - min_iv) / (max_iv - min_iv) * 100
    iv_percentile = float((series <= current_value).sum() / len(series) * 100)
    fallback_date = str((fallback or {}).get("date") or "")
    if fallback_date and normalize_trade_date(fallback_date) == normalize_trade_date(trade_date):
        iv_rank = (fallback or {}).get("iv_rank", iv_rank)
        iv_percentile = (fallback or {}).get("iv_percentile", iv_percentile)
    current_change_1d = None
    if not history.empty:
        current_change_1d = pd.to_numeric(pd.Series([history["iv_change_1d"].iloc[-1]]), errors="coerce").dropna()
        current_change_1d = float(current_change_1d.iloc[0]) if not current_change_1d.empty else None
    return {
        "iv_rank": iv_rank,
        "iv_percentile": iv_percentile,
        "current_monthly_iv_pct": current_value,
        "iv_change_1d": current_change_1d,
        "iv_change_1d_percentile": _percentile_rank(history["iv_change_1d"], current_change_1d),
        "iv_change_5d": current_value - float(series.iloc[-6]) if len(series) >= 6 else None,
        "iv_change_20d": current_value - float(series.iloc[-21]) if len(series) >= 21 else None,
        "iv_history_days": int(len(series)),
    }


def _prepared_option_chain(chain_df: pd.DataFrame) -> pd.DataFrame:
    if chain_df is None or chain_df.empty:
        return pd.DataFrame()
    df = chain_df.copy()
    for col in ("strike", "open_interest", "volume", "iv_pct", "moneyness_pct", "dte", "underlying_price"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = None
    df["call_put"] = df.get("call_put", pd.Series(dtype=object)).astype(str).str.upper()
    df["expiration_type"] = df.get("expiration_type", pd.Series(dtype=object)).astype(str)
    df["expiration_date"] = df.get("expiration_date", pd.Series(dtype=object)).astype(str)
    df["open_interest"] = df["open_interest"].fillna(0)
    df["volume"] = df["volume"].fillna(0)
    return df


def _monthly_atm_frame(chain_df: pd.DataFrame, *, moneyness_band: float = 2.0) -> pd.DataFrame:
    df = _prepared_option_chain(chain_df)
    if df.empty:
        return df
    monthly = df[
        (df["expiration_type"] == "monthly")
        & df["iv_pct"].notna()
        & df["dte"].notna()
        & (df["dte"] > 0)
        & df["moneyness_pct"].notna()
    ].copy()
    if monthly.empty:
        return monthly
    return monthly[monthly["moneyness_pct"].abs() <= float(moneyness_band)].copy()


def calculate_atm_iv_pct(
    chain_df: pd.DataFrame,
    *,
    underlying_price: float | None = None,
    dte_min: int = 20,
    dte_max: int = 90,
    moneyness_band: float = 10.0,
) -> float | None:
    monthly_atm = _monthly_atm_frame(chain_df, moneyness_band=moneyness_band)
    if monthly_atm.empty:
        return None
    monthly_atm = monthly_atm[
        monthly_atm["dte"].between(float(dte_min), float(dte_max))
        & monthly_atm["iv_pct"].notna()
    ].copy()
    if monthly_atm.empty:
        return None
    if underlying_price is not None and "strike" in monthly_atm.columns:
        monthly_atm["_distance"] = (pd.to_numeric(monthly_atm["strike"], errors="coerce") - float(underlying_price)).abs()
        nearest = monthly_atm.sort_values(["_distance", "dte"]).head(24)
        if not nearest.empty:
            monthly_atm = nearest
    return _weighted_average(monthly_atm["iv_pct"], monthly_atm["open_interest"])


def _term_iv_metrics(chain_df: pd.DataFrame) -> dict[str, Any]:
    monthly_atm = _monthly_atm_frame(chain_df, moneyness_band=2.0)
    if monthly_atm.empty:
        return {
            "iv_30d": None,
            "iv_60d": None,
            "term_slope_30_60": None,
            "term_slope_percentile": None,
            "term_state": "样本不足",
        }

    rows = []
    for expiration, group in monthly_atm.groupby("expiration_date", dropna=False):
        rows.append(
            {
                "expiration_date": str(expiration),
                "dte": float(group["dte"].median()),
                "iv_pct": _weighted_average(group["iv_pct"], group["open_interest"]),
            }
        )
    term = pd.DataFrame(rows).dropna(subset=["iv_pct", "dte"])
    if term.empty:
        return {
            "iv_30d": None,
            "iv_60d": None,
            "term_slope_30_60": None,
            "term_slope_percentile": None,
            "term_state": "样本不足",
        }

    def nearest(target: float) -> float | None:
        near = term.assign(distance=(term["dte"] - target).abs()).sort_values("distance").head(1)
        return float(near["iv_pct"].iloc[0]) if not near.empty else None

    iv_30d = nearest(30)
    iv_60d = nearest(60)
    slope = iv_60d - iv_30d if iv_30d is not None and iv_60d is not None else None
    if slope is None:
        state = "样本不足"
    elif slope <= -1.0:
        state = "Backwardation"
    elif slope >= 1.0:
        state = "Contango"
    else:
        state = "Flat"

    slope_samples = []
    ordered = term.sort_values("dte").reset_index(drop=True)
    for left_idx in range(len(ordered)):
        for right_idx in range(left_idx + 1, len(ordered)):
            left = ordered.iloc[left_idx]
            right = ordered.iloc[right_idx]
            if float(right["dte"]) <= float(left["dte"]):
                continue
            slope_samples.append(float(right["iv_pct"]) - float(left["iv_pct"]))

    return {
        "iv_30d": iv_30d,
        "iv_60d": iv_60d,
        "term_slope_30_60": slope,
        "term_slope_percentile": _percentile_rank(pd.Series(slope_samples, dtype=float), slope),
        "term_state": state,
    }


def _fixed_moneyness_iv(group: pd.DataFrame, *, call_put: str | None, center: float, band: float = 1.0) -> float | None:
    side = group
    if call_put:
        side = side[side["call_put"] == call_put]
    side = side[side["moneyness_pct"].between(center - band, center + band)]
    if side.empty:
        return None
    return _weighted_average(side["iv_pct"], side["open_interest"])


def _skew_metrics(chain_df: pd.DataFrame) -> dict[str, Any]:
    df = _prepared_option_chain(chain_df)
    if df.empty:
        return {
            "skew_expiration": None,
            "put_skew_5pct": None,
            "call_skew_5pct": None,
            "put_call_skew_5pct": None,
            "put_skew_5pct_percentile": None,
            "call_skew_5pct_percentile": None,
            "put_call_skew_5pct_percentile": None,
        }
    monthly = df[
        (df["expiration_type"] == "monthly")
        & df["iv_pct"].notna()
        & df["moneyness_pct"].notna()
        & df["dte"].notna()
        & (df["dte"] > 0)
    ].copy()
    if monthly.empty:
        return {
            "skew_expiration": None,
            "put_skew_5pct": None,
            "call_skew_5pct": None,
            "put_call_skew_5pct": None,
            "put_skew_5pct_percentile": None,
            "call_skew_5pct_percentile": None,
            "put_call_skew_5pct_percentile": None,
        }

    skew_rows = []
    for expiration_date, group in monthly.groupby("expiration_date", dropna=False):
        atm_iv = _fixed_moneyness_iv(group, call_put=None, center=0, band=1.0)
        put_iv = _fixed_moneyness_iv(group, call_put="P", center=-5, band=1.0)
        call_iv = _fixed_moneyness_iv(group, call_put="C", center=5, band=1.0)
        skew_rows.append(
            {
                "expiration_date": str(expiration_date),
                "dte": float(pd.to_numeric(group["dte"], errors="coerce").median()),
                "put_skew_5pct": put_iv - atm_iv if put_iv is not None and atm_iv is not None else None,
                "call_skew_5pct": call_iv - atm_iv if call_iv is not None and atm_iv is not None else None,
            }
        )
    skew_table = pd.DataFrame(skew_rows).dropna(subset=["dte"])
    if not skew_table.empty and {"put_skew_5pct", "call_skew_5pct"}.issubset(skew_table.columns):
        skew_table["put_call_skew_5pct"] = pd.to_numeric(
            skew_table["put_skew_5pct"], errors="coerce"
        ) - pd.to_numeric(skew_table["call_skew_5pct"], errors="coerce")

    candidates = monthly[monthly["dte"].between(20, 45)]
    if candidates.empty:
        candidates = monthly
    expiration = (
        candidates.assign(dte_distance=(candidates["dte"] - 30).abs())
        .sort_values(["dte_distance", "expiration_date"])
        ["expiration_date"]
        .iloc[0]
    )
    slice_df = monthly[monthly["expiration_date"] == expiration]
    atm_iv = _fixed_moneyness_iv(slice_df, call_put=None, center=0, band=1.0)
    put_iv = _fixed_moneyness_iv(slice_df, call_put="P", center=-5, band=1.0)
    call_iv = _fixed_moneyness_iv(slice_df, call_put="C", center=5, band=1.0)
    put_skew = put_iv - atm_iv if put_iv is not None and atm_iv is not None else None
    call_skew = call_iv - atm_iv if call_iv is not None and atm_iv is not None else None
    put_call_skew = put_skew - call_skew if put_skew is not None and call_skew is not None else None
    return {
        "skew_expiration": str(expiration),
        "put_skew_5pct": put_skew,
        "call_skew_5pct": call_skew,
        "put_call_skew_5pct": put_call_skew,
        "put_skew_5pct_percentile": _percentile_rank(skew_table["put_skew_5pct"], put_skew)
        if "put_skew_5pct" in skew_table
        else None,
        "call_skew_5pct_percentile": _percentile_rank(skew_table["call_skew_5pct"], call_skew)
        if "call_skew_5pct" in skew_table
        else None,
        "put_call_skew_5pct_percentile": _percentile_rank(skew_table["put_call_skew_5pct"], put_call_skew)
        if "put_call_skew_5pct" in skew_table
        else None,
    }


def _positioning_metrics(chain_df: pd.DataFrame) -> dict[str, Any]:
    df = _prepared_option_chain(chain_df)
    if df.empty:
        return {
            "put_call_oi": None,
            "put_call_volume": None,
            "zero_dte_volume_share_pct": None,
            "top_oi_strike": None,
            "top_oi": None,
            "top5_oi_share_pct": None,
            "total_open_interest": None,
            "total_volume": None,
            "put_call_oi_percentile": None,
            "put_call_volume_percentile": None,
        }
    call_oi = float(df.loc[df["call_put"] == "C", "open_interest"].sum())
    put_oi = float(df.loc[df["call_put"] == "P", "open_interest"].sum())
    call_volume = float(df.loc[df["call_put"] == "C", "volume"].sum())
    put_volume = float(df.loc[df["call_put"] == "P", "volume"].sum())
    all_oi = float(df["open_interest"].sum())
    total_volume = call_volume + put_volume
    zero_dte_volume = float(df.loc[df["dte"] <= 0, "volume"].sum())

    oi_candidates = df[df["open_interest"] > 0].copy()
    near_term = oi_candidates[oi_candidates["dte"].between(0, 90)]
    if not near_term.empty:
        oi_candidates = near_term
    near_price = oi_candidates[oi_candidates["moneyness_pct"].abs() <= 25] if "moneyness_pct" in oi_candidates else oi_candidates
    if not near_price.empty:
        oi_candidates = near_price

    if oi_candidates.empty:
        top_oi_strike = None
        top_oi = None
        top5_share = None
    else:
        by_strike = oi_candidates.groupby("strike", dropna=True)["open_interest"].sum().sort_values(ascending=False)
        top_oi_strike = float(by_strike.index[0]) if not by_strike.empty else None
        top_oi = float(by_strike.iloc[0]) if not by_strike.empty else None
        total_oi = float(by_strike.sum())
        top5_share = float(by_strike.head(5).sum() / total_oi * 100) if total_oi > 0 else None

    expiry_rows = []
    for _, group in df.groupby("expiration_date", dropna=False):
        expiry_call_oi = float(group.loc[group["call_put"] == "C", "open_interest"].sum())
        expiry_put_oi = float(group.loc[group["call_put"] == "P", "open_interest"].sum())
        expiry_call_volume = float(group.loc[group["call_put"] == "C", "volume"].sum())
        expiry_put_volume = float(group.loc[group["call_put"] == "P", "volume"].sum())
        expiry_rows.append(
            {
                "put_call_oi": expiry_put_oi / expiry_call_oi if expiry_call_oi > 0 else None,
                "put_call_volume": expiry_put_volume / expiry_call_volume if expiry_call_volume > 0 else None,
            }
        )
    expiry_metrics = pd.DataFrame(expiry_rows)
    put_call_oi = put_oi / call_oi if call_oi > 0 else None
    put_call_volume = put_volume / call_volume if call_volume > 0 else None

    return {
        "put_call_oi": put_call_oi,
        "put_call_volume": put_call_volume,
        "zero_dte_volume_share_pct": zero_dte_volume / total_volume * 100 if total_volume > 0 else None,
        "top_oi_strike": top_oi_strike,
        "top_oi": top_oi,
        "top5_oi_share_pct": top5_share,
        "total_open_interest": all_oi if all_oi > 0 else None,
        "total_volume": total_volume if total_volume > 0 else None,
        "put_call_oi_percentile": _percentile_rank(expiry_metrics.get("put_call_oi", pd.Series(dtype=float)), put_call_oi),
        "put_call_volume_percentile": _percentile_rank(
            expiry_metrics.get("put_call_volume", pd.Series(dtype=float)), put_call_volume
        ),
    }


def _iv_rv_spread_metrics(
    stock_df: pd.DataFrame,
    iv_history: pd.DataFrame,
    *,
    trade_date: str,
    current_iv_pct: float | None,
    rv20_pct: float | None,
) -> dict[str, float | None]:
    current_spread = current_iv_pct - rv20_pct if current_iv_pct is not None and rv20_pct is not None else None
    history = _iv_history_until(iv_history, trade_date)
    if history.empty:
        return {"iv_rv20_spread": current_spread, "iv_rv20_percentile": None}

    rows = []
    for _, row in history.iterrows():
        day = str(row.get("trade_date") or "")
        day_iv = pd.to_numeric(pd.Series([row.get("iv_pct")]), errors="coerce").dropna()
        day_rv = calculate_realized_volatility(stock_df, window=20, trade_date=day)
        if day_iv.empty or day_rv is None:
            continue
        rows.append(float(day_iv.iloc[0]) - float(day_rv))
    series = pd.Series(rows, dtype=float)
    return {
        "iv_rv20_spread": current_spread,
        "iv_rv20_percentile": _percentile_rank(series, current_spread),
    }


def _metric_history_until(metrics_history: pd.DataFrame, trade_date: str | None) -> pd.DataFrame:
    if metrics_history is None or metrics_history.empty:
        return pd.DataFrame(columns=MARKET_METRICS_COLUMNS)
    out = metrics_history.copy()
    out["trade_date"] = out["trade_date"].apply(lambda value: normalize_trade_date(value) if value is not None else "")
    out = out.sort_values("trade_date")
    if trade_date:
        out = out[out["trade_date"] <= normalize_trade_date(trade_date)]
    for col in MARKET_METRIC_NUMERIC_COLUMNS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out.reset_index(drop=True)


def _clean_metric_value(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _derive_put_call_skew(put_skew: Any, call_skew: Any) -> float | None:
    put_value = _clean_metric_value(put_skew)
    call_value = _clean_metric_value(call_skew)
    if put_value is None or call_value is None:
        return None
    return put_value - call_value


def apply_historical_percentiles(
    metrics: dict[str, Any],
    metrics_history: pd.DataFrame,
    *,
    trade_date: str | dt.date | dt.datetime,
    window: int = 252,
    min_samples: int = 60,
) -> dict[str, Any]:
    out = dict(metrics or {})
    history = _metric_history_until(metrics_history, normalize_trade_date(trade_date))
    out["historical_percentile_window"] = int(window)
    out["historical_percentile_min_samples"] = int(min_samples)

    for percentile_key in HISTORICAL_PERCENTILE_FIELDS.values():
        out[percentile_key] = None

    if history.empty:
        out["put_call_skew_5pct"] = _derive_put_call_skew(out.get("put_skew_5pct"), out.get("call_skew_5pct"))
        for field in HISTORICAL_PERCENTILE_FIELDS:
            out[f"{field}_history_count"] = 0
            out[f"{field}_insufficient_history"] = True
        return out

    exact = history[history["trade_date"] == normalize_trade_date(trade_date)]
    if not exact.empty:
        exact_row = exact.iloc[-1]
        for col in MARKET_METRICS_COLUMNS:
            if col in {"trade_date", "underlying", "source", "updated_at"}:
                continue
            value = exact_row.get(col)
            if pd.notna(value):
                out[col] = value

    out["put_call_skew_5pct"] = _derive_put_call_skew(out.get("put_skew_5pct"), out.get("call_skew_5pct"))
    if {"put_skew_5pct", "call_skew_5pct"}.issubset(history.columns):
        history["put_call_skew_5pct"] = pd.to_numeric(history["put_skew_5pct"], errors="coerce") - pd.to_numeric(
            history["call_skew_5pct"], errors="coerce"
        )

    history_window = max(int(window or 252), 1)
    min_count = max(int(min_samples or 1), 1)
    for field, percentile_key in HISTORICAL_PERCENTILE_FIELDS.items():
        series = pd.to_numeric(history.get(field, pd.Series(dtype=float)), errors="coerce").dropna().tail(history_window)
        current_value = _clean_metric_value(out.get(field))
        sample_count = int(len(series))
        out[f"{field}_history_count"] = sample_count
        out[f"{field}_insufficient_history"] = sample_count < min_count
        if current_value is None or sample_count < min_count:
            out[percentile_key] = None
        else:
            out[percentile_key] = _percentile_rank(series, current_value)
    return out


def calculate_overview_metrics_from_market_history(
    *,
    stock_df: pd.DataFrame,
    market_metrics_history: pd.DataFrame,
    trade_date: str | dt.date | dt.datetime,
) -> dict[str, Any]:
    trade_date_text = normalize_trade_date(trade_date)
    history = _metric_history_until(market_metrics_history, trade_date_text)
    if history.empty:
        return calculate_volatility_positioning_metrics(
            stock_df=stock_df,
            chain_df=pd.DataFrame(),
            iv_history=pd.DataFrame(columns=["trade_date", "iv_pct"]),
            trade_date=trade_date_text,
            current_iv_pct=None,
            iv_rank=None,
            market_metrics_history=market_metrics_history,
        )

    exact = history[history["trade_date"] == trade_date_text]
    current_row = exact.iloc[-1] if not exact.empty else history.iloc[-1]
    rv20 = calculate_realized_volatility(stock_df, window=20, trade_date=trade_date_text)
    rv60 = calculate_realized_volatility(stock_df, window=60, trade_date=trade_date_text)

    metrics: dict[str, Any] = {
        "rv20_pct": rv20,
        "rv60_pct": rv60,
    }
    for col in MARKET_METRICS_COLUMNS:
        if col in {"trade_date", "underlying", "source", "updated_at"}:
            continue
        value = current_row.get(col)
        if pd.notna(value):
            metrics[col] = value

    current_iv = _clean_metric_value(metrics.get("atm_iv_pct"))
    series = pd.to_numeric(history.get("atm_iv_pct", pd.Series(dtype=float)), errors="coerce").dropna().tail(252)
    if current_iv is not None and not series.empty:
        min_iv = float(series.min())
        max_iv = float(series.max())
        metrics["iv_rank"] = None if math.isclose(max_iv, min_iv) else (current_iv - min_iv) / (max_iv - min_iv) * 100
        metrics["iv_percentile"] = float((series <= current_iv).sum() / len(series) * 100)
        metrics["current_monthly_iv_pct"] = current_iv
        metrics["iv_history_days"] = int(len(series))

        if _clean_metric_value(metrics.get("iv_change_1d")) is None:
            metrics["iv_change_1d"] = current_iv - float(series.iloc[-2]) if len(series) >= 2 else None
        metrics["iv_change_5d"] = current_iv - float(series.iloc[-6]) if len(series) >= 6 else None
        metrics["iv_change_20d"] = current_iv - float(series.iloc[-21]) if len(series) >= 21 else None
    else:
        metrics.setdefault("iv_rank", None)
        metrics.setdefault("iv_percentile", None)
        metrics.setdefault("current_monthly_iv_pct", current_iv)
        metrics.setdefault("iv_history_days", 0)
        metrics.setdefault("iv_change_5d", None)
        metrics.setdefault("iv_change_20d", None)

    if _clean_metric_value(metrics.get("iv_rv20_spread")) is None and current_iv is not None and rv20 is not None:
        metrics["iv_rv20_spread"] = current_iv - rv20

    return apply_historical_percentiles(
        metrics,
        history,
        trade_date=trade_date_text,
    )


def calculate_volatility_positioning_metrics(
    *,
    stock_df: pd.DataFrame,
    chain_df: pd.DataFrame,
    iv_history: pd.DataFrame,
    trade_date: str | dt.date | dt.datetime,
    current_iv_pct: float | None,
    iv_rank: dict[str, Any] | None = None,
    market_metrics_history: pd.DataFrame | None = None,
) -> dict[str, Any]:
    trade_date_text = normalize_trade_date(trade_date)
    rank_metrics = _iv_rank_from_history(
        iv_history,
        current_iv_pct=current_iv_pct,
        trade_date=trade_date_text,
        fallback=iv_rank,
    )
    rv20 = calculate_realized_volatility(stock_df, window=20, trade_date=trade_date_text)
    rv60 = calculate_realized_volatility(stock_df, window=60, trade_date=trade_date_text)
    spread_metrics = _iv_rv_spread_metrics(
        stock_df,
        iv_history,
        trade_date=trade_date_text,
        current_iv_pct=current_iv_pct,
        rv20_pct=rv20,
    )
    metrics: dict[str, Any] = {
        "atm_iv_pct": current_iv_pct,
        "rv20_pct": rv20,
        "rv60_pct": rv60,
        **rank_metrics,
        **spread_metrics,
        **_term_iv_metrics(chain_df),
        **_skew_metrics(chain_df),
        **_positioning_metrics(chain_df),
    }
    if market_metrics_history is not None:
        metrics = apply_historical_percentiles(
            metrics,
            market_metrics_history,
            trade_date=trade_date_text,
        )
    return metrics


def _duplicate_count(engine, table_name: str, key_columns: list[str]) -> int | None:
    table_name = safe_table_name(table_name)
    columns = table_columns(engine, table_name)
    if not all(col in columns for col in key_columns):
        return None
    group_cols = ", ".join(safe_table_name(col) for col in key_columns)
    sql = text(
        f"""
        SELECT COALESCE(SUM(extra_count), 0)
        FROM (
            SELECT COUNT(*) - 1 AS extra_count
            FROM {table_name}
            GROUP BY {group_cols}
            HAVING COUNT(*) > 1
        ) dupes
        """
    )
    value = _scalar(engine, sql)
    return int(value or 0)


def collect_option_table_diagnostics(
    underlying: str,
    trade_date: str | dt.date | dt.datetime,
    *,
    use_test_tables: bool = True,
    engine=None,
) -> dict[str, Any]:
    engine = engine or dashboard_engine()
    names = option_table_names(use_test_tables)
    underlying = normalize_underlying(underlying)
    trade_date = normalize_trade_date(trade_date)
    table_rows: list[dict[str, Any]] = []
    missing_rows: list[dict[str, Any]] = []

    if engine is None:
        return {"tables": table_rows, "missing": missing_rows}

    duplicate_keys = {
        "contracts": ["option_ticker"],
        "daily": ["trade_date", "option_ticker"],
        "iv": ["trade_date", "option_ticker"],
        "metrics": ["trade_date", "underlying"],
    }
    latest_col = {"daily": "trade_date", "iv": "trade_date", "metrics": "trade_date"}

    for logical_name, table_name in names.items():
        table_name = safe_table_name(table_name)
        exists = table_exists(engine, table_name)
        columns = table_columns(engine, table_name) if exists else set()
        row_count = _scalar(engine, text(f"SELECT COUNT(*) FROM {table_name}")) if exists else None
        underlying_count = None
        if exists and "underlying" in columns:
            underlying_count = _scalar(
                engine,
                text(f"SELECT COUNT(*) FROM {table_name} WHERE underlying = :underlying"),
                {"underlying": underlying},
            )
        latest_trade_date = None
        if exists and latest_col.get(logical_name) in columns:
            latest_trade_date = _scalar(
                engine,
                text(
                    f"""
                    SELECT MAX(trade_date)
                    FROM {table_name}
                    WHERE underlying = :underlying
                    """
                ),
                {"underlying": underlying},
            )
        duplicate_count = _duplicate_count(engine, table_name, duplicate_keys[logical_name]) if exists else None
        table_rows.append(
            {
                "table": table_name,
                "exists": exists,
                "rows": int(row_count or 0) if exists else 0,
                "underlying_rows": int(underlying_count or 0) if underlying_count is not None else None,
                "latest_trade_date": latest_trade_date,
                "duplicate_keys": duplicate_count,
            }
        )

        if not exists:
            continue
        checks: list[tuple[str, str]] = []
        if logical_name == "contracts":
            checks = [
                ("expiration_type", "expiration_type IS NULL OR expiration_type = '' OR expiration_type = 'unknown'"),
                ("settlement_type", "settlement_type IS NULL OR settlement_type = '' OR settlement_type = 'unknown'"),
            ]
        elif logical_name == "daily":
            checks = [
                ("close", "close IS NULL"),
                ("volume", "volume IS NULL"),
                ("open_interest", "open_interest IS NULL"),
            ]
        elif logical_name == "iv":
            checks = [
                ("iv", "provider_iv IS NULL AND computed_iv IS NULL"),
                ("underlying_price", "underlying_price IS NULL"),
            ]
        elif logical_name == "metrics":
            checks = [
                ("atm_iv_pct", "atm_iv_pct IS NULL"),
                ("iv_rv20_spread", "iv_rv20_spread IS NULL"),
                ("term_slope_30_60", "term_slope_30_60 IS NULL"),
                ("put_call_oi", "put_call_oi IS NULL"),
            ]
        for field, condition in checks:
            if field != "iv" and field not in columns:
                continue
            sql = text(
                f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE underlying = :underlying
                  AND ({condition})
                """
            )
            missing_rows.append(
                {
                    "table": table_name,
                    "field": field,
                    "missing_or_unknown": int(_scalar(engine, sql, {"underlying": underlying}) or 0),
                }
            )

    return {"tables": table_rows, "missing": missing_rows}


def validate_short_cycle_band(chain: pd.DataFrame, band_pct: float = 5.0) -> dict[str, Any]:
    if chain is None or chain.empty:
        return {"status": "no_rows", "short_cycle_rows": 0, "checked_rows": 0, "out_of_band_rows": 0}

    dte = pd.to_numeric(chain.get("dte", pd.Series(dtype=float)), errors="coerce")
    exp_type = chain.get("expiration_type", pd.Series(dtype=object)).astype(str)
    short_cycle = chain[(exp_type != "monthly") | (dte <= 1)].copy()
    if short_cycle.empty:
        return {"status": "ok", "short_cycle_rows": 0, "checked_rows": 0, "out_of_band_rows": 0}

    price = pd.to_numeric(short_cycle.get("underlying_price"), errors="coerce")
    strike = pd.to_numeric(short_cycle.get("strike"), errors="coerce")
    valid = short_cycle[(price > 0) & strike.notna()].copy()
    if valid.empty:
        return {
            "status": "unknown",
            "short_cycle_rows": int(len(short_cycle)),
            "checked_rows": 0,
            "out_of_band_rows": None,
        }

    moneyness = (strike.loc[valid.index] - price.loc[valid.index]).abs() / price.loc[valid.index] * 100
    out_of_band = valid[moneyness > float(band_pct)]
    return {
        "status": "ok" if out_of_band.empty else "fail",
        "short_cycle_rows": int(len(short_cycle)),
        "checked_rows": int(len(valid)),
        "out_of_band_rows": int(len(out_of_band)),
    }
