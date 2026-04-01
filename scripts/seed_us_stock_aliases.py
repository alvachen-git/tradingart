from __future__ import annotations

import os
from typing import Dict, List

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv(override=True)


US_ALIAS_MAP: Dict[str, List[str]] = {
    # Tech
    "AAPL": ["苹果"],
    "MSFT": ["微软"],
    "NVDA": ["英伟达", "英伟达AI"],
    "GOOG": ["谷歌", "谷歌C"],
    "META": ["Meta", "脸书", "元宇宙平台"],
    "AMZN": ["亚马逊"],
    "TSM": ["台积电"],
    "AVGO": ["博通"],
    "AMD": ["超威", "超威半导体"],
    "INTC": ["英特尔"],
    "ORCL": ["甲骨文"],
    "ADBE": ["奥多比"],
    "CRM": ["赛富时"],
    "CSCO": ["思科"],
    "QCOM": ["高通"],
    "TXN": ["德州仪器"],
    "MU": ["美光", "美光科技"],
    "AMAT": ["应用材料"],
    "LRCX": ["拉姆研究", "科磊设备"],
    "KLAC": ["KLA", "科磊KLA"],
    "PANW": ["派拓网络"],
    "CRWD": ["CrowdStrike", "众击"],
    "PLTR": ["帕兰提尔"],
    "SNOW": ["Snowflake", "雪花"],
    "NOW": ["ServiceNow"],
    "ANET": ["Arista", "阿里斯塔网络"],
    "CDNS": ["铿腾电子", "Cadence"],
    "SNPS": ["新思科技", "Synopsys"],
    "INTU": ["Intuit", "财捷"],
    "SHOP": ["Shopify"],
    # AI thematic
    "ARM": ["安谋", "ARM安谋"],
    "MRVL": ["迈威尔", "美满电子"],
    "SMCI": ["超微电脑"],
    "DELL": ["戴尔", "戴尔科技"],
    "ASML": ["阿斯麦"],
    # Financial
    "JPM": ["摩根大通"],
    "BAC": ["美国银行"],
    "WFC": ["富国银行"],
    "C": ["花旗", "花旗集团"],
    "GS": ["高盛"],
    "MS": ["摩根士丹利"],
    "BLK": ["贝莱德"],
    "SCHW": ["嘉信理财"],
    "AXP": ["美国运通"],
    "SPGI": ["标普全球"],
    "V": ["Visa"],
    "MA": ["万事达"],
    "PYPL": ["贝宝", "PayPal"],
    "COF": ["第一资本金融"],
    "USB": ["美国合众银行"],
    "PNC": ["PNC银行"],
    "BK": ["纽约梅隆银行"],
    "ICE": ["洲际交易所"],
    "CME": ["芝商所"],
    "CB": ["安达保险"],
    # Consumer
    "TSLA": ["特斯拉"],
    "HD": ["家得宝"],
    "LOW": ["劳氏"],
    "NKE": ["耐克"],
    "SBUX": ["星巴克"],
    "MCD": ["麦当劳"],
    "CMG": ["奇波雷"],
    "DIS": ["迪士尼"],
    "NFLX": ["奈飞"],
    "BKNG": ["缤客"],
    "COST": ["好市多"],
    "WMT": ["沃尔玛"],
    "TGT": ["塔吉特"],
    "KO": ["可口可乐"],
    "PEP": ["百事可乐"],
    "PG": ["宝洁"],
    "MDLZ": ["亿滋"],
    "PM": ["菲利普莫里斯"],
    "MO": ["奥驰亚"],
    "EL": ["雅诗兰黛"],
    # Space
    "RKLB": ["火箭实验室"],
    "LUNR": ["直觉机器"],
    "ASTS": ["AST太空移动"],
    "PL": ["行星实验室"],
    "SPCE": ["维珍银河"],
    "NOC": ["诺斯罗普格鲁曼"],
    "LMT": ["洛克希德马丁"],
    # Stablecoin infra
    "COIN": ["Coinbase"],
    "HOOD": ["罗宾汉", "Robinhood"],
    "SQ": ["Block", "Square"],
    "MSTR": ["微策略", "Strategy"],
    # Oil
    "XOM": ["埃克森美孚"],
    "CVX": ["雪佛龙"],
    "COP": ["康菲石油"],
    "EOG": ["EOG资源"],
    "OXY": ["西方石油"],
    "SLB": ["斯伦贝谢"],
    "HAL": ["哈里伯顿"],
    "MPC": ["马拉松石油"],
    "VLO": ["瓦莱罗能源"],
    # ETF broad
    "SPY": ["标普500ETF"],
    "IVV": ["标普500ETF贝莱德"],
    "VOO": ["标普500ETF先锋"],
    "QQQ": ["纳指100ETF"],
    "VTI": ["全美股ETF"],
    "IWM": ["罗素2000ETF"],
    "DIA": ["道指ETF"],
    # ETF sector
    "XLK": ["科技板块ETF"],
    "XLF": ["金融板块ETF"],
    "XLE": ["能源板块ETF"],
    "XLV": ["医疗板块ETF"],
    "XLI": ["工业板块ETF"],
    "XLY": ["可选消费ETF"],
    "XLP": ["必选消费ETF"],
    "XLU": ["公用事业ETF"],
    "XLB": ["原材料ETF"],
    "XLC": ["通信服务ETF"],
    "SMH": ["半导体ETF范艾克"],
    "SOXX": ["半导体ETF贝莱德"],
    # ETF macro
    "GLD": ["黄金ETF"],
    "SLV": ["白银ETF"],
    "USO": ["原油ETF"],
    "TLT": ["20年美债ETF"],
    "IEF": ["7-10年美债ETF"],
    "HYG": ["高收益债ETF"],
    "LQD": ["投资级公司债ETF"],
    # Crypto spot ETF
    "IBIT": ["贝莱德比特币ETF"],
    "FBTC": ["富达比特币ETF"],
    "ETHA": ["贝莱德以太坊ETF"],
    "FETH": ["富达以太坊ETF"],
}


def _norm(s: str) -> str:
    return str(s or "").strip().upper()


def main() -> None:
    db_user = os.getenv("DB_USER", "root")
    db_password = os.getenv("DB_PASSWORD", "")
    db_host = os.getenv("DB_HOST", "127.0.0.1")
    db_port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "finance_data")
    db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    engine = create_engine(db_url, pool_pre_ping=True)

    ddl_sql = text(
        """
        CREATE TABLE IF NOT EXISTS us_stock_alias (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(16) NOT NULL,
            alias VARCHAR(64) NOT NULL,
            is_primary TINYINT(1) NOT NULL DEFAULT 0,
            enabled TINYINT(1) NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_us_stock_alias_alias (alias),
            KEY idx_us_stock_alias_ticker (ticker),
            KEY idx_us_stock_alias_enabled (enabled)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )

    upsert_sql = text(
        """
        INSERT INTO us_stock_alias (ticker, alias, is_primary, enabled)
        VALUES (:ticker, :alias, :is_primary, 1)
        ON DUPLICATE KEY UPDATE
            ticker = VALUES(ticker),
            is_primary = VALUES(is_primary),
            enabled = 1,
            updated_at = CURRENT_TIMESTAMP
        """
    )

    tickers = sorted(_norm(k) for k in US_ALIAS_MAP.keys() if _norm(k))
    inserted = 0
    updated = 0

    with engine.begin() as conn:
        conn.execute(ddl_sql)
        before = conn.execute(text("SELECT COUNT(1) AS c FROM us_stock_alias")).mappings().first()["c"]

        for ticker in tickers:
            # 保证 ticker 自身可查
            conn.execute(upsert_sql, {"ticker": ticker, "alias": ticker, "is_primary": 1})
            # 写入中文/英文别名
            for alias in US_ALIAS_MAP.get(ticker, []):
                alias_text = str(alias or "").strip()
                if not alias_text:
                    continue
                conn.execute(
                    upsert_sql,
                    {"ticker": ticker, "alias": alias_text, "is_primary": 0},
                )

        after = conn.execute(text("SELECT COUNT(1) AS c FROM us_stock_alias")).mappings().first()["c"]
        inserted = max(0, after - before)
        # 这里按 upsert 场景给出估算更新数
        expected_total_ops = len(tickers) + sum(len(v) for v in US_ALIAS_MAP.values())
        updated = max(0, expected_total_ops - inserted)

    print("us_stock_alias 批量写入完成")
    print(f" - ticker覆盖数: {len(tickers)}")
    print(f" - 预期写入操作: {len(tickers) + sum(len(v) for v in US_ALIAS_MAP.values())}")
    print(f" - 新增行数(估算): {inserted}")
    print(f" - 更新行数(估算): {updated}")


if __name__ == "__main__":
    main()
