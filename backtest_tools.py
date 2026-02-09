from pydantic import BaseModel, Field
from langchain_core.tools import tool

from backtest_engine import run_max_oi_backtest, run_etf_roll_backtest


class BacktestRequest(BaseModel):
    symbol: str = Field(description="标的，如 510050/510300/白银/黄金/au")
    strategy: str = Field(description="策略：max_oi_call / max_oi_put / double_sell / deep_otm_put")
    start_date: str = Field(description="起始日 YYYYMMDD", default=None)
    end_date: str = Field(description="结束日 YYYYMMDD", default=None)
    fee_rate: float = Field(description="手续费率，如 0.0003", default=0.0003)
    fee_per_lot: float = Field(description="每手固定手续费(元)，仅用于双卖/深虚值回测", default=2.0)


@tool(args_schema=BacktestRequest)
def run_option_backtest(
    symbol: str,
    strategy: str,
    start_date: str = None,
    end_date: str = None,
    fee_rate: float = 0.0003,
    fee_per_lot: float = 2.0,
):
    """
    期权策略回测（MVP）：基于每日持仓量最大合约，T+1 持有，固定手续费。
    strategy: max_oi_call / max_oi_put
    """
    strat = strategy.lower().strip()
    if strat in {"double_sell", "deep_otm_put"}:
        result = run_etf_roll_backtest(
            underlying=symbol,
            strategy=strat,
            start_date=start_date,
            end_date=end_date,
            fee_per_lot=fee_per_lot,
        )
    else:
        if strat not in {"max_oi_call", "max_oi_put"}:
            return "⚠️ strategy 仅支持: max_oi_call / max_oi_put / double_sell / deep_otm_put"

        option_type = "C" if strat == "max_oi_call" else "P"
        result = run_max_oi_backtest(
            symbol=symbol,
            option_type=option_type,
            start_date=start_date,
            end_date=end_date,
            fee_rate=fee_rate,
        )

    if "error" in result:
        return result["error"]

    summary = result["summary"]
    trades = result["trades"].tail(5) if not result["trades"].empty else result["trades"]

    fee_line = (
        f"- 手续费(每手): {summary['fee_per_lot']}\n"
        if "fee_per_lot" in summary
        else f"- 手续费率: {summary['fee_rate']:.4f}\n"
    )

    asset_type = summary.get("asset_type", "etf")
    ann_pct = summary.get("annualized_return_pct", None)
    ann_pct_line = f"- 年化收益率: {ann_pct:.2%}\n" if ann_pct is not None else "- 年化收益率: N/A\n"

    summary_md = (
        f"**期权回测结果**\n"
        f"- 标的: {summary['symbol']} ({asset_type})\n"
        f"- 策略: {summary['strategy']}\n"
        f"- 区间: {summary['start_date']} ~ {summary['end_date']}\n"
        f"- 交易次数: {summary['trades']}\n"
        f"- 总盈亏: {summary.get('total_pnl', summary.get('total_return', 0.0)):.2f}\n"
        f"- 年化盈亏: {summary.get('annualized_pnl', summary.get('annualized_return', 0.0)):.2f}\n"
        f"- 最大回撤: {summary['max_drawdown']:.2f}\n"
        f"{ann_pct_line}"
        f"- 胜率: {summary['win_rate']:.2%}\n"
        f"- 平均单笔: {summary['avg_return']:.2f}\n"
        f"{fee_line}"
    )

    trades_md = trades.rename(
        columns={
            "entry_date": "开仓日",
            "exit_date": "平仓日",
            "ts_code": "合约",
            "close": "开仓价",
            "next_close": "平仓价",
            "ret": "单笔收益",
        }
    ).to_markdown(index=False)

    return f"{summary_md}\n最近 5 笔交易:\n{trades_md}"
