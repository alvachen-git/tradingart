"""
portfolio_tools.py
持仓分析工具模块 - 为Portfolio Analyst Agent提供持仓数据访问和分析能力
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd
from langchain_core.tools import tool

from portfolio_analysis_service import (
    get_user_portfolio_snapshot,
    get_user_portfolio_positions_df
)


def _parse_datetime(dt_str: str) -> Optional[datetime]:
    """解析多种格式的时间字符串"""
    if not dt_str:
        return None
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(str(dt_str).strip(), fmt)
        except ValueError:
            continue
    return None


def _calculate_profit_loss(row: pd.Series) -> tuple[Optional[float], Optional[float]]:
    """计算单个持仓的盈亏"""
    try:
        quantity = float(row.get('quantity', 0))
        price = float(row.get('price', 0))
        cost_price = float(row.get('cost_price', 0))

        if quantity <= 0 or cost_price <= 0:
            return None, None

        current_value = quantity * price if price > 0 else float(row.get('market_value', 0))
        cost_value = quantity * cost_price

        if current_value <= 0 or cost_value <= 0:
            return None, None

        pnl = current_value - cost_value
        pnl_pct = (pnl / cost_value) * 100

        return round(pnl, 2), round(pnl_pct, 2)
    except (TypeError, ValueError, KeyError):
        return None, None


@tool
def get_user_portfolio_summary(user_id: str) -> str:
    """
    获取用户持仓摘要信息（轻量级）

    Args:
        user_id: 用户ID

    Returns:
        持仓摘要的JSON字符串，包含：
        - has_portfolio: 是否有持仓数据
        - total_positions: 持仓数量
        - total_market_value: 总市值
        - top_holdings: 前3大持仓
        - top_industries: 主要行业
        - risk_level: 风险等级
        - last_updated: 最后更新时间
        - ai_summary: AI生成的持仓总结
    """
    try:
        snapshot = get_user_portfolio_snapshot(user_id)

        if not snapshot or snapshot.get('recognized_count', 0) == 0:
            return json.dumps({
                "has_portfolio": False,
                "message": "用户暂无持仓数据"
            }, ensure_ascii=False)

        # 获取持仓明细
        positions_df = get_user_portfolio_positions_df(user_id)

        # 计算总市值
        total_mv = 0.0
        if not positions_df.empty and 'market_value' in positions_df.columns:
            total_mv = positions_df['market_value'].sum()

        # 获取前3大持仓
        top_holdings = []
        if not positions_df.empty:
            top_df = positions_df.nlargest(3, 'market_value') if 'market_value' in positions_df.columns else positions_df.head(3)
            for _, row in top_df.iterrows():
                symbol = row.get('symbol', '')
                name = row.get('name', symbol)
                mv = row.get('market_value', 0)
                weight = (mv / total_mv * 100) if total_mv > 0 else 0
                top_holdings.append(f"{name}({symbol}) {weight:.1f}%")

        # 获取主要行业
        industry_alloc = snapshot.get('industry_allocation', [])
        top_industries = [item.get('industry', '') for item in industry_alloc[:3]]

        # 评估风险等级（基于行业集中度）
        risk_level = "低"
        if industry_alloc:
            top_industry_pct = industry_alloc[0].get('weight_pct', 0)
            if top_industry_pct > 50:
                risk_level = "高"
            elif top_industry_pct > 30:
                risk_level = "中等"

        # 获取更新时间
        updated_at = snapshot.get('updated_at', '')
        last_updated = "未知"
        if updated_at:
            dt = _parse_datetime(str(updated_at))
            if dt:
                # UTC转北京时间
                dt_bj = dt + timedelta(hours=8)
                last_updated = dt_bj.strftime("%Y-%m-%d %H:%M")

        result = {
            "has_portfolio": True,
            "total_positions": snapshot.get('recognized_count', 0),
            "total_market_value": round(total_mv, 2),
            "top_holdings": top_holdings,
            "top_industries": top_industries,
            "risk_level": risk_level,
            "last_updated": last_updated,
            "ai_summary": snapshot.get('summary_text', '暂无总结')
        }

        return json.dumps(result, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({
            "has_portfolio": False,
            "error": f"获取持仓摘要失败: {str(e)}"
        }, ensure_ascii=False)


@tool
def get_user_portfolio_details(user_id: str, top_n: int = 10) -> str:
    """
    获取用户持仓详细信息

    Args:
        user_id: 用户ID
        top_n: 返回前N个持仓，默认10个

    Returns:
        持仓详情的JSON字符串，包含每只股票的完整信息
    """
    try:
        snapshot = get_user_portfolio_snapshot(user_id)
        positions_df = get_user_portfolio_positions_df(user_id)

        if positions_df.empty:
            return json.dumps({
                "has_data": False,
                "message": "用户暂无持仓明细"
            }, ensure_ascii=False)

        # 计算总市值
        total_mv = positions_df['market_value'].sum() if 'market_value' in positions_df.columns else 0

        # 处理前N个持仓
        top_positions = positions_df.head(top_n)

        positions_list = []
        for _, row in top_positions.iterrows():
            mv = float(row.get('market_value', 0))
            weight = (mv / total_mv * 100) if total_mv > 0 else 0

            # 计算盈亏
            pnl, pnl_pct = _calculate_profit_loss(row)

            position = {
                "symbol": row.get('symbol', ''),
                "name": row.get('name', ''),
                "market": row.get('market', 'A'),
                "quantity": float(row.get('quantity', 0)) if pd.notna(row.get('quantity')) else None,
                "market_value": mv,
                "weight": round(weight, 2),
                "price": float(row.get('price', 0)) if pd.notna(row.get('price')) else None,
                "cost_price": float(row.get('cost_price', 0)) if pd.notna(row.get('cost_price')) else None,
                "industry": row.get('industry', '未知'),
                "technical_grade": row.get('technical_grade', '持有'),
                "technical_reason": row.get('technical_reason', ''),
                "profit_loss": pnl,
                "profit_loss_pct": pnl_pct
            }

            # 添加指数相关度（只保留最相关的）
            if 'index_corr' in row and isinstance(row['index_corr'], dict):
                corr_dict = row['index_corr']
                if corr_dict:
                    top_corr = max(corr_dict.items(), key=lambda x: abs(x[1]))
                    position['top_correlated_index'] = top_corr[0]
                    position['correlation'] = round(top_corr[1], 3)

            positions_list.append(position)

        result = {
            "has_data": True,
            "total_positions": len(positions_df),
            "showing": len(positions_list),
            "total_market_value": round(total_mv, 2),
            "positions": positions_list,
            "industry_allocation": snapshot.get('industry_allocation', []),
            "portfolio_corr": snapshot.get('portfolio_corr', {})
        }

        return json.dumps(result, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({
            "has_data": False,
            "error": f"获取持仓详情失败: {str(e)}"
        }, ensure_ascii=False)


@tool
def analyze_user_trading_style(user_id: str) -> str:
    """
    分析用户的交易风格和偏好

    Args:
        user_id: 用户ID

    Returns:
        交易风格分析的JSON字符串
    """
    try:
        snapshot = get_user_portfolio_snapshot(user_id)
        positions_df = get_user_portfolio_positions_df(user_id)

        if positions_df.empty:
            return json.dumps({
                "has_data": False,
                "message": "用户暂无持仓数据，无法分析交易风格"
            }, ensure_ascii=False)

        # 分析市场偏好
        markets = positions_df['market'].value_counts().to_dict() if 'market' in positions_df.columns else {}
        preferred_markets = list(markets.keys())

        # 分析行业偏好
        industry_alloc = snapshot.get('industry_allocation', [])
        preferred_industries = [item['industry'] for item in industry_alloc[:3]]

        # 计算持仓集中度
        total_mv = positions_df['market_value'].sum() if 'market_value' in positions_df.columns else 0
        if total_mv > 0 and not positions_df.empty and 'market_value' in positions_df.columns:
            top_holding_weight = (positions_df['market_value'].iloc[0] / total_mv * 100)
            top3_weight = (positions_df['market_value'].head(3).sum() / total_mv * 100)
        else:
            top_holding_weight = 0
            top3_weight = 0

        # 评估集中度风险
        if top_holding_weight > 40:
            concentration_risk = "高"
        elif top_holding_weight > 25:
            concentration_risk = "中等"
        else:
            concentration_risk = "低"

        # 评估分散度（基于持仓数量和行业分布）
        position_count = len(positions_df)
        industry_count = len(industry_alloc)

        diversification_score = 0
        if position_count >= 10:
            diversification_score += 3
        elif position_count >= 5:
            diversification_score += 2
        elif position_count >= 3:
            diversification_score += 1

        if industry_count >= 5:
            diversification_score += 3
        elif industry_count >= 3:
            diversification_score += 2
        elif industry_count >= 2:
            diversification_score += 1

        if top_holding_weight < 20:
            diversification_score += 2
        elif top_holding_weight < 30:
            diversification_score += 1

        if top3_weight < 50:
            diversification_score += 2
        elif top3_weight < 70:
            diversification_score += 1

        # 推断风格类型
        if concentration_risk == "高" and position_count < 5:
            style = "激进型"
        elif concentration_risk == "低" and position_count >= 8:
            style = "稳健型"
        else:
            style = "平衡型"

        # 生成特征描述
        characteristics = []

        if top_holding_weight > 30:
            characteristics.append(f"单一持仓占比较高({top_holding_weight:.1f}%)")

        if industry_alloc:
            top_industry = industry_alloc[0]['industry']
            top_industry_pct = industry_alloc[0]['weight_pct']
            if top_industry_pct > 40:
                characteristics.append(f"重仓{top_industry}板块({top_industry_pct:.1f}%)")

        if position_count >= 10:
            characteristics.append("持仓较为分散")
        elif position_count <= 3:
            characteristics.append("持仓高度集中")

        # 分析技术评级分布
        if 'technical_grade' in positions_df.columns:
            grade_dist = positions_df['technical_grade'].value_counts().to_dict()
            if grade_dist.get('增持', 0) > len(positions_df) * 0.5:
                characteristics.append("多数持仓处于技术强势")
            elif grade_dist.get('减仓', 0) > len(positions_df) * 0.5:
                characteristics.append("多数持仓处于技术弱势")

        result = {
            "has_data": True,
            "style": style,
            "preferred_markets": preferred_markets,
            "preferred_industries": preferred_industries,
            "avg_position_count": position_count,
            "concentration_risk": concentration_risk,
            "top_holding_weight": round(top_holding_weight, 2),
            "top3_holdings_weight": round(top3_weight, 2),
            "diversification_score": diversification_score,
            "max_diversification_score": 10,
            "characteristics": characteristics
        }

        return json.dumps(result, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({
            "has_data": False,
            "error": f"分析交易风格失败: {str(e)}"
        }, ensure_ascii=False)


@tool
def check_portfolio_risks(user_id: str) -> str:
    """
    检查用户持仓的潜在风险

    Args:
        user_id: 用户ID

    Returns:
        风险检查结果的JSON字符串
    """
    try:
        snapshot = get_user_portfolio_snapshot(user_id)
        positions_df = get_user_portfolio_positions_df(user_id)

        if positions_df.empty:
            return json.dumps({
                "has_data": False,
                "message": "用户暂无持仓数据"
            }, ensure_ascii=False)

        risks = []
        suggestions = []

        # 风险1：持仓集中度风险
        total_mv = positions_df['market_value'].sum() if 'market_value' in positions_df.columns else 0
        if total_mv > 0 and not positions_df.empty and 'market_value' in positions_df.columns:
            top_holding = positions_df.iloc[0]
            top_weight = (top_holding['market_value'] / total_mv * 100)

            if top_weight > 40:
                risks.append({
                    "type": "concentration",
                    "severity": "高",
                    "detail": f"{top_holding.get('name', '')}占比{top_weight:.1f}%，单一持仓过重"
                })
                suggestions.append(f"建议适当降低{top_holding.get('name', '')}仓位至30%以下")
            elif top_weight > 30:
                risks.append({
                    "type": "concentration",
                    "severity": "中",
                    "detail": f"{top_holding.get('name', '')}占比{top_weight:.1f}%，存在集中度风险"
                })

        # 风险2：行业集中度风险
        industry_alloc = snapshot.get('industry_allocation', [])
        if industry_alloc:
            top_industry = industry_alloc[0]
            top_industry_pct = top_industry.get('weight_pct', 0)

            if top_industry_pct > 50:
                risks.append({
                    "type": "industry_concentration",
                    "severity": "高",
                    "detail": f"{top_industry['industry']}占比{top_industry_pct:.1f}%，行业过度集中"
                })
                suggestions.append(f"建议增加其他行业配置，降低{top_industry['industry']}占比")

            # 检查前2大行业是否相关
            if len(industry_alloc) >= 2:
                top2_pct = sum(item['weight_pct'] for item in industry_alloc[:2])
                if top2_pct > 70:
                    risks.append({
                        "type": "industry_concentration",
                        "severity": "中",
                        "detail": f"前2大行业占比{top2_pct:.1f}%，行业分散度不足"
                    })

        # 风险3：指数相关度风险
        portfolio_corr = snapshot.get('portfolio_corr', {})
        if portfolio_corr:
            high_corr_indices = [(k, v) for k, v in portfolio_corr.items() if abs(v) > 0.8]
            if high_corr_indices:
                idx_name, corr_val = high_corr_indices[0]
                risks.append({
                    "type": "correlation",
                    "severity": "中",
                    "detail": f"组合与{idx_name}高度相关({corr_val:.2f})，系统性风险较高"
                })
                suggestions.append("可考虑增加与主要指数低相关或负相关的资产")

        # 风险4：技术面风险
        if 'technical_grade' in positions_df.columns:
            weak_positions = positions_df[positions_df['technical_grade'] == '减仓']
            weak_count = len(weak_positions)
            total_count = len(positions_df)

            if weak_count > total_count * 0.5:
                risks.append({
                    "type": "technical",
                    "severity": "高",
                    "detail": f"{weak_count}/{total_count}只持仓处于减仓评级，技术面整体偏弱"
                })
                suggestions.append("建议关注技术面较弱的持仓，考虑适当减仓")
            elif weak_count > 0:
                weak_names = weak_positions['name'].head(3).tolist()
                risks.append({
                    "type": "technical",
                    "severity": "中",
                    "detail": f"{weak_count}只持仓处于减仓评级: {', '.join(weak_names)}"
                })

        # 风险5：盈亏风险
        loss_count = 0
        heavy_loss_positions = []

        for _, row in positions_df.iterrows():
            pnl, pnl_pct = _calculate_profit_loss(row)
            if pnl_pct is not None and pnl_pct < 0:
                loss_count += 1
                if pnl_pct < -20:
                    heavy_loss_positions.append({
                        'name': row.get('name', ''),
                        'loss_pct': pnl_pct
                    })

        if heavy_loss_positions:
            risks.append({
                "type": "loss",
                "severity": "高",
                "detail": f"{len(heavy_loss_positions)}只持仓亏损超过20%"
            })
            suggestions.append("建议评估深度亏损持仓的基本面，考虑止损或补仓")

        # 评估总体风险等级
        high_risk_count = sum(1 for r in risks if r['severity'] == '高')
        medium_risk_count = sum(1 for r in risks if r['severity'] == '中')

        if high_risk_count >= 2:
            overall_risk = "高"
        elif high_risk_count >= 1 or medium_risk_count >= 3:
            overall_risk = "中等"
        else:
            overall_risk = "低"

        result = {
            "has_data": True,
            "overall_risk_level": overall_risk,
            "risk_count": len(risks),
            "risks": risks,
            "suggestions": suggestions
        }

        return json.dumps(result, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({
            "has_data": False,
            "error": f"检查持仓风险失败: {str(e)}"
        }, ensure_ascii=False)


# 导出所有工具
__all__ = [
    'get_user_portfolio_summary',
    'get_user_portfolio_details',
    'analyze_user_trading_style',
    'check_portfolio_risks'
]
