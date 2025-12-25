import akshare as ak
import pandas as pd
from langchain_core.tools import tool
from datetime import datetime


# --- 辅助函数：清洗和格式化 ---
def format_news(df, limit=10):
    """将新闻 DataFrame 转换为 AI 可读的文本"""
    if df is None or df.empty:
        return "暂无相关新闻。"

    news_text = []
    df = df.head(limit)

    for _, row in df.iterrows():
        # 1. 获取时间 (容错处理)
        time_str = str(row.get('发布时间', ''))
        if len(time_str) > 16: time_str = time_str[:16]  # 截取到分钟

        # 2. 获取内容 (容错处理)
        title = str(row.get('标题', '')).strip()
        content = str(row.get('内容', '')).strip()

        # 互补逻辑：如果内容为空，用标题凑；反之亦然
        if not content or content == 'nan': content = title
        if not title or title == 'nan': title = content[:20]

        # 👇【新增】垃圾过滤逻辑
        if any(x in title for x in ['开户', '广告', '中奖', '领福利']):
            continue

        # 移除无效数据
        if not title and not content: continue

        # 3. 格式化输出
        # 如果标题和内容差不多（重复），只显示一个
        if len(content) < len(title) + 5 and title in content:
            item_str = f"⏰[{time_str}] {content}"
        else:
            # 内容太长截断，保持整洁
            clean_content = content[:200].replace('\n', ' ')
            item_str = f"⏰[{time_str}] **{title}**\n   {clean_content}..."

        news_text.append(item_str)

    return "\n\n".join(news_text) if news_text else "未找到有效新闻内容。"


def standardize_columns(df):
    """
    【核心修复】自动识别并重命名列，防止 KeyError
    不依赖固定的列名，而是通过关键词模糊匹配
    """
    cols = df.columns.tolist()
    col_map = {}

    # 1. 找“标题”列
    # 优先找完全匹配的，再找包含关键词的
    title_col = None
    for c in cols:
        c_str = str(c).lower()
        if c_str in ['title', '标题', 'news_title']:
            title_col = c;
            break
    if not title_col:  # 模糊找
        for c in cols:
            if 'title' in str(c).lower() or '标题' in str(c):
                title_col = c;
                break

    # 2. 找“内容”列
    content_col = None
    for c in cols:
        c_str = str(c).lower()
        if c_str in ['content', '内容', 'news_content', 'digest']:
            content_col = c;
            break
    if not content_col:  # 模糊找
        for c in cols:
            if 'content' in str(c).lower() or '内容' in str(c) or 'digest' in str(c):
                content_col = c;
                break

    # 3. 找“时间”列
    time_col = None
    for c in cols:
        c_str = str(c).lower()
        if c_str in ['time', 'date', 'show_time', 'publish_time', '发布时间']:
            time_col = c;
            break
    if not time_col:
        for c in cols:
            if 'time' in str(c).lower() or 'date' in str(c).lower() or '时间' in str(c):
                time_col = c;
                break

    # 4. 构建映射并重命名
    if title_col: col_map[title_col] = '标题'
    if content_col: col_map[content_col] = '内容'
    if time_col: col_map[time_col] = '发布时间'

    df_new = df.rename(columns=col_map)

    # 5. 兜底逻辑：如果还没找到‘内容’，就把‘标题’复制一份当‘内容’
    if '标题' in df_new.columns and '内容' not in df_new.columns:
        df_new['内容'] = df_new['标题']
    elif '内容' in df_new.columns and '标题' not in df_new.columns:
        df_new['标题'] = df_new['内容']

    # 6. 如果连标题都没有，说明数据源彻底废了
    if '标题' not in df_new.columns:
        # 强行把第一列当标题
        if len(df_new.columns) > 0:
            df_new['标题'] = df_new.iloc[:, 0].astype(str)
            df_new['内容'] = df_new['标题']

    return df_new


@tool
def get_financial_news(query: str = ""):
    """
    【财经新闻搜索工具】
    获取实时的财经快讯，优先使用财联社电报。

    参数:
    - query: 搜索关键词，例如 "白银", "黄金", "原油", "贵州茅台"。
             如果不填，则返回全市场宏观快讯。
    """
    print(f"[*] AI 正在检索新闻: {query if query else '宏观快讯'} ...")

    news_sources = []

    # --- 源 1: 财联社电报 (最快，质量最高) ---
    try:
        df_cls = ak.stock_info_global_cls()
        if not df_cls.empty:
            news_sources.append(("财联社电报", df_cls))
    except Exception as e:
        print(f" [!] 财联社接口调用失败: {e}")

    # --- 源 2: 东财全球快讯 (覆盖面广，宏观强) ---
    try:
        df_em = ak.stock_info_global_em()
        if not df_em.empty:
            news_sources.append(("东财快讯", df_em))
    except Exception as e:
        print(f" [!] 东财接口调用失败: {e}")

    # --- 统一处理与筛选 ---
    final_output = []

    for source_name, df in news_sources:
        try:
            # 🔥【核心调用】智能标准化列名
            df = standardize_columns(df)

            # 确保现在一定有 '内容' 和 '标题' 列，否则 standardize_columns 会做兜底
            if '内容' not in df.columns:
                print(f" [!] {source_name} 格式标准化失败，可用列: {df.columns.tolist()}")
                continue

            # 2. 筛选逻辑
            if query:
                # 模糊匹配
                mask = df['内容'].astype(str).str.contains(query, case=False, na=False) | \
                       df['标题'].astype(str).str.contains(query, case=False, na=False)
                df_filtered = df[mask]

                if not df_filtered.empty:
                    final_output.append(
                        f"📣 **来自 {source_name} 关于 '{query}' 的消息：**\n" + format_news(df_filtered, limit=3))
            else:
                # 无关键词，只取前5条
                final_output.append(f"🌍 **{source_name} 最新头条：**\n" + format_news(df, limit=3))

        except Exception as e:
            print(f" [!] 处理 {source_name} 数据时出错: {e}")
            continue

    if not final_output:
        return "⚠️ 所有新闻接口暂无响应，或未搜索到相关内容。建议稍后再试。"

    return "\n\n".join(final_output)


if __name__ == "__main__":
    # 测试代码
    print(get_financial_news.invoke({"query": "原油"}))