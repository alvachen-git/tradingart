import os
from langchain_core.tools import tool
from zhipuai import ZhipuAI
from dotenv import load_dotenv

# 初始化智谱客户端
# 建议将 key 放入 .env 文件: ZHIPUAI_API_KEY=...

load_dotenv(override=True)
ZHIPU_API_KEY = os.getenv("ZHIPUAI_API_KEY")

@tool
def search_web(query: str) -> str:
    """
    【互联网搜索工具】
    使用智谱 AI 的内置联网功能进行搜索。
    适用于：查询财经和政治新闻、宏观政策、具体事件细节。

    Args:
        query (str): 搜索关键词或具体的查询问题。
    """
    if not ZHIPU_API_KEY:
        return "❌ 错误：未配置 ZHIPUAI_API_KEY，请检查 .env 文件。"

    print(f"🔍 AI 正在调用智谱联网搜索 (GLM-4): {query} ...")

    try:
        client = ZhipuAI(api_key=ZHIPU_API_KEY)

        # 🔥 [修改 1] 工具定义增强
        # 显式开启 search_result=True，这通常能强制模型进行搜索
        tools = [{
            "type": "web_search",
            "web_search": {
                "enable": True,
                "search_result": True,  # 👈 强制返回搜索来源，激活搜索开关
                "search_query": query
            }
        }]

        messages = [
            {
                "role": "user",
                # 🔥 [修改 2] Prompt 更加强硬，防止模型“偷懒”
                "content": f"你是一个具备联网能力的搜索助手。现有查询：“{query}”。\n请务必调用 web_search 工具获取最新信息。\n不要使用你自己的训练数据，必须基于搜索结果回答。"
            }
        ]

        # 🔥 [修改 3] 强烈建议使用 glm-4 或 glm-4-plus
        # glm-4-flash 经常会忽视工具调用，直接回答“我无法联网”
        response = client.chat.completions.create(
            model="glm-4-plus",  # 👈 建议改为 glm-4
            messages=messages,
            tools=tools
        )

        # 提取内容
        answer = response.choices[0].message.content

        # 调试信息：看看模型到底返回了什么（有时候它会返回 tool_calls 但没有 content）
        # print(f"🔍 [Debug] Raw Response: {response.choices[0].message}")

        if not answer:
            # 如果 content 为空，可能是模型尝试返回 tool_calls 结构（虽然 web_search 通常直接返回 content）
            # 这里的兜底是为了防止报错
            return "⚠️ 搜索成功执行，但模型未生成文本摘要（可能是内容被审核拦截）。"

        return answer

    except Exception as e:
        error_msg = f"互联网搜索工具调用失败: {str(e)}"
        print(error_msg)
        return error_msg


# 测试代码
if __name__ == "__main__":
    # 需要先设置环境变量才能运行测试
    # os.environ["ZHIPUAI_API_KEY"] = "你的key"
    print(search_web.invoke("最近的铝价格走势原因"))