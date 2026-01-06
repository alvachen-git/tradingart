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

    print(f"🔍 AI 正在调用智谱联网搜索: {query} ...")

    try:
        client = ZhipuAI(api_key=ZHIPU_API_KEY)

        # 调用 GLM-4-Flash (目前免费)
        # 启用 web_search 工具
        response = client.chat.completions.create(
            model="glm-4-flash",
            messages=[
                {"role": "user",
                 "content": f"请搜索互联网，查询关于“{query}”的最新详细信息。列出关键事实、数据和来源。"}
            ],
            tools=[
                {
                    "type": "web_search",
                    "web_search": {
                        "search_result": True,  # 强制返回搜索结果
                        "search_query": query
                    }
                }
            ]
        )

        # 获取回答内容
        answer = response.choices[0].message.content

        # 有时候单纯的内容不够，我们可以尝试提取引用链接（如果有的话）
        # 但通常 GLM-4 的 content 里已经包含了总结好的信息，非常适合作为 Agent 的输入

        if not answer:
            return "⚠️ 搜索完成，但没有返回有效内容。"

        return f"### 智谱搜索结果:\n{answer}"

    except Exception as e:
        error_msg = f"❌ 智谱搜索接口报错: {str(e)}"
        print(error_msg)
        return error_msg


# 测试代码
if __name__ == "__main__":
    # 需要先设置环境变量才能运行测试
    # os.environ["ZHIPUAI_API_KEY"] = "你的key"
    print(search_web.invoke("最近的铝价格走势原因"))