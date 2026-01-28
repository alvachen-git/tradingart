import os
from langchain_core.tools import tool
from zhipuai import ZhipuAI
from dotenv import load_dotenv
from langchain_community.chat_models import ChatTongyi # 引入通义千问
from langchain_core.prompts import ChatPromptTemplate

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
    """
    if not ZHIPU_API_KEY:
        return "❌ 错误：未配置 ZHIPUAI_API_KEY"

    # ==========================================
    # 🔥 [新增] 关键词优化层 (Keyword Optimization)
    # ==========================================
    try:
        # 使用便宜且快的模型 (qwen-turbo) 来做关键词提取
        # 注意：这里需要你有 DASHSCOPE_API_KEY
        llm_optimizer = ChatTongyi(model="qwen-turbo", temperature=0.1)

        prompt = ChatPromptTemplate.from_template("""
        你是一个搜索引擎优化专家(SEO)。你的任务是将用户的复杂问题转换为【最适合搜索引擎】的关键词。

        【规则】
        1. 去除“帮我查”、“分析一下”、“最新的”等无关词汇。
        2. 提取核心实体和时间。
        3. 如果包含多个不同主题，用空格分隔。
        4. **直接输出优化后的关键词，不要任何解释。**

        用户问题: {raw_query}
        优化后的搜索词:
        """)

        # 调用 LLM 进行优化
        chain = prompt | llm_optimizer
        optimized_query = chain.invoke({"raw_query": query}).content.strip()

        print(f"🔍 [搜索优化] 原词: '{query}' -> 优化词: '{optimized_query}'")

    except Exception as e:
        print(f"⚠️ 关键词优化失败，降级使用原词: {e}")
        optimized_query = query  # 失败了就用原词兜底

    # ==========================================
    # 🔥 [原有逻辑] 调用智谱进行搜索
    # ==========================================
    try:
        client = ZhipuAI(api_key=ZHIPU_API_KEY)

        # 使用优化后的词去搜
        tools = [{
            "type": "web_search",
            "web_search": {
                "enable": True,
                "search_result": True,
                "search_query": optimized_query  # 👈 这里用优化后的词
            }
        }]

        messages = [
            {
                "role": "user",
                # Prompt 里也可以顺便带上原词，让模型知道上下文
                "content": f"请搜索以下内容：{optimized_query}。原始问题背景：{query}"
            }
        ]

        response = client.chat.completions.create(
            model="glm-4",  # 建议用 GLM-4
            messages=messages,
            tools=tools
        )

        answer = response.choices[0].message.content

        # 如果还是没搜到，尝试回退策略
        if not answer and len(optimized_query) > 10:
            print("⚠️ 搜索结果为空，尝试简化搜索...")
            # 这里可以写更复杂的回退逻辑，比如只搜前两个词

        return answer if answer else "📭 未搜索到相关内容。"

    except Exception as e:
        return f"搜索出错: {e}"


# 测试代码
if __name__ == "__main__":
    # 需要先设置环境变量才能运行测试
    # os.environ["ZHIPUAI_API_KEY"] = "你的key"
    print(search_web.invoke("最近的铝价格走势原因"))