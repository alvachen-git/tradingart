import pandas as pd
from dotenv import load_dotenv  # <--- [新增] 引入 dotenv
import os

# 1. 强制加载 .env 环境变量 (最关键的一步)
load_dotenv(override=True)

# 2. 检查一下有没有加载成功
if not os.getenv("DASHSCOPE_API_KEY"):
    print("❌ 错误: 依然未读取到 DASHSCOPE_API_KEY，请检查 .env 文件路径或内容。")
    exit()

# 3. 之后再引入 memory_utils，因为它一被引入就会尝试初始化 Embedding
from memory_utils import get_vector_store


def view_all_memories():
    print("=== 📖 正在读取本地向量记忆库... ===")

    try:
        # 1. 获取数据库实例
        vector_store = get_vector_store()

        # 2. 直接通过 get() 方法获取所有数据
        # ChromaDB 的 get() 默认返回前 100 条，我们可以设大一点
        data = vector_store.get(limit=20000)

        ids = data['ids']
        documents = data['documents']
        metadatas = data['metadatas']

        if not ids:
            print("📭 记忆库是空的。")
            return

        # 3. 整理成表格显示
        records = []
        for i, doc_id in enumerate(ids):
            meta = metadatas[i] if metadatas else {}
            records.append({
                "User ID": meta.get('user_id', 'Unknown'),
                "Time": meta.get('timestamp', 'N/A'),
                "Content (Snippet)": documents[i][:50] + "..." if len(documents[i]) > 50 else documents[i]
            })

        df = pd.DataFrame(records)
        print(f"✅ 共找到 {len(df)} 条记忆：")
        print("-" * 60)
        # 打印表格，设置显示宽度防止折行太乱
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_colwidth', 100)
        pd.set_option('display.width', 2000)
        print(df)
        print("-" * 60)

    except Exception as e:
        print(f"❌ 读取失败: {e}")


if __name__ == "__main__":
    view_all_memories()