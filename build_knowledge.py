import os
from langchain_community.document_loaders import PyPDFLoader, DirectoryLoader, TextLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.embeddings import DashScopeEmbeddings
from langchain_community.vectorstores import Qdrant
from dotenv import load_dotenv

# 1. 初始化
load_dotenv(override=True)
api_key = os.getenv("DASHSCOPE_API_KEY")

if not api_key:
    print("❌ 未找到 DASHSCOPE_API_KEY")
    exit()

# 使用阿里云通义千问的 Embedding 模型 (性价比高)
embeddings = DashScopeEmbeddings(model="text-embedding-v1")


def build_vector_db():
    print("1. 正在加载文档...")
    # 加载 knowledge_docs 目录下的所有 pdf 和 txt
    loader_pdf = DirectoryLoader('./knowledge_docs', glob="**/*.pdf", loader_cls=PyPDFLoader)
    loader_txt = DirectoryLoader('./knowledge_docs', glob="**/*.txt", loader_cls=TextLoader)

    docs = []
    try:
        docs.extend(loader_pdf.load())
    except:
        pass
    try:
        docs.extend(loader_txt.load())
    except:
        pass

    if not docs:
        print("❌ 没找到文档，请在 knowledge_docs 文件夹里放点东西。")
        return

    print(f"   加载了 {len(docs)} 个文件。")

    # 2. 切分文档 (Chunking)
    print("2. 正在切分文档...")
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,  # 每块约500-1000字
        chunk_overlap=200  # 重叠部分，防止上下文断裂
    )
    splits = text_splitter.split_documents(docs)
    print(f"   切分成了 {len(splits)} 个片段。")

    # 3. 向量化并存储 (使用本地 Qdrant 文件存储)
    print("3. 正在存入向量数据库 (Qdrant)...")
    # 将数据存在本地的 qdrant_db 文件夹里
    Qdrant.from_documents(
        splits,
        embeddings,
        path="./qdrant_db",
        collection_name="finance_knowledge"
    )
    print("✅ 知识库构建完成！")


if __name__ == "__main__":
    build_vector_db()