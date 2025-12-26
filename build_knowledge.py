import os
import glob
from qdrant_client import QdrantClient
from qdrant_client.http import models  # 【新增】用於配置向量參數
from langchain_community.document_loaders import PyPDFLoader, TextLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.embeddings import DashScopeEmbeddings
from langchain_community.vectorstores import Qdrant
from dotenv import load_dotenv

# 1. 初始化
load_dotenv(override=True)
if not os.getenv("DASHSCOPE_API_KEY"):
    print("❌ 錯誤：未找到 DASHSCOPE_API_KEY")
    exit()

embeddings = DashScopeEmbeddings(model="text-embedding-v1")


def load_txt_safe(file_path):
    """嘗試多種編碼讀取 TXT"""
    encodings = ['utf-8', 'gbk', 'gb18030', 'utf-16']
    for enc in encodings:
        try:
            loader = TextLoader(file_path, encoding=enc)
            return loader.load(), enc
        except Exception:
            continue
    return [], None


def build_db():
    print("🚀 開始構建知識庫 (終極穩健版)...")

    base_dir = '../future-app/knowledge_docs'
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
        print(f"已創建 {base_dir} 文件夾，請放入文件後重試。")
        return

    # --- A. 加載文檔 ---
    all_docs = []

    # PDF
    pdf_files = glob.glob(os.path.join(base_dir, "**/*.pdf"), recursive=True)
    for pdf_file in pdf_files:
        try:
            loader = PyPDFLoader(pdf_file)
            all_docs.extend(loader.load())
            print(f"   [√] 成功讀取 PDF: {os.path.basename(pdf_file)}")
        except Exception as e:
            print(f"   [!] PDF 讀取錯誤 {os.path.basename(pdf_file)}: {e}")

    # TXT
    txt_files = glob.glob(os.path.join(base_dir, "**/*.txt"), recursive=True)
    for txt_file in txt_files:
        docs, enc = load_txt_safe(txt_file)
        if docs:
            all_docs.extend(docs)
            print(f"   [√] 成功讀取 ({enc}): {os.path.basename(txt_file)}")

    if not all_docs:
        print("\n❌ 沒有加載到任何有效文檔。")
        return

    print(f"\n-> 共加載了 {len(all_docs)} 頁/篇文檔。")

    # --- B. 切分文檔 ---
    print("-> 正在切分文檔...")
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=500,
        chunk_overlap=100
    )
    splits = text_splitter.split_documents(all_docs)
    print(f"-> 切分成了 {len(splits)} 個知識片段。")

    # --- C. 向量化並存入 (核心修復) ---
    print("-> 正在初始化 Qdrant 數據庫...")

    try:
        # 1. 創建本地客戶端
        client = QdrantClient(path="../future-app/qdrant_db")

        # 2. 【關鍵步驟】計算向量維度
        # 我們先試著把一個簡單的詞變成向量，看看它是多少維的
        # DashScope text-embedding-v1 通常是 1536 維
        print("   正在計算向量維度...", end="")
        sample_emb = embeddings.embed_query("test")
        vector_size = len(sample_emb)
        print(f" {vector_size} 維")

        # 3. 【關鍵步驟】顯式創建集合 (Collection)
        # 這會覆蓋舊的同名集合，解決 "Collection not found" 錯誤
        client.recreate_collection(
            collection_name="finance_knowledge",
            vectors_config=models.VectorParams(
                size=vector_size,
                distance=models.Distance.COSINE
            ),
        )
        print("   集合創建成功！")

        # 4. 綁定並寫入
        qdrant = Qdrant(
            client=client,
            collection_name="finance_knowledge",
            embeddings=embeddings,
        )

        print(f"-> 正在寫入 {len(splits)} 條數據 (這可能需要一點時間)...")
        qdrant.add_documents(splits)

        print("✅ 知識庫構建全部完成！")

    except Exception as e:
        print(f"\n❌ 發生錯誤: {e}")
        # 打印詳細錯誤堆棧以便排查
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    build_db()