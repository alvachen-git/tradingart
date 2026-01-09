"""
数据库迁移脚本
为 users 表添加邮箱相关字段

运行方式：python migrate_add_email.py
"""

from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv(override=True)

db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)


def migrate():
    """执行数据库迁移"""

    migrations = [
        # 1. 添加 email 字段
        {
            "name": "添加 email 字段",
            "check": "SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='users' AND column_name='email'",
            "sql": "ALTER TABLE users ADD COLUMN email VARCHAR(100) DEFAULT NULL AFTER username"
        },
        # 2. 添加 email_verified 字段
        {
            "name": "添加 email_verified 字段",
            "check": "SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='users' AND column_name='email_verified'",
            "sql": "ALTER TABLE users ADD COLUMN email_verified TINYINT(1) DEFAULT 0 AFTER email"
        },
        # 3. 为 email 添加唯一索引
        {
            "name": "添加 email 唯一索引",
            "check": "SELECT 1 FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='users' AND index_name='idx_email'",
            "sql": "ALTER TABLE users ADD UNIQUE INDEX idx_email (email)"
        },
    ]

    print("=" * 50)
    print("🚀 开始数据库迁移（邮箱功能）...")
    print("=" * 50)

    success_count = 0
    skip_count = 0
    fail_count = 0

    for m in migrations:
        try:
            with engine.connect() as conn:
                # 检查是否已执行
                result = conn.execute(text(m["check"])).fetchone()

                if result:
                    print(f"⏭️  跳过: {m['name']} (已存在)")
                    skip_count += 1
                    continue

            # 执行迁移（需要新连接）
            with engine.begin() as conn:
                conn.execute(text(m["sql"]))
                print(f"✅ 成功: {m['name']}")
                success_count += 1

        except Exception as e:
            print(f"❌ 失败: {m['name']} - {e}")
            fail_count += 1

    print("=" * 50)
    print(f"📊 迁移完成: 成功 {success_count} | 跳过 {skip_count} | 失败 {fail_count}")
    print("=" * 50)


def show_table_structure():
    """显示当前表结构"""
    print("\n📋 当前 users 表结构:")
    print("-" * 60)

    try:
        with engine.connect() as conn:
            result = conn.execute(text("DESCRIBE users"))
            print(f"{'字段名':<20} | {'类型':<25} | {'允许空':<8}")
            print("-" * 60)
            for row in result:
                print(f"{row[0]:<20} | {row[1]:<25} | {row[2]:<8}")
    except Exception as e:
        print(f"查询表结构失败: {e}")


if __name__ == "__main__":
    migrate()
    show_table_structure()