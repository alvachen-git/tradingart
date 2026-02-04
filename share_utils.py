"""
情报站分享工具模块 v4.1
参考个人资料页的成功分享逻辑,简化代码
"""
import streamlit as st
import uuid


def add_share_button(content_title: str, content_summary: str, content_html: str,
                        channel_icon: str, pub_time: str, content_id: int):
    """
    在情报站内容下方添加分享按钮
    生成包含完整内容的长图
    """
    st.markdown("<br>", unsafe_allow_html=True)

    unique_id = str(uuid.uuid4())[:8]
    container_id = f"report-{unique_id}"
    btn_id = f"btn-{unique_id}"

    share_html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
<style>
    body {{
        margin: 0;
        padding: 10px;
        background: transparent;
    }}

    /* 🔥 关键改动1：固定400px宽度，和个人资料页一致 */
    #{container_id} {{
        background: linear-gradient(145deg, #1e293b 0%, #0f172a 100%);
        color: #e6e6e6;
        padding: 25px;
        border-radius: 16px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        line-height: 1.6;
        width: 400px;
        position: fixed;
        top: -9999px;
        left: -9999px;
        box-sizing: border-box;
    }}

    #{container_id} .share-header {{
        display: flex;
        align-items: center;
        margin-bottom: 20px;
        border-bottom: 1px solid rgba(255,255,255,0.1);
        padding-bottom: 15px;
    }}

    #{container_id} .share-header .icon {{
        font-size: 24px;
        margin-right: 10px;
    }}

    #{container_id} .share-header .title {{
        font-weight: 900;
        font-size: 16px;
        color: #fff;
    }}

    #{container_id} .share-header .time {{
        font-size: 11px;
        color: #94a3b8;
    }}

    #{container_id} .report-body {{
        font-size: 13px;
        color: #cbd5e1;
        margin-bottom: 20px;
    }}

    /* 内容区域的表格样式 */
    #{container_id} .report-body table {{
        border-collapse: collapse;
        width: 100%;
        margin: 10px 0;
        font-size: 12px;
        color: #e6e6e6;
    }}
    #{container_id} .report-body th,
    #{container_id} .report-body td {{
        border: 1px solid #475569;
        padding: 6px 8px;
        text-align: left;
    }}
    #{container_id} .report-body th {{
        background-color: rgba(255, 255, 255, 0.1);
        color: #fff;
        font-weight: bold;
    }}
    #{container_id} .report-body strong {{
        color: #FFD700;
    }}

    #{container_id} .share-footer {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        border-top: 1px dashed rgba(255,255,255,0.1);
        padding-top: 10px;
        margin-top: 15px;
    }}

    #{container_id} .share-footer .brand {{
        font-size: 11px;
        color: #64748b;
    }}

    #{container_id} .share-footer .url {{
        font-size: 11px;
        color: #3b82f6;
    }}

    /* 按钮样式 - 增大、明显、居中 */
    .share-btn-container {{
        display: flex;
        justify-content: center;
        margin-top: 15px;
    }}
    .share-btn {{
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        border: none;
        color: white;
        padding: 12px 24px;
        border-radius: 20px;
        font-size: 15px;
        font-weight: 600;
        cursor: pointer;
        display: inline-flex;
        align-items: center;
        gap: 8px;
        transition: all 0.3s;
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
    }}
    .share-btn:hover {{
        background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
        transform: translateY(-2px);
        box-shadow: 0 6px 16px rgba(59, 130, 246, 0.4);
    }}
    .share-btn:active {{
        transform: translateY(0);
    }}
</style>
</head>
<body>

<!-- 隐藏的截图容器 -->
<div id="{container_id}">
    <div class="share-header">
        <div class="icon">{channel_icon}</div>
        <div>
            <div class="title">{content_title}</div>
            <div class="time">{pub_time}</div>
        </div>
    </div>

    <div class="report-body">
        {content_html}
    </div>

    <div class="share-footer">
        <div class="brand">爱波塔 · 资金流研究中心</div>
        <div class="url">www.aiprota.com</div>
    </div>
</div>

<!-- 分享按钮 - 居中容器 -->
<div class="share-btn-container">
    <button class="share-btn" id="{btn_id}" onclick="generateAndShare()">
        <i class="fas fa-share-alt"></i>
        <span>分享此情报</span>
    </button>
</div>

<script>
// 🔥 关键改动2：完全复制个人资料页的简洁逻辑
function generateAndShare() {{
    const btn = document.getElementById('{btn_id}');
    const originalText = btn.innerHTML;
    const target = document.getElementById('{container_id}');

    btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 生成中...';

    html2canvas(target, {{
        backgroundColor: null,
        scale: 2,
        logging: false,
        useCORS: true
    }}).then(canvas => {{
        canvas.toBlob(function(blob) {{
            // 🔥 关键改动3：使用简单的英文文件名
            const file = new File([blob], "aiprota_report.png", {{ type: "image/png" }});

            if (navigator.canShare && navigator.canShare({{ files: [file] }})) {{
                navigator.share({{
                    files: [file],
                    title: '爱波塔情报'
                }}).then(() => {{
                    resetBtn(btn, originalText);
                }}).catch(() => {{
                    resetBtn(btn, originalText);
                }});
            }} else {{
                // 不支持分享时，直接下载
                const url = canvas.toDataURL('image/png');
                const link = document.createElement('a');
                link.download = 'aiprota_report.png';
                link.href = url;
                link.click();
                resetBtn(btn, originalText);
            }}
        }}, 'image/png');
    }});
}}

function resetBtn(btn, text) {{
    btn.innerHTML = text;
}}
</script>

</body>
</html>
    """

    st.components.v1.html(share_html, height=70, scrolling=False)