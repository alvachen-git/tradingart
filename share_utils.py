"""
情报站分享工具模块 v3.0
简化版：只保留生成完整长图功能
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
<style>
    body {{
        margin: 0;
        padding: 20px;
        background: transparent;
    }}

    #{container_id} {{
        max-width: 700px;
        margin: 0 auto;
        background: #0f172a;
        border-radius: 16px;
        overflow: hidden;
        /* 🔥 关键：隐藏到屏幕外，只用于截图 */
        position: fixed;
        left: -9999px;
        top: 0;
    }}

    .share-header {{
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        padding: 25px 30px;
        border-bottom: 1px solid rgba(255,255,255,0.08);
        display: flex;
        align-items: center;
        gap: 15px;
    }}

    .share-header .icon {{
        font-size: 36px;
    }}

    .share-header .info {{
        flex: 1;
    }}

    .share-header .title {{
        color: #f1f5f9;
        font-size: 18px;
        font-weight: 700;
        margin-bottom: 4px;
    }}

    .share-header .time {{
        color: #64748b;
        font-size: 12px;
    }}

    .share-footer {{
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        padding: 20px 30px;
        text-align: center;
        border-top: 1px solid rgba(255,255,255,0.08);
    }}

    .share-footer .brand {{
        color: #94a3b8;
        font-size: 13px;
        margin-bottom: 5px;
    }}

    .share-footer .url {{
        color: #3b82f6;
        font-size: 14px;
        font-weight: 600;
    }}

    .btn-container {{
        text-align: center;
        margin: 30px 0;
    }}

    .share-btn {{
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        color: white;
        border: none;
        padding: 14px 40px;
        border-radius: 12px;
        font-size: 15px;
        font-weight: 600;
        cursor: pointer;
        box-shadow: 0 4px 20px rgba(59,130,246,0.4);
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }}

    .share-btn:hover {{
        transform: translateY(-2px);
        box-shadow: 0 6px 25px rgba(59,130,246,0.5);
    }}

    .share-btn:disabled {{
        opacity: 0.6;
        cursor: not-allowed;
    }}

    #status {{
        margin-top: 20px;
        color: #94a3b8;
        font-size: 14px;
        text-align: center;
        min-height: 25px;
    }}
</style>
</head>
<body>

<div class="btn-container">
    <button id="{btn_id}" class="share-btn">📸 生成分享长图</button>
    <div id="status"></div>
</div>

<div id="{container_id}">
    <div class="share-header">
        <div class="icon">{channel_icon}</div>
        <div class="info">
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

<script>
document.getElementById('{btn_id}').addEventListener('click', function() {{
    const container = document.getElementById('{container_id}');
    const statusDiv = document.getElementById('status');
    const btn = this;

    btn.disabled = true;
    btn.textContent = '⏳ 生成中...';
    statusDiv.textContent = '正在生成图片，请稍候...';

    setTimeout(() => {{
        html2canvas(container, {{
            backgroundColor: '#0f172a',
            scale: 2,
            logging: false,
            useCORS: true,
            allowTaint: true,
            scrollY: 0,
            scrollX: 0,
            windowWidth: container.scrollWidth,
            windowHeight: container.scrollHeight
        }}).then(canvas => {{
            statusDiv.textContent = '图片生成成功！';

            canvas.toBlob(blob => {{
                const filename = '爱波塔_{content_title.replace(" ", "_")}.png';

                // 尝试原生分享（手机端）
                if (navigator.share && navigator.canShare({{files: [new File([blob], filename, {{type: 'image/png'}})]}})) {{
                    navigator.share({{
                        files: [new File([blob], filename, {{type: 'image/png'}})],
                        title: '{content_title}',
                        text: '来自爱波塔资金流研究中心'
                    }}).then(() => {{
                        statusDiv.innerHTML = '<span style="color:#22c55e;">✅ 分享成功！</span>';
                        btn.disabled = false;
                        btn.textContent = '📸 生成分享长图';
                    }}).catch(() => {{
                        downloadImage(canvas, filename);
                    }});
                }} else {{
                    downloadImage(canvas, filename);
                }}
            }}, 'image/png');
        }}).catch(err => {{
            console.error('生成失败:', err);
            statusDiv.innerHTML = '<span style="color:#ef4444;">❌ 生成失败，请重试</span>';
            btn.disabled = false;
            btn.textContent = '📸 生成分享长图';
        }});
    }}, 300);

    function downloadImage(canvas, filename) {{
        const url = canvas.toDataURL('image/png');
        const link = document.createElement('a');
        link.download = filename;
        link.href = url;
        link.click();

        statusDiv.innerHTML = '<span style="color:#22c55e;">✅ 图片已保存！</span>';
        btn.disabled = false;
        btn.textContent = '📸 生成分享长图';
    }}
}});
</script>

</body>
</html>
    """

    st.components.v1.html(share_html, height=100, scrolling=True)