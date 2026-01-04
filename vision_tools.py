import os
import base64
from http import HTTPStatus
import dashscope

def analyze_financial_image(uploaded_file):
    """
    【全能金融眼 - 省钱版】
    Prompt 极简优化，降低 Token 消耗
    """
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key: return "❌ 未配置 API Key"

    try:
        uploaded_file.seek(0)
        image_bytes = uploaded_file.read()
        base64_data = base64.b64encode(image_bytes).decode('utf-8')
        img_url = f"data:image/png;base64,{base64_data}"
    except Exception as e:
        return f"图片处理错误: {e}"

    # 🔥【优化点】极简指令 Prompt
    # 字数减少约 60%，去除所有废话，直接命中核心任务
    prompt = """
    任务：分析金融图片。
    根据图片类型执行对应逻辑：
    1. [持仓/账户]：OCR提取表格数据(标的/数量/盈亏)，评估仓位风险。
    2. [K线/走势]：判断标的名称，识别趋势(涨/跌/盘)，识别关键支撑压力位及形态。
    3. [文字/研报]：提取核心观点与策略逻辑。
    输出要求：直接输出数据与结论，严禁啰嗦。
    """

    messages = [
        {
            "role": "user",
            "content": [
                {"image": img_url},
                {"text": prompt}
            ]
        }
    ]

    try:
        # 💡 建议：如果心疼钱，可以将 model 改为 'qwen-vl-plus'
        # Plus 版本价格通常大幅低于 Max，且处理 OCR 任务能力仅稍弱一点点
        response = dashscope.MultiModalConversation.call(
            model='qwen-vl-plus',
            messages=messages,
            api_key=api_key
        )

        if response.status_code == HTTPStatus.OK:
            return response.output.choices[0].message.content[0]['text']
        else:
            return f"视觉模型错误: {response.code} - {response.message}"

    except Exception as e:
        return f"识别异常: {str(e)}"