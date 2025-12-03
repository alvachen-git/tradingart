import random
import string
from PIL import Image, ImageDraw, ImageFont
import io


def generate_captcha_image(length=4):
    """
    生成一个 4 位数字验证码图片和对应的字符串
    """
    # 1. 生成随机字符串 (只用数字，方便手机输入)
    code = ''.join(random.choices(string.digits, k=length))

    # 2. 创建图片画布 (宽120，高40)
    width, height = 120, 40
    image = Image.new('RGB', (width, height), color=(240, 240, 240))  # 浅灰背景
    draw = ImageDraw.Draw(image)

    # 3. 添加干扰线 (防止机器识别)
    for _ in range(5):
        x1 = random.randint(0, width)
        y1 = random.randint(0, height)
        x2 = random.randint(0, width)
        y2 = random.randint(0, height)
        draw.line(((x1, y1), (x2, y2)), fill=(200, 200, 200), width=1)

    # 4. 绘制文字
    # 如果没有特殊字体，就用默认字体，尽量画大一点
    # Streamlit Cloud 或 Linux 服务器可能没有好看的字体，这里用默认的最稳妥
    # 为了让字不重叠，我们手动计算位置
    for i, char in enumerate(code):
        # 随机颜色
        color = (random.randint(0, 150), random.randint(0, 150), random.randint(0, 150))
        # 稍微随机一点位置
        x = 10 + i * 25 + random.randint(-2, 2)
        y = 10 + random.randint(-2, 2)

        # 绘制
        draw.text((x, y), char, fill=color)  # 如果字太小，可以考虑加载 ttf 字体

    return image, code