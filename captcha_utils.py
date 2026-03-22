import random
from PIL import Image, ImageDraw


def generate_captcha_image(length: int = 5):
    """
    生成英文+数字图形验证码图片。
    默认 5 位，排除易混淆字符：0/O、1/I。
    """
    alphabet = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
    code = "".join(random.choices(alphabet, k=max(4, int(length or 5))))

    width, height = 140, 46
    image = Image.new("RGB", (width, height), color=(240, 243, 248))
    draw = ImageDraw.Draw(image)

    for _ in range(6):
        x1 = random.randint(0, width)
        y1 = random.randint(0, height)
        x2 = random.randint(0, width)
        y2 = random.randint(0, height)
        draw.line(((x1, y1), (x2, y2)), fill=(185, 195, 212), width=1)

    for i, char in enumerate(code):
        color = (
            random.randint(20, 140),
            random.randint(20, 140),
            random.randint(20, 140),
        )
        x = 10 + i * 24 + random.randint(-2, 2)
        y = 10 + random.randint(-2, 2)
        draw.text((x, y), char, fill=color)

    return image, code
