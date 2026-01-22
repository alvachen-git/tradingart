import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import os
from dotenv import load_dotenv

load_dotenv(override=True)


# 阿里云 DirectMail 配置 (请在 .env 中设置)
# MAIL_HOST = "smtpdm.aliyun.com"
# MAIL_PORT = 465 or 80
# MAIL_USER = "你的发信地址@yourdomain.com"
# MAIL_PASS = "你的SMTP密码"

def send_email(to_email, subject, html_content):
    """
    发送 HTML 邮件
    """
    my_sender = os.getenv("MAIL_USER")
    my_pass = os.getenv("MAIL_PASS")
    my_host = os.getenv("MAIL_HOST", "smtpdm.aliyun.com")
    my_port = int(os.getenv("MAIL_PORT", 465))

    if not all([my_sender, my_pass]):
        print("❌ 邮件配置缺失，请检查 .env")
        return False

    try:
        msg = MIMEText(html_content, 'html', 'utf-8')
        msg['From'] = formataddr(["交易汇首席投研", my_sender])
        msg['To'] = formataddr(["订阅者", to_email])
        msg['Subject'] = subject

        # 连接 SMTP
        if my_port == 465:
            server = smtplib.SMTP_SSL(my_host, my_port)
        else:
            server = smtplib.SMTP(my_host, my_port)

        server.login(my_sender, my_pass)
        server.sendmail(my_sender, [to_email], msg.as_string())
        server.quit()
        return True
    except Exception as e:
        print(f"❌ 发送邮件给 {to_email} 失败: {e}")
        return False


# 测试用
if __name__ == "__main__":
    # 请先配置好 .env 再测试
    send_email("alvachenart@163.com", "测试邮件", "<h1>Hello AI</h1>")
    pass