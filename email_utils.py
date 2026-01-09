import random
import time
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr  # 🔥 关键：用于正确格式化From字段
from datetime import datetime
from dotenv import load_dotenv
import streamlit as st
import re

load_dotenv(override=True)

# 验证码有效期（秒）
CODE_EXPIRE_SECONDS = 300  # 5分钟
# 发送间隔（秒）
SEND_INTERVAL_SECONDS = 60  # 1分钟内不能重复发送


class EmailVerification:
    """邮箱验证码管理类"""

    def __init__(self):
        """初始化邮件配置"""
        self.smtp_server = os.getenv('EMAIL_SMTP_SERVER', 'smtp.163.com')
        self.smtp_port = int(os.getenv('EMAIL_SMTP_PORT', 465))
        self.sender_email = os.getenv('EMAIL_SENDER')
        self.sender_password = os.getenv('EMAIL_PASSWORD')
        self.sender_name = os.getenv('EMAIL_SENDER_NAME', '爱波塔')

        # 检查配置
        self.is_configured = all([
            self.sender_email,
            self.sender_password
        ])

        if not self.is_configured:
            print("⚠️ 邮箱未配置，将使用开发模式（验证码直接显示）")

    def generate_code(self, length=6):
        """生成随机验证码"""
        return ''.join([str(random.randint(0, 9)) for _ in range(length)])

    def _validate_email(self, email: str) -> bool:
        """验证邮箱格式"""
        if not email:
            return False
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))

    def _create_email_content(self, code: str, purpose: str) -> str:
        """创建邮件HTML内容"""
        purpose_text = {
            'register': '注册账号',
            'login': '登录验证',
            'reset_password': '重置密码',
            'bind_email': '绑定邮箱'
        }.get(purpose, '验证')

        html = f"""
        <div style="max-width: 600px; margin: 0 auto; padding: 20px; font-family: Arial, sans-serif;">
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; border-radius: 10px 10px 0 0;">
                <h1 style="color: white; margin: 0; text-align: center;">{self.sender_name}</h1>
            </div>
            <div style="background: #f9f9f9; padding: 30px; border-radius: 0 0 10px 10px; border: 1px solid #e0e0e0;">
                <h2 style="color: #333; margin-top: 0;">您好！</h2>
                <p style="color: #666; font-size: 16px;">您正在进行 <strong>{purpose_text}</strong> 操作，验证码如下：</p>
                <div style="background: #fff; border: 2px dashed #667eea; padding: 20px; text-align: center; margin: 20px 0; border-radius: 8px;">
                    <span style="font-size: 36px; font-weight: bold; color: #667eea; letter-spacing: 8px;">{code}</span>
                </div>
                <p style="color: #999; font-size: 14px;">验证码 <strong>5分钟</strong> 内有效，请勿泄露给他人。</p>
                <p style="color: #999; font-size: 14px;">如非本人操作，请忽略此邮件。</p>
                <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 20px 0;">
                <p style="color: #bbb; font-size: 12px; text-align: center;">此邮件由系统自动发送，请勿回复</p>
            </div>
        </div>
        """
        return html

    def send_email(self, to_email: str, code: str, purpose: str = "register") -> tuple:
        """
        发送验证码邮件

        Args:
            to_email: 收件邮箱
            code: 验证码
            purpose: 用途

        Returns:
            (success, message)
        """
        if not self.is_configured:
            # 开发模式
            print(f"[开发模式] 向 {to_email} 发送验证码: {code}")
            return True, f"验证码已发送 [开发模式: {code}]"

        try:
            # 创建邮件
            msg = MIMEMultipart('alternative')

            # 🔥 关键修复：使用 formataddr 正确格式化 From 字段
            # 这样可以兼容 QQ邮箱等对邮件头格式要求严格的邮箱
            msg['From'] = formataddr((self.sender_name, self.sender_email))
            msg['To'] = formataddr(('', to_email))
            msg['Subject'] = Header(f'【{self.sender_name}】验证码', 'utf-8')

            # HTML内容
            html_content = self._create_email_content(code, purpose)
            msg.attach(MIMEText(html_content, 'html', 'utf-8'))

            # 发送邮件
            if self.smtp_port == 465:
                # SSL
                server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port)
            else:
                # TLS
                server = smtplib.SMTP(self.smtp_server, self.smtp_port)
                server.starttls()

            server.login(self.sender_email, self.sender_password)
            server.sendmail(self.sender_email, to_email, msg.as_string())
            server.quit()

            print(f"✅ 邮件发送成功: {to_email}")
            return True, "验证码已发送，请查收邮件"

        except smtplib.SMTPAuthenticationError:
            print("❌ 邮箱认证失败，请检查授权码")
            return False, "邮件发送失败，请检查配置"
        except smtplib.SMTPRecipientsRefused as e:
            print(f"❌ 收件人被拒绝: {e}")
            return False, "收件邮箱地址无效"
        except Exception as e:
            print(f"❌ 邮件发送异常: {e}")
            return False, f"发送失败，请稍后重试"

    def send_verification_code(self, email: str, purpose: str = "register") -> tuple:
        """
        发送验证码（带频率限制）

        Args:
            email: 邮箱地址
            purpose: 用途 (register/login/reset_password/bind_email)

        Returns:
            (success, message)
        """
        # 验证邮箱格式
        if not self._validate_email(email):
            return False, "请输入正确的邮箱地址"

        # 检查发送频率
        cache_key = f"email_{purpose}_{email}"
        last_send = st.session_state.get(f"{cache_key}_time", 0)

        if time.time() - last_send < SEND_INTERVAL_SECONDS:
            remaining = int(SEND_INTERVAL_SECONDS - (time.time() - last_send))
            return False, f"请{remaining}秒后再试"

        # 生成验证码
        code = self.generate_code()

        # 发送邮件
        success, message = self.send_email(email, code, purpose)

        if success:
            # 存储验证码到 session_state
            st.session_state[f"{cache_key}_code"] = code
            st.session_state[f"{cache_key}_time"] = time.time()
            st.session_state[f"{cache_key}_expire"] = time.time() + CODE_EXPIRE_SECONDS

        return success, message

    def verify_code(self, email: str, code: str, purpose: str = "register") -> tuple:
        """
        验证验证码

        Args:
            email: 邮箱地址
            code: 用户输入的验证码
            purpose: 用途

        Returns:
            (success, message)
        """
        if not code:
            return False, "请输入验证码"

        cache_key = f"email_{purpose}_{email}"
        stored_code = st.session_state.get(f"{cache_key}_code")
        expire_time = st.session_state.get(f"{cache_key}_expire", 0)

        if not stored_code:
            return False, "请先获取验证码"

        if time.time() > expire_time:
            self._clear_code(email, purpose)
            return False, "验证码已过期，请重新获取"

        if code != stored_code:
            return False, "验证码错误"

        # 验证成功，清理验证码
        self._clear_code(email, purpose)
        return True, "验证成功"

    def _clear_code(self, email: str, purpose: str):
        """清理验证码缓存"""
        cache_key = f"email_{purpose}_{email}"
        for suffix in ['_code', '_time', '_expire']:
            key = f"{cache_key}{suffix}"
            if key in st.session_state:
                del st.session_state[key]


# 全局实例
email_service = EmailVerification()


# ============================================
# 便捷函数（供外部调用）
# ============================================

def send_register_code(email: str) -> tuple:
    """发送注册验证码"""
    return email_service.send_verification_code(email, "register")


def verify_register_code(email: str, code: str) -> tuple:
    """验证注册验证码"""
    return email_service.verify_code(email, code, "register")


def send_reset_password_code(email: str) -> tuple:
    """发送重置密码验证码"""
    return email_service.send_verification_code(email, "reset_password")


def verify_reset_password_code(email: str, code: str) -> tuple:
    """验证重置密码验证码"""
    return email_service.verify_code(email, code, "reset_password")


def send_login_code(email: str) -> tuple:
    """发送登录验证码"""
    return email_service.send_verification_code(email, "login")


def verify_login_code(email: str, code: str) -> tuple:
    """验证登录验证码"""
    return email_service.verify_code(email, code, "login")


def send_bind_email_code(email: str) -> tuple:
    """发送绑定邮箱验证码"""
    return email_service.send_verification_code(email, "bind_email")


def verify_bind_email_code(email: str, code: str) -> tuple:
    """验证绑定邮箱验证码"""
    return email_service.verify_code(email, code, "bind_email")