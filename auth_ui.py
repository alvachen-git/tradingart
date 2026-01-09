"""
登录/注册 UI 组件 (邮箱验证版)
- 新用户：强制邮箱验证注册
- 登录：支持密码/验证码两种方式
- 找回密码：通过邮箱验证码
"""

import streamlit as st
import time


def show_auth_dialog():
    """
    显示登录/注册对话框

    Returns:
        (logged_in, username, token)
    """
    from auth_utils import (
        register_with_email,
        login_user, login_with_email_code,
        reset_password_with_email
    )
    from email_utils import (
        send_register_code, send_login_code, send_reset_password_code
    )

    # 检查是否已登录
    if st.session_state.get('is_logged_in'):
        return True, st.session_state.get('user_id'), st.session_state.get('token')

    # Tab 切换
    tab1, tab2, tab3 = st.tabs(["🔐 登录", "📝 注册", "🔑 忘记密码"])

    # ============================================
    # Tab 1: 登录
    # ============================================
    with tab1:
        login_method = st.radio(
            "登录方式",
            ["密码登录", "验证码登录"],
            horizontal=True,
            key="login_method"
        )

        if login_method == "密码登录":
            account = st.text_input("用户名/邮箱", key="login_account",
                                    placeholder="输入用户名或邮箱")
            password = st.text_input("密码", type="password", key="login_password")

            col1, col2 = st.columns([1, 1])
            with col1:
                login_btn = st.button("登录", type="primary", use_container_width=True, key="btn_login")
            with col2:
                guest_btn = st.button("游客体验", use_container_width=True, key="btn_guest")

            if guest_btn:
                st.session_state.is_logged_in = True
                st.session_state.user_id = "访客"
                st.session_state.token = None
                st.rerun()

            if login_btn:
                if account and password:
                    with st.spinner("登录中..."):
                        # 🔥 login_user 返回4个值
                        success, msg, token, real_username = login_user(account, password)
                    if success:
                        st.session_state.is_logged_in = True
                        st.session_state.user_id = real_username  # 🔥 使用真正的用户名
                        st.session_state.token = token
                        st.success(msg)
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error(msg)
                else:
                    st.warning("请输入账号和密码")

        else:
            # 验证码登录
            email = st.text_input("邮箱", key="login_email", placeholder="your@email.com")

            col1, col2 = st.columns([2, 1])
            with col1:
                code = st.text_input("验证码", key="login_code", max_chars=6)
            with col2:
                st.write("")
                st.write("")
                if st.button("获取验证码", key="btn_send_login", use_container_width=True):
                    if email:
                        with st.spinner("发送中..."):
                            success, msg = send_login_code(email)
                        if success:
                            st.success(msg)
                        else:
                            st.error(msg)
                    else:
                        st.warning("请输入邮箱")

            if st.button("登录", type="primary", use_container_width=True, key="btn_email_login"):
                if email and code:
                    with st.spinner("登录中..."):
                        # 🔥 返回4个值
                        success, msg, token, real_username = login_with_email_code(email, code)
                    if success:
                        st.session_state.is_logged_in = True
                        st.session_state.user_id = real_username  # 🔥 使用真正的用户名
                        st.session_state.token = token
                        st.success(msg)
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error(msg)
                else:
                    st.warning("请输入邮箱和验证码")

    # ============================================
    # Tab 2: 注册（强制邮箱验证）
    # ============================================
    with tab2:
        st.info("📧 使用邮箱注册，方便找回密码")

        reg_email = st.text_input("邮箱（必填）", key="reg_email",
                                  placeholder="your@email.com",
                                  help="邮箱将用于登录和找回密码")

        col1, col2 = st.columns([2, 1])
        with col1:
            reg_code = st.text_input("验证码", key="reg_code", max_chars=6)
        with col2:
            st.write("")
            st.write("")
            if st.button("获取验证码", key="btn_send_reg", use_container_width=True):
                if reg_email:
                    with st.spinner("发送中..."):
                        success, msg = send_register_code(reg_email)
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
                else:
                    st.warning("请输入邮箱")

        reg_username = st.text_input("用户名（选填）", key="reg_username",
                                     placeholder="不填则自动生成",
                                     help="用户名用于显示，不影响登录")
        reg_password = st.text_input("设置密码", type="password", key="reg_password",
                                     placeholder="至少6位")
        reg_password2 = st.text_input("确认密码", type="password", key="reg_password2")

        if st.button("注册", type="primary", use_container_width=True, key="btn_register"):
            if not reg_email:
                st.warning("📧 邮箱是必填项")
            elif not reg_code:
                st.warning("请输入验证码")
            elif not reg_password:
                st.warning("请设置密码")
            elif len(reg_password) < 6:
                st.warning("密码至少6位")
            elif reg_password != reg_password2:
                st.error("两次密码不一致")
            else:
                with st.spinner("注册中..."):
                    success, msg = register_with_email(
                        email=reg_email,
                        password=reg_password,
                        email_code=reg_code,
                        username=reg_username if reg_username else None
                    )
                if success:
                    st.success(msg)
                    st.balloons()
                    st.info("👆 请切换到登录页面登录")
                else:
                    st.error(msg)

    # ============================================
    # Tab 3: 忘记密码
    # ============================================
    with tab3:
        st.info("📧 通过注册邮箱重置密码")

        reset_email = st.text_input("注册邮箱", key="reset_email", placeholder="your@email.com")

        col1, col2 = st.columns([2, 1])
        with col1:
            reset_code = st.text_input("验证码", key="reset_code", max_chars=6)
        with col2:
            st.write("")
            st.write("")
            if st.button("获取验证码", key="btn_send_reset", use_container_width=True):
                if reset_email:
                    with st.spinner("发送中..."):
                        success, msg = send_reset_password_code(reset_email)
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
                else:
                    st.warning("请输入邮箱")

        new_password = st.text_input("新密码", type="password", key="new_password")
        new_password2 = st.text_input("确认新密码", type="password", key="new_password2")

        if st.button("重置密码", type="primary", use_container_width=True, key="btn_reset"):
            if not reset_email:
                st.warning("请输入邮箱")
            elif not reset_code:
                st.warning("请输入验证码")
            elif not new_password:
                st.warning("请设置新密码")
            elif len(new_password) < 6:
                st.warning("密码至少6位")
            elif new_password != new_password2:
                st.error("两次密码不一致")
            else:
                with st.spinner("重置中..."):
                    success, msg = reset_password_with_email(reset_email, reset_code, new_password)
                if success:
                    st.success(msg)
                    st.balloons()
                else:
                    st.error(msg)

    return False, None, None


def sidebar_user_menu(username: str):
    """
    侧边栏用户菜单
    """
    from auth_utils import get_masked_email

    with st.sidebar:
        st.markdown(f"### 👤 {username}")

        masked_email = get_masked_email(username)
        if masked_email:
            st.caption(f"📧 {masked_email}")
        else:
            st.caption("📧 未绑定邮箱")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("👤 资料", use_container_width=True, key="btn_profile"):
                st.switch_page("pages/09_个人资料.py")
        with col2:
            if st.button("🚪 退出", use_container_width=True, key="btn_logout"):
                st.session_state.is_logged_in = False
                st.session_state.user_id = None
                st.session_state.token = None
                st.rerun()