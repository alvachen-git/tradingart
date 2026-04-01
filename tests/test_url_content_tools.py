import socket

import requests

from tools import url_content_tools as uct


def test_extract_first_url_picks_first_and_strips_punctuation():
    text = (
        "这链接里说了啥 https://mp.weixin.qq.com/s/ZUuvQXHN1qXaIF8S0mzlLg）。"
        "还有备用链接 https://example.com/abc"
    )
    assert uct.extract_first_url(text) == "https://mp.weixin.qq.com/s/ZUuvQXHN1qXaIF8S0mzlLg"


def test_is_safe_public_url_blocks_local_and_private_hosts():
    assert not uct.is_safe_public_url("ftp://example.com/a")
    assert not uct.is_safe_public_url("http://localhost:8000/a")
    assert not uct.is_safe_public_url("http://127.0.0.1:8000/a")
    assert not uct.is_safe_public_url("http://10.0.0.1/abc")
    assert not uct.is_safe_public_url("http://192.168.1.10/abc")


def test_is_safe_public_url_allows_public_domain(monkeypatch):
    def _fake_getaddrinfo(*args, **kwargs):
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
        ]

    monkeypatch.setattr(uct.socket, "getaddrinfo", _fake_getaddrinfo)
    assert uct.is_safe_public_url("https://example.com/path?a=1")


def test_extract_main_text_for_wechat_html():
    html = """
    <html>
      <head><title>页面标题</title></head>
      <body>
        <h1 id="activity-name">光纤招标价格，炸了！</h1>
        <div id="js_content">
          <p>2026年3月，国内光纤招标市场，炸了。</p>
          <p>据报道，部分规格价格出现较大波动。</p>
          <script>var bad='x';</script>
        </div>
      </body>
    </html>
    """
    title, content = uct.extract_main_text(html, "https://mp.weixin.qq.com/s/xxx")
    assert "光纤招标价格，炸了！" in title
    assert "国内光纤招标市场" in content
    assert "var bad" not in content


def test_extract_main_text_for_generic_html():
    html = """
    <html>
      <head><title>Generic Title</title></head>
      <body>
        <main>
          <h1>主标题</h1>
          <p>第一段内容。</p>
          <p>第二段内容。</p>
        </main>
        <script>window.bad = 1;</script>
      </body>
    </html>
    """
    title, content = uct.extract_main_text(html, "https://news.example.com/a")
    assert title in {"Generic Title", "主标题"}
    assert "第一段内容" in content
    assert "window.bad" not in content


def test_extract_main_text_from_next_data_json():
    html = """
    <html>
      <head><title>壳页面</title></head>
      <body>
        <div id="__next"></div>
        <script id="__NEXT_DATA__" type="application/json">
        {
          "props": {
            "pageProps": {
              "article": {
                "title": "华尔街见闻测试标题",
                "content": "这是一段来自__NEXT_DATA__的正文内容，长度足够触发结构化提取逻辑，避免壳页面误判。"
              }
            }
          }
        }
        </script>
      </body>
    </html>
    """
    title, content = uct.extract_main_text(html, "https://wallstreetcn.com/articles/123")
    assert "华尔街见闻测试标题" in title
    assert "__NEXT_DATA__" in content


def test_build_link_context_timeout_is_caught(monkeypatch):
    monkeypatch.setattr(uct, "is_safe_public_url", lambda _url: True)

    def _raise_timeout(*args, **kwargs):
        raise requests.Timeout("timed out")

    monkeypatch.setattr(uct, "fetch_html", _raise_timeout)
    result = uct.build_link_context("看这个链接 https://example.com/news")
    assert result["ok"] is False
    assert result["error_code"] == "timeout"


def test_build_link_context_truncates_snippet(monkeypatch):
    monkeypatch.setattr(uct, "is_safe_public_url", lambda _url: True)
    long_text = "A" * 5000
    fake_html = f"<html><body><article><h1>标题</h1><p>{long_text}</p></article></body></html>"
    monkeypatch.setattr(uct, "fetch_html", lambda *args, **kwargs: fake_html)

    result = uct.build_link_context("请分析 https://example.com/long-article")
    assert result["ok"] is True
    assert result["title"]
    assert len(result["snippet"]) == 1200


def test_build_link_context_wechat_too_large_retry_success(monkeypatch):
    monkeypatch.setattr(uct, "is_safe_public_url", lambda _url: True)
    calls = {"n": 0}

    def _fake_fetch(url, timeout=(3, 5), max_bytes=2_000_000):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("too_large")
        return (
            "<html><body>"
            "<h1 id='activity-name'>标题</h1>"
            "<div id='js_content'><p>这是正文内容，用于验证微信 too_large 重试路径正常，"
            "并且文本长度足够通过最小正文长度校验，不会被判定为内容过短。</p></div>"
            "</body></html>"
        )

    monkeypatch.setattr(uct, "fetch_html", _fake_fetch)
    result = uct.build_link_context("请分析 https://mp.weixin.qq.com/s/test123")
    assert result["ok"] is True
    assert "标题" in result["title"]
    assert calls["n"] == 2


def test_build_link_context_non_wechat_too_large_fails(monkeypatch):
    monkeypatch.setattr(uct, "is_safe_public_url", lambda _url: True)

    def _always_large(*args, **kwargs):
        raise ValueError("too_large")

    monkeypatch.setattr(uct, "fetch_html", _always_large)
    result = uct.build_link_context("请分析 https://example.com/long")
    assert result["ok"] is False
    assert result["error_code"] == "too_large"


def test_build_link_context_dynamic_fallback_for_whitelist_domain(monkeypatch):
    monkeypatch.setattr(uct, "is_safe_public_url", lambda _url: True)
    monkeypatch.setattr(uct, "_fetch_wallstreetcn_article_api", lambda _url: ("", "", "api_failed"))
    monkeypatch.setattr(uct, "fetch_html", lambda *args, **kwargs: "<html><body><div id='root'></div></body></html>")
    monkeypatch.setattr(
        uct,
        "_fetch_dynamic_text",
        lambda *args, **kwargs: (
            "动态渲染正文内容，长度足够用于摘要提取与回答。该段用于验证白名单域名在静态正文提取失败后，"
            "能够进入动态渲染兜底流程并返回可用内容。",
            "",
        ),
    )

    result = uct.build_link_context("解读这个链接 https://wallstreetcn.com/articles/3768907")
    assert result["ok"] is True
    assert "动态渲染正文内容" in result["snippet"]


def test_build_link_context_wallstreetcn_api_success(monkeypatch):
    monkeypatch.setattr(uct, "is_safe_public_url", lambda _url: True)
    monkeypatch.setattr(
        uct,
        "_fetch_wallstreetcn_article_api",
        lambda _url: ("华尔街见闻标题", "通过API拿到的正文内容，长度足够用于提取和回答。", ""),
    )
    fetch_called = {"hit": False}

    def _fetch_html(*args, **kwargs):
        fetch_called["hit"] = True
        return "<html></html>"

    monkeypatch.setattr(uct, "fetch_html", _fetch_html)
    result = uct.build_link_context("解读 https://wallstreetcn.com/articles/3768907")
    assert result["ok"] is True
    assert "华尔街见闻标题" in result["title"]
    assert "通过API拿到的正文内容" in result["snippet"]
    assert fetch_called["hit"] is False


def test_build_link_context_wallstreetcn_api_fail_fallback_static(monkeypatch):
    monkeypatch.setattr(uct, "is_safe_public_url", lambda _url: True)
    monkeypatch.setattr(uct, "_fetch_wallstreetcn_article_api", lambda _url: ("", "", "api_failed"))
    monkeypatch.setattr(
        uct,
        "fetch_html",
        lambda *args, **kwargs: (
            "<html><body><article><h1>回退标题</h1><p>"
            "回退正文长度足够，说明API失败后静态流程仍可继续工作，并且不会直接中断。"
            "</p></article></body></html>"
        ),
    )
    result = uct.build_link_context("解读 https://wallstreetcn.com/articles/3768907")
    assert result["ok"] is True
    assert "回退标题" in result["title"]
