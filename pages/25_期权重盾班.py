import html
from typing import Dict, List

import streamlit as st
from sidebar_navigation import show_navigation
from ui_components import inject_sidebar_toggle_style


st.set_page_config(page_title="期权重盾班 - 爱波塔", page_icon="🛡️", layout="wide")


COURSE_PAGE: Dict[str, object] = {
    "title": "期权重盾班",
    "hero_tagline": "你也能跟机构一样卖期权",
    "hero_summary": "重盾班核心不是教你赌方向，而是教你像保险公司一样经营风险。",
    "hero_chips": ["机构卖方思维", "对冲风险", "稳扎稳打", "策略替代乱卖"],
    "pain_points": [
        "高胜率不等于乱卖，真正决定长期曲线的，是时机、结构、仓位和调整能力。",
        "机构卖方不是靠胆子大，而是靠规则、策略和风控，把波动经营成一门生意。",
    ],
    "problem_image": "https://aiprota-img.oss-cn-beijing.aliyuncs.com/sell.png",
    "problem_side_image": "https://aiprota-img.oss-cn-beijing.aliyuncs.com/tra1.png",
    "video_cover": "https://aiprota-img.oss-cn-beijing.aliyuncs.com/ScreenShot_2026-04-13_103637_301.png",
    "activity_images": [
        "https://aiprota-img.oss-cn-beijing.aliyuncs.com/classroom.jpg",
        "https://aiprota-img.oss-cn-beijing.aliyuncs.com/classrom2.jpg",
    ],
    "audiences": [
        {
            "title": "想系统入门卖方的人",
            "summary": "适合对期权有兴趣，但过去更多是用买方思维理解市场的人。",
            "points": [
                "先把卖方底层逻辑搞清楚，不再把卖方理解成“赌不涨不跌”。",
                "学会判断什么时候更适合做卖方，什么时候宁可空仓也不硬上。",
                "把仓位、结构、边界和纪律，放到交易动作之前。",
            ],
        },
        {
            "title": "有经验但收益不稳定的人",
            "summary": "适合已经做过交易，但盈利模式还不够稳定，或者风控常常慢一步的人。",
            "points": [
                "把零散经验整理成机构化流程，而不是继续凭盘感做卖方。",
                "理解卖认购、卖认沽、双卖和结构化策略该在什么环境下使用。",
                "更关注回撤与稳定性，而不是单次漂亮结果。",
            ],
        },
    ],
    "modules": [
        {
            "index": "01",
            "title": "第一阶段：入门知识｜打好卖方基础",
            "benefit": "从卖方最底层的知识体系开始，把时间价值、虚实值、波动率、盈亏结构和合约理解一次打通。",
            "points": [
                "卖期权的优势解析",
                "时间价值与虚实值理解",
                "卖期权的到期盈亏分析",
                "什么是隐含波动率，波动率如何影响卖方",
                "隐含波动率深度分析（IVSmile、Cskew、Pskew）",
                "卖期权的未到期盈亏分析",
                "什么时候赚波动率？什么时候赚时间价值",
                "正确理解期权卖方的杠杆",
                "如何处理被行权",
                "ETF期权与股指期权合约解析",
            ],
        },
        {
            "index": "02",
            "title": "第二阶段：风险控制｜风控思维全面升级",
            "benefit": "把卖方风控从概念变成实战能力，真正理解保证金、希腊字母和波动率在账户里的作用。",
            "points": [
                "保证金的使用与报酬率思考",
                "为什么要用希腊字母做风控",
                "Delta 的实战风控使用",
                "Gamma 的实战风控使用",
                "Vega 的实战风控使用",
                "波动率如何影响希腊字母",
                "波动率的波动率是什么",
                "组合保证金怎么用？应该用吗？",
            ],
        },
        {
            "index": "03",
            "title": "第三阶段：实战思考｜不同情境布局",
            "benefit": "开始进入真实交易环境，理解不同期限、不同布局、不同风险应对下，卖方应该如何选择结构与动作。",
            "points": [
                "卖方布局的行权价合约选择",
                "持仓量对卖方布局的参考意义",
                "40 天以上期权的操作建议",
                "5~40 天期权的操作建议",
                "5 天内末日期权的操作建议",
                "期权卖方的对冲与止损抉择",
                "动态调整与对冲套路解析",
                "何时该买保险",
                "Delta 中性策略操作",
                "车轮饼策略操作",
            ],
        },
        {
            "index": "04",
            "title": "第四阶段：进阶布局考量｜先为不可胜",
            "benefit": "进入布局前的高级考量，把品种、K 线、突破、预期报酬率与风险评估放进同一个决策框架。",
            "points": [
                "布局前的考量依据",
                "卖期权的品种选择思考",
                "K线如何影响卖方策略选择",
                "从K线结构与形态做卖方布局",
                "突破行情时，卖方该用什么策略应对",
                "预期报酬率计算与意义",
                "事前风险评估一国际局势",
                "多空逆转环境下的风险控制",
                "事前风险评估一成本优势",
            ],
        },
        {
            "index": "05",
            "title": "第五阶段：进阶动态调整｜赢在修正",
            "benefit": "把调整这件事讲透，学会在跳空、趋势反转、结算日逼近时，如何让卖方从被动挨打变成主动修正。",
            "points": [
                "做好这几个就可避免卖方爆仓",
                "如何处理跳空行情",
                "针对方向和隐波的动态调整差异",
                "从多空气势转折判断卖方调整",
                "特殊K线如何影响卖方调整",
                "加仓调整与减仓调整",
                "用卖方部位调整的时机与技巧",
                "用买方部位调整的时机与技巧",
                "结算日逼近的落点预测",
            ],
        },
        {
            "index": "06",
            "title": "第六阶段：资管操作｜打造专业卖方思维",
            "benefit": "最后从资金管理与资管操作的视角，理解大资金如何做卖方、如何分配资金、如何把回撤控制纳入专业系统。",
            "points": [
                "大资金的卖方考量",
                "私募机构的卖方策略解说",
                "如何控制账户回撤",
                "如何分配资金",
                "卖方获利的最佳舞台特征",
                "卖方策略对现货的增益效果",
                "多品种卖方策略管理技巧",
            ],
        },
    ],
    "fit_users": [
        "想从买方思维切到卖方框架的人",
        "像建立长期稳健获利的人",
        "想学会更像机构而不是靠感觉交易的人",
    ],
    "not_fit": [
        "只想一夜暴利的人",
        "不愿接受纪律和风控的人",
        "只想抄作业、不想理解底层逻辑的人",
    ],
    "outcomes": [
        "建立一套完整的卖方交易框架",
        "知道什么时候能卖、什么时候最好不卖",
        "理解如何用仓位与结构控制回撤",
        "把高胜率思维变成可执行流程",
    ],
    "instructors": [
        {
            "tag": "主讲讲师",
            "name": "陈竑廷",
            "role": "交易艺术汇创办人 / 期权资深交易员",
            "image": "https://aiprota-img.oss-cn-beijing.aliyuncs.com/%E5%BE%AE%E4%BF%A1%E5%9B%BE%E7%89%87_20201021170311.png",
            "bio": "清华大学物理系、台湾大学应用物理所，曾任艾扬软件期权总监。长期担任各期货商、和讯网、郑商所、CME、夺冠高手、四川大学、紫金矿业、银河证券、德邦证券等机构期权讲师，也是深圳证券交易所 ETF 期权特聘讲师、多家私募基金公司期权顾问。深入理解期权在投机与风险管理中的策略用法，拥有多年金融和商品期权获利经历；中国开放期权后，曾协助开发期权软件“咏春”，并著有《期权新世界》。",
            "highlights": [
                "清华大学物理系 / 台湾大学应用物理所",
                "深交所 ETF 期权特聘讲师",
                "多家私募基金公司期权顾问",
                "著有《期权新世界》",
            ],
        },
        {
            "tag": "主讲讲师",
            "name": "Jack",
            "role": "古木投资基金经理",
            "image": "https://aiprota-img.oss-cn-beijing.aliyuncs.com/%E5%BE%AE%E4%BF%A1%E5%9B%BE%E7%89%87_20201021170307.png",
            "bio": "台湾大学财务金融本科、政治大学会计研究所，曾任国内期货商资产管理计划首席顾问。1995 年开始金融交易生涯，累计交易经验 20 余年，交易风格以 K 线分析为主、宏观为辅，精研买卖点技巧。长期投入卖方庄家策略，擅长通过总体部位 Greek 的掌控，配合技术面的多空研判，充分发挥操作优势，追求持续稳定获利，同样著有《期权新世界》。",
            "highlights": [
                "20 余年金融交易经验",
                "古木投资基金经理",
                "卖方庄家策略长期实战者",
                "著有《期权新世界》",
            ],
        },
    ],
    "reviews": [
        "https://aiprota-img.oss-cn-beijing.aliyuncs.com/good1.png",
        "https://aiprota-img.oss-cn-beijing.aliyuncs.com/good2.png",
        "https://aiprota-img.oss-cn-beijing.aliyuncs.com/good3.png",
        "https://aiprota-img.oss-cn-beijing.aliyuncs.com/good4.png",
        "https://aiprota-img.oss-cn-beijing.aliyuncs.com/good5.png",
        "https://aiprota-img.oss-cn-beijing.aliyuncs.com/good6.png",
    ],
    "promo": {
        "kicker": "限时报名",
        "title": "在重盾班，还能认识高手战友",
        "summary": "除了主课程，还有群服务，还不定期有线下加强班。",
        "deadline": "6月30日前",
        "scarcity": "名额有限，按报名顺序锁定",
        "offer_primary_title": "6月底前报名",
        "offer_primary_body": "可享本期限时优惠价。越接近截止时间，越容易错过这一轮的更优入场成本。",
        "offer_secondary_title": "报名即送 2 个月 Jack 实盘课",
        "offer_secondary_body": "不是简单送资料，而是直接把额外实战陪跑权益叠上去。名额有限，按先后顺序锁定。",
    },
    "faqs": [
        {
            "question": "新手能不能学？",
            "answer": "可以！主课程（视频课）第一阶段从零讲起，适合所有对期权感兴趣的投资者。",
        },
        {
            "question": "课程偏理论还是实战？",
            "answer": "偏“理论框架 + 实战落地”。重点不是记概念，而是理解这些概念如何决定你真实下单时的生死。",
        },
        {
            "question": "重盾班主课程内容有多少，可看多久？",
            "answer": "主课程内容以视频为主，共12节课约12小时，另外还搭配2个月的实盘直播以及课程交流群，1年内不限次观看复习。",
        },
        {
            "question": "课程适用于哪些期权品种？",
            "answer": "主要针对国内ETF期权、股指期权、商品期权，但也适用于美股期权。",
        },
        {
            "question": "怎么咨询和报名？",
            "answer": "当前统一通过微信咨询。先确认课程是否适合你，再进一步了解安排和报名信息。",
        },
    ],
    "wechat_id": "trader-sec",
    "phone": "17521591756",
    "address": "上海市源深金融大厦A座",
    "wechat_qr": "https://aiprota-img.oss-cn-beijing.aliyuncs.com/jim.png",
    "wechat_qr_secondary": "https://aiprota-img.oss-cn-beijing.aliyuncs.com/ScreenShot_2026-04-10_122444_061.png",
}


def esc(value: object) -> str:
    return html.escape(str(value or ""))


def render_list_items(items: List[str], class_name: str) -> str:
    return "".join(f"<li class='{class_name}'>{esc(item)}</li>" for item in items)


def render_audience_cards(items: List[Dict[str, object]]) -> str:
    blocks = []
    for item in items:
        points_html = "".join(f"<li>{esc(point)}</li>" for point in item["points"])
        blocks.append(
            (
                "<article class='audience-card'>"
                f"<div class='card-tag'>适合人群</div>"
                f"<h3>{esc(item['title'])}</h3>"
                f"<p class='card-summary'>{esc(item['summary'])}</p>"
                f"<ul class='card-list'>{points_html}</ul>"
                "</article>"
            )
        )
    return "".join(blocks)


def render_module_cards(items: List[Dict[str, str]]) -> str:
    blocks = []
    for item in items:
        points = item.get("points", [])
        points_html = ""
        if points:
            points_html = "<ul class='route-points'>" + "".join(f"<li>{esc(point)}</li>" for point in points) + "</ul>"
        blocks.append(
            (
                "<article class='route-step'>"
                f"<div class='route-node'>{esc(item['index'])}</div>"
                "<div class='route-body'>"
                f"<div class='card-tag'>课程路径</div>"
                f"<h3>{esc(item['title'])}</h3>"
                f"<p>{esc(item['benefit'])}</p>"
                f"{points_html}"
                "</div>"
                "</article>"
            )
        )
    return "".join(blocks)


def render_instructor_cards(items: List[Dict[str, object]]) -> str:
    blocks = []
    for item in items:
        tags_html = "".join(f"<span class='mentor-chip'>{esc(tag)}</span>" for tag in item["highlights"])
        blocks.append(
            (
                "<article class='mentor-card'>"
                "<div class='mentor-photo-shell'>"
                f"<img class='mentor-photo' src='{esc(item['image'])}' alt='{esc(item['name'])}'>"
                "</div>"
                "<div class='mentor-copy'>"
                f"<div class='card-tag'>{esc(item['tag'])}</div>"
                f"<h3>{esc(item['name'])}</h3>"
                f"<p class='mentor-role'>{esc(item['role'])}</p>"
                f"<p class='mentor-bio'>{esc(item['bio'])}</p>"
                f"<div class='mentor-meta'>{tags_html}</div>"
                "</div>"
                "</article>"
            )
        )
    return "".join(blocks)


def render_faq_items(items: List[Dict[str, str]]) -> str:
    return "".join(
        (
            "<details class='faq-item'>"
            f"<summary>{esc(item['question'])}</summary>"
            f"<p>{esc(item['answer'])}</p>"
            "</details>"
        )
        for item in items
    )


def render_review_wall(items: List[str]) -> str:
    return "".join(
        (
            "<figure class='review-shot'>"
            f"<img src='{esc(url)}' alt='往期学员评价截图'>"
            "</figure>"
        )
        for url in items
    )


def render_activity_gallery(items: List[str]) -> str:
    return "".join(
        (
            "<figure class='activity-shot'>"
            f"<img src='{esc(url)}' alt='上课活动现场图'>"
            "</figure>"
        )
        for url in items
    )


def render_promo_panel(item: Dict[str, str]) -> str:
    footnote = item.get("footnote", "")
    footnote_html = f"<div class='promo-footnote'>{esc(footnote)}</div>" if footnote else ""
    return (
        "<section class='promo-panel'>"
        "<div class='promo-copy'>"
        f"<div class='promo-kicker'>{esc(item['kicker'])}</div>"
        f"<h3>{esc(item['title'])}</h3>"
        f"<p class='promo-summary'>{esc(item['summary'])}</p>"
        "<div class='promo-meta'>"
        f"<span>{esc(item['deadline'])}</span>"
        f"<span>{esc(item['scarcity'])}</span>"
        "</div>"
        "</div>"
        "<div class='promo-offers'>"
        "<article class='promo-offer primary'>"
        "<div class='promo-badge'>优惠窗口</div>"
        f"<h4>{esc(item['offer_primary_title'])}</h4>"
        f"<p>{esc(item['offer_primary_body'])}</p>"
        "</article>"
        "<article class='promo-offer secondary'>"
        "<div class='promo-badge'>附加权益</div>"
        f"<h4>{esc(item['offer_secondary_title'])}</h4>"
        f"<p>{esc(item['offer_secondary_body'])}</p>"
        "</article>"
        f"{footnote_html}"
        "</div>"
        "</section>"
    )


def build_page_html() -> str:
    page = COURSE_PAGE
    hero_notice = page.get("hero_notice", "")
    hero_notice_html = f"<p class='hero-notice'>{esc(hero_notice)}</p>" if hero_notice else ""
    problem_image_html = (
        f"<div class='image-slot problem-image'><img src='{esc(page['problem_image'])}' alt='卖方逻辑图'></div>"
        if page.get("problem_image")
        else "<div class='image-slot'>认知图 / 卖方逻辑图占位</div>"
    )
    problem_side_image_html = (
        f"<div class='image-slot problem-image problem-side-image'><img src='{esc(page['problem_side_image'])}' alt='卖方认知图'></div>"
        if page.get("problem_side_image")
        else ""
    )
    video_cover_style = ""
    if page.get("video_cover"):
        video_cover_style = (
            "style=\"background:"
            "linear-gradient(180deg, rgba(7,12,20,.06), rgba(7,12,20,.18) 38%, rgba(7,12,20,.72) 100%), "
            f"url('{esc(page['video_cover'])}') center/cover no-repeat;\""
        )
    pains_html = "".join(f"<li>{esc(item)}</li>" for item in page["pain_points"])
    audiences_html = render_audience_cards(page["audiences"])
    modules_html = render_module_cards(page["modules"])
    fit_html = render_list_items(page["fit_users"], "fit-item")
    not_fit_html = render_list_items(page["not_fit"], "not-fit-item")
    outcomes_html = "".join(
        (
            "<article class='outcome-card'>"
            "<div class='outcome-mark'>+</div>"
            f"<p>{esc(item)}</p>"
            "</article>"
        )
        for item in page["outcomes"]
    )
    instructors_html = render_instructor_cards(page["instructors"])
    reviews_html = render_review_wall(page["reviews"])
    activities_html = render_activity_gallery(page["activity_images"])
    activities_section_html = (
        "<div class='activity-gallery-block'>"
        "<div class='activity-gallery-head'>"
        "<div class='card-tag'>课程现场</div>"
        "<h3>上课活动与现场氛围</h3>"
        "<p>除了学习内容，你也可以直观看到课程氛围、讲师互动和现场状态。</p>"
        "</div>"
        f"<div class='activity-gallery'>{activities_html}</div>"
        "</div>"
        if page.get("activity_images")
        else ""
    )
    promo_html = render_promo_panel(page["promo"])
    faqs_html = render_faq_items(page["faqs"])

    return f"""
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=IBM+Plex+Mono:wght@400;500;600&family=Noto+Sans+SC:wght@400;500;700;800&display=swap');
    :root {{
      --bg-0:#050913;
      --bg-1:#091323;
      --bg-2:#101a2e;
      --surface:rgba(12,19,33,.84);
      --surface-soft:rgba(255,255,255,.03);
      --line:rgba(255,255,255,.08);
      --text:#f5f7fb;
      --muted:#a6b2c4;
      --muted-2:#d3dae6;
      --amber:#f0b35d;
      --copper:#cc7c34;
      --cyan:#67d4ff;
      --green:#32d07f;
      --shadow:0 24px 64px rgba(0,0,0,.32);
    }}
    * {{ box-sizing:border-box; }}
    html, body {{ margin:0; padding:0; background:transparent; color:var(--text); font-family:"Noto Sans SC",sans-serif; }}
    body {{
      background:
        radial-gradient(1200px 700px at 80% -10%, rgba(103,212,255,.12), transparent 56%),
        radial-gradient(900px 520px at 10% 0%, rgba(240,179,93,.10), transparent 50%),
        linear-gradient(160deg, var(--bg-0), var(--bg-1) 58%, var(--bg-2));
    }}
    .page {{
      width:100%;
      margin:0;
      padding:0 0 72px;
      color:var(--text);
      font-family:"Noto Sans SC",sans-serif;
    }}
    .band {{
      width:100%;
      padding:0 clamp(32px, 5vw, 88px);
    }}
    .band-inner {{
      width:min(1480px, 100%);
      margin:0 auto;
    }}
    .hero {{
      min-height:800px;
      display:grid;
      grid-template-columns:minmax(0,1.08fr) minmax(420px,.92fr);
      gap:56px;
      align-items:center;
      padding:82px 0 72px;
      position:relative;
    }}
    .hero-copy {{
      align-self:center;
      padding-top:0;
    }}
    .eyebrow {{
      display:inline-flex; align-items:center; gap:10px;
      padding:8px 14px; border-radius:999px;
      border:1px solid rgba(240,179,93,.22); background:rgba(255,255,255,.03);
      color:var(--muted-2); font:700 13px/1 "Noto Sans SC",sans-serif; letter-spacing:.12em;
    }}
    .eyebrow::before {{
      content:""; width:10px; height:10px; border-radius:999px;
      background:linear-gradient(135deg,var(--amber),var(--copper)); box-shadow:0 0 18px rgba(240,179,93,.55);
    }}
    .hero h1 {{
      margin:18px 0 10px; font-family:"Rajdhani","Noto Sans SC",sans-serif;
      font-size:clamp(64px,8vw,104px); line-height:.92; letter-spacing:-.05em;
      color:var(--text);
    }}
    .hero-tagline {{
      margin:0; color:var(--amber); font-size:clamp(24px,2.3vw,34px); font-weight:700;
    }}
    .hero-summary {{
      display:none;
    }}
    .hero-notice {{
      margin:18px 0 0; max-width:720px; color:var(--muted); font-size:15px; line-height:1.9;
    }}
    .hero-manifesto {{
      margin:28px 0 0;
      padding-left:22px;
      border-left:2px solid rgba(240,179,93,.45);
      color:var(--muted-2);
      font-size:20px;
      line-height:1.75;
      max-width:680px;
    }}
    .hero-scroll-cue {{
      position:absolute;
      left:50%;
      bottom:28px;
      transform:translateX(-50%);
      display:flex;
      flex-direction:column;
      align-items:center;
      gap:8px;
      color:rgba(255,255,255,.58);
      text-decoration:none;
      z-index:3;
    }}
    .hero-scroll-cue span {{
      font-size:11px;
      letter-spacing:.18em;
      color:rgba(255,255,255,.34);
    }}
    .hero-scroll-cue i {{
      display:flex;
      align-items:center;
      justify-content:center;
      width:30px;
      height:30px;
      border-radius:999px;
      border:1px solid rgba(255,255,255,.08);
      background:rgba(255,255,255,.02);
      font-style:normal;
      font-size:14px;
      animation:heroArrowFloat 1.8s ease-in-out infinite;
    }}
    @keyframes heroArrowFloat {{
      0%, 100% {{ transform:translateY(0); opacity:.42; }}
      50% {{ transform:translateY(7px); opacity:.9; }}
    }}
    .hero-chip-row, .hero-actions, .hero-meta {{ display:flex; flex-wrap:wrap; }}
    .hero-chip-row {{ gap:14px; margin-top:30px; }}
    .hero-chip {{
      min-height:36px; display:inline-flex; align-items:center; padding:0 16px;
      border-radius:999px; background:rgba(255,255,255,.04); border:1px solid var(--line);
      color:var(--muted-2); font-size:14px;
    }}
    .hero-actions {{ gap:16px; margin-top:34px; }}
    .btn {{
      display:inline-flex; align-items:center; justify-content:center; min-height:52px;
      padding:0 24px; border-radius:999px; text-decoration:none; font-weight:800; transition:transform .18s ease;
    }}
    .btn:hover {{ transform:translateY(-2px); }}
    .btn-primary {{ color:#0c1220; background:linear-gradient(135deg,#f4bf74,#d78645); box-shadow:0 14px 36px rgba(215,134,69,.24); }}
    .btn-secondary {{ color:var(--text); background:rgba(255,255,255,.04); border:1px solid var(--line); }}
    .hero-meta {{ gap:22px; margin-top:28px; color:var(--muted); font-size:15px; }}
    .hero-meta strong {{ color:var(--text); font-family:"IBM Plex Mono",monospace; }}
    .hero-visual {{
      position:relative; min-height:680px; border-radius:40px; overflow:hidden;
      background:
        radial-gradient(circle at 80% 18%, rgba(240,179,93,.22), transparent 18%),
        radial-gradient(circle at 18% 22%, rgba(103,212,255,.18), transparent 18%),
        linear-gradient(150deg, rgba(16,26,46,.86), rgba(8,14,24,.96));
      border:1px solid rgba(255,255,255,.08); box-shadow:var(--shadow);
    }}
    .video-shell {{
      position:absolute;
      inset:28px;
      display:grid;
      grid-template-rows:minmax(0,1fr) auto;
      gap:18px;
      z-index:1;
    }}
    .video-screen {{
      position:relative;
      min-height:0;
      display:grid;
      grid-template-rows:minmax(0, 1fr) auto;
      border-radius:30px;
      overflow:hidden;
      border:1px solid rgba(255,255,255,.08);
      background:
        linear-gradient(rgba(255,255,255,.04) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255,255,255,.04) 1px, transparent 1px),
        linear-gradient(160deg, rgba(16,26,46,.92), rgba(8,14,24,.98));
      background-size:36px 36px, 36px 36px, auto;
      box-shadow:0 24px 60px rgba(0,0,0,.28);
    }}
    .video-screen::before {{
      content:"";
      position:absolute;
      inset:0;
      background:
        radial-gradient(circle at 18% 22%, rgba(103,212,255,.18), transparent 20%),
        radial-gradient(circle at 82% 18%, rgba(240,179,93,.14), transparent 22%);
      pointer-events:none;
    }}
    .video-screen::after {{
      content:"";
      position:absolute;
      inset:18px;
      border-radius:22px;
      border:1px solid rgba(255,255,255,.06);
      pointer-events:none;
    }}
    .video-embed {{
      position:relative;
      min-height:420px;
      z-index:1;
      overflow:hidden;
      border-bottom:1px solid rgba(255,255,255,.06);
    }}
    .video-link-cover {{
      position:relative;
      width:100%;
      min-height:420px;
      display:flex;
      align-items:flex-end;
      padding:30px;
      text-decoration:none;
      background:
        radial-gradient(circle at 18% 18%, rgba(103,212,255,.22), transparent 22%),
        radial-gradient(circle at 82% 16%, rgba(240,179,93,.18), transparent 20%),
        linear-gradient(rgba(255,255,255,.04) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255,255,255,.04) 1px, transparent 1px),
        linear-gradient(160deg, rgba(16,26,46,.95), rgba(8,14,24,.98));
      background-size:auto, auto, 36px 36px, 36px 36px, auto;
      background-position:center;
      color:var(--text);
      transition:transform .28s ease, box-shadow .28s ease;
    }}
    .video-link-cover::after {{
      content:"";
      position:absolute;
      inset:0;
      background:
        radial-gradient(circle at 50% 44%, rgba(10,18,32,.08), rgba(10,18,32,.34) 24%, rgba(10,18,32,.62) 54%, rgba(7,12,20,.82) 100%);
      pointer-events:none;
    }}
    .video-link-cover:hover {{
      transform:scale(1.012);
      box-shadow:0 24px 56px rgba(0,0,0,.28);
    }}
    .video-link-play {{
      position:absolute;
      left:50%;
      top:50%;
      transform:translate(-50%, -50%);
      z-index:1;
      width:96px;
      height:96px;
      display:flex;
      align-items:center;
      justify-content:center;
      border-radius:999px;
      background:linear-gradient(135deg, rgba(240,179,93,.96), rgba(204,124,52,.96));
      color:#07111f;
      font-size:34px;
      font-weight:700;
      box-shadow:0 18px 40px rgba(204,124,52,.28);
    }}
    .video-link-copy {{
      position:relative;
      z-index:1;
      max-width:260px;
    }}
    .video-link-copy .video-mini {{
      display:inline-flex;
      align-items:center;
      min-height:30px;
      padding:0 12px;
      border-radius:999px;
      background:rgba(255,255,255,.06);
      border:1px solid rgba(255,255,255,.08);
      color:var(--muted-2);
      font-size:12px;
      letter-spacing:.08em;
    }}
    .video-link-copy h4 {{
      margin:12px 0 0;
      color:var(--text);
      font-size:16px;
      line-height:1.4;
      letter-spacing:0;
      font-weight:700;
    }}
    .video-link-copy p {{
      display:none;
    }}
    .video-link-action {{
      display:none;
    }}
    .video-overlay {{
      position:relative;
      z-index:1;
      display:none;
    }}
    .video-label {{
      display:inline-flex;
      align-items:center;
      min-height:28px;
      padding:0 10px;
      border-radius:999px;
      background:rgba(255,255,255,.06);
      border:1px solid rgba(255,255,255,.08);
      color:var(--muted-2);
      font-size:12px;
      letter-spacing:.1em;
    }}
    .video-caption {{
      margin:16px 0 0;
      color:var(--text);
      font-family:"Rajdhani","Noto Sans SC",sans-serif;
      font-size:42px;
      line-height:.95;
      letter-spacing:-.03em;
    }}
    .video-sub {{
      margin:12px 0 0;
      color:var(--muted-2);
      font-size:16px;
      line-height:1.8;
    }}
    .video-meta {{
      display:grid;
      grid-template-columns:repeat(2, minmax(0,1fr));
      gap:14px;
    }}
    .video-card {{
      min-height:108px;
      padding:18px 18px 16px;
      border-radius:20px;
      background:rgba(10,18,32,.72);
      border:1px solid rgba(255,255,255,.08);
      backdrop-filter:blur(14px);
    }}
    .video-card .label {{
      display:block;
      color:var(--muted);
      font:700 12px/1 "Noto Sans SC",sans-serif;
      letter-spacing:.12em;
    }}
    .video-card .value {{
      display:block;
      margin-top:8px;
      font-family:"Rajdhani",sans-serif;
      font-size:34px;
      font-weight:700;
      line-height:1;
      color:var(--text);
    }}
    .video-card .sub {{
      display:block;
      margin-top:6px;
      color:var(--amber);
      font-size:13px;
      line-height:1.6;
    }}
    .section {{ margin-top:132px; }}
    .section-head {{ max-width:960px; margin-bottom:34px; }}
    .section-kicker {{ color:var(--amber); font:700 14px/1 "Noto Sans SC",sans-serif; letter-spacing:.16em; }}
    .section h2 {{ margin:18px 0 0; font-family:"Rajdhani","Noto Sans SC",sans-serif; font-size:clamp(44px,5vw,78px); line-height:.96; letter-spacing:-.04em; color:var(--text); }}
    .section-intro {{ margin:22px 0 0; max-width:860px; color:var(--muted-2); font-size:20px; line-height:2; }}
    .problem-grid {{
      display:grid; grid-template-columns:minmax(0,1.08fr) minmax(0,.92fr); gap:28px; align-items:stretch;
    }}
    .problem-lead, .problem-list, .audience-card, .who-card, .outcome-card, .faq-item, .consult-main, .consult-side, .closing, .route-step, .mentor-card {{
      border:1px solid rgba(255,255,255,.08); border-radius:30px; background:linear-gradient(180deg, rgba(255,255,255,.03), transparent 100%), rgba(10,18,32,.76); box-shadow:0 18px 48px rgba(0,0,0,.22);
    }}
    .problem-lead {{
      padding:42px; display:flex; flex-direction:column; justify-content:space-between;
      background:radial-gradient(circle at 100% 0, rgba(240,179,93,.12), transparent 24%), rgba(10,18,32,.76);
    }}
    .problem-lead h3 {{ margin:0; max-width:560px; font-size:48px; line-height:1.02; font-family:"Rajdhani","Noto Sans SC",sans-serif; color:var(--text); }}
    .problem-lead p {{ margin:18px 0 0; max-width:620px; color:var(--muted-2); line-height:2; font-size:18px; }}
    .problem-list {{ padding:34px 34px 30px; }}
    .problem-list ol {{ list-style:none; margin:0; padding:0; display:grid; gap:22px; }}
    .problem-list li {{ color:var(--muted-2); line-height:1.95; padding-bottom:22px; border-bottom:1px solid rgba(255,255,255,.07); font-size:17px; }}
    .problem-list li:last-child {{ border-bottom:none; padding-bottom:0; }}
    .problem-side-image {{ margin-top:28px; min-height:520px; }}
    .audience-grid, .who-grid, .outcome-grid, .consult-grid {{
      display:grid; gap:24px;
    }}
    .audience-grid {{ grid-template-columns:repeat(2, minmax(0,1fr)); }}
    .card-tag {{ color:var(--amber); font:700 12px/1 "Noto Sans SC",sans-serif; letter-spacing:.14em; }}
    .audience-card, .who-card, .consult-main, .consult-side {{ padding:34px; }}
    .audience-card h3, .who-card h3 {{ margin:16px 0 0; font-size:30px; line-height:1.14; color:var(--text); }}
    .card-summary, .consult-main p, .closing p {{ margin:16px 0 0; color:var(--muted-2); line-height:2; font-size:17px; }}
    .card-list, .who-list {{ margin:20px 0 0; padding-left:22px; color:var(--muted); line-height:1.95; font-size:16px; }}
    .route-map {{
      position:relative;
      display:grid;
      grid-template-columns:repeat(2, minmax(0,1fr));
      gap:28px 72px;
      padding-top:16px;
    }}
    .route-map::before {{
      content:"";
      position:absolute;
      left:50%;
      top:0;
      bottom:0;
      width:2px;
      transform:translateX(-1px);
      background:linear-gradient(180deg, rgba(103,212,255,.0), rgba(103,212,255,.32) 15%, rgba(240,179,93,.42) 50%, rgba(103,212,255,.32) 85%, rgba(103,212,255,.0));
    }}
    .route-step {{
      position:relative;
      display:flex;
      align-items:flex-start;
      gap:18px;
      padding:30px 28px;
      min-height:190px;
    }}
    .route-step:nth-child(odd) {{ margin-right:24px; }}
    .route-step:nth-child(even) {{ margin-left:24px; }}
    .route-step::before {{
      content:"";
      position:absolute;
      top:38px;
      width:34px;
      height:2px;
      background:linear-gradient(90deg, rgba(240,179,93,.0), rgba(240,179,93,.65));
    }}
    .route-step:nth-child(odd)::before {{
      right:-52px;
    }}
    .route-step:nth-child(even)::before {{
      left:-52px;
      background:linear-gradient(90deg, rgba(240,179,93,.65), rgba(240,179,93,.0));
    }}
    .route-node {{
      width:56px;
      height:56px;
      flex:0 0 56px;
      display:flex;
      align-items:center;
      justify-content:center;
      border-radius:18px;
      background:linear-gradient(135deg, rgba(240,179,93,.18), rgba(103,212,255,.14));
      border:1px solid rgba(255,255,255,.1);
      color:var(--text);
      font:600 16px/1 "IBM Plex Mono",monospace;
      box-shadow:0 10px 24px rgba(0,0,0,.24);
    }}
    .route-body h3 {{
      margin:12px 0 0;
      font-size:30px;
      line-height:1.12;
      max-width:420px;
      color:var(--text);
    }}
    .route-body p {{
      margin:16px 0 0;
      color:var(--muted-2);
      line-height:1.95;
      font-size:17px;
      max-width:420px;
    }}
    .route-points {{
      margin:18px 0 0;
      padding-left:20px;
      color:var(--muted);
      line-height:1.85;
      font-size:15px;
      max-width:430px;
    }}
    .route-points li {{
      margin-bottom:10px;
    }}
    .route-points li:last-child {{
      margin-bottom:0;
    }}
    .source-note {{ margin-top:20px; color:var(--muted); font-size:15px; line-height:1.9; max-width:920px; }}
    .who-grid {{ grid-template-columns:repeat(2, minmax(0,1fr)); }}
    .who-card.fit {{ border-color:rgba(50,208,127,.22); }}
    .who-card.unfit {{ border-color:rgba(240,179,93,.18); }}
    .fit-item::marker {{ color:var(--green); }}
    .not-fit-item::marker {{ color:var(--amber); }}
    .outcome-grid {{ grid-template-columns:repeat(4, minmax(0,1fr)); }}
    .outcome-card {{ padding:28px; min-height:180px; }}
    .outcome-mark {{
      width:34px; height:34px; display:inline-flex; align-items:center; justify-content:center;
      border-radius:999px; background:rgba(240,179,93,.14); color:var(--amber); font:600 16px/1 "IBM Plex Mono",monospace;
    }}
    .outcome-card p {{ margin:16px 0 0; color:var(--muted-2); line-height:1.9; font-size:16px; }}
    .mentor-stack {{
      display:grid;
      gap:28px;
    }}
    .mentor-card {{
      display:grid;
      grid-template-columns:minmax(280px, 360px) minmax(0, 1fr);
      gap:30px;
      padding:32px;
      align-items:stretch;
      background:
        radial-gradient(circle at 0 0, rgba(103,212,255,.10), transparent 32%),
        radial-gradient(circle at 100% 0, rgba(240,179,93,.12), transparent 28%),
        linear-gradient(180deg, rgba(255,255,255,.03), transparent 100%), rgba(10,18,32,.78);
    }}
    .mentor-photo-shell {{
      min-height:420px;
      border-radius:28px;
      overflow:hidden;
      border:1px solid rgba(255,255,255,.08);
      background:rgba(255,255,255,.03);
      box-shadow:0 16px 40px rgba(0,0,0,.24);
    }}
    .mentor-photo {{
      width:100%;
      height:100%;
      display:block;
      object-fit:cover;
      background:linear-gradient(160deg, rgba(16,26,46,.92), rgba(8,14,24,.96));
    }}
    .mentor-copy {{
      display:flex;
      flex-direction:column;
      justify-content:center;
      min-width:0;
    }}
    .mentor-copy h3 {{
      margin:16px 0 0;
      font-size:42px;
      line-height:1.02;
      font-family:"Rajdhani","Noto Sans SC",sans-serif;
      color:var(--text);
    }}
    .mentor-role {{
      margin:14px 0 0;
      color:var(--amber);
      font-size:20px;
      font-weight:700;
      line-height:1.6;
    }}
    .mentor-bio {{
      margin:18px 0 0;
      color:var(--muted-2);
      font-size:17px;
      line-height:2;
    }}
    .mentor-meta {{
      display:flex;
      flex-wrap:wrap;
      gap:12px;
      margin-top:24px;
    }}
    .mentor-chip {{
      min-height:38px;
      display:inline-flex;
      align-items:center;
      padding:0 14px;
      border-radius:999px;
      background:rgba(255,255,255,.04);
      border:1px solid rgba(255,255,255,.08);
      color:var(--muted-2);
      font-size:14px;
      line-height:1.4;
    }}
    .image-slot {{
      margin-top:22px; min-height:220px; border-radius:24px; display:flex; align-items:center; justify-content:center; text-align:center;
      background:rgba(255,255,255,.02); border:1px dashed rgba(255,255,255,.14); color:var(--muted); font-size:14px; line-height:1.8;
    }}
    .problem-image {{
      overflow:hidden;
      padding:0;
      border-style:solid;
      background:rgba(255,255,255,.03);
    }}
    .problem-image img {{
      width:100%;
      height:100%;
      display:block;
      object-fit:cover;
    }}
    .image-slot.large {{
      min-height:420px;
      height:100%;
    }}
    .image-slot.small {{
      min-height:160px;
    }}
    .faq-list {{
      border-top:1px solid rgba(255,255,255,.08);
      margin-top:8px;
    }}
    .faq-item {{
      padding:0;
      border:none;
      border-radius:0;
      background:transparent;
      box-shadow:none;
      border-bottom:1px solid rgba(255,255,255,.08);
    }}
    .faq-item summary {{
      position:relative;
      list-style:none;
      cursor:pointer;
      padding:28px 56px 28px 0;
      font-weight:700;
      font-size:22px;
      line-height:1.45;
      color:var(--text);
    }}
    .faq-item summary::-webkit-details-marker {{ display:none; }}
    .faq-item summary::after {{
      content:"+";
      position:absolute;
      right:0;
      top:50%;
      transform:translateY(-50%);
      color:var(--muted);
      font-family:"Rajdhani",sans-serif;
      font-size:30px;
      font-weight:700;
    }}
    .faq-item[open] summary::after {{
      content:"×";
      color:var(--amber);
    }}
    .faq-item p {{
      margin:0 0 26px;
      max-width:940px;
      color:var(--muted-2);
      line-height:1.95;
      font-size:17px;
    }}
    .review-wall {{
      display:grid;
      grid-template-columns:repeat(3, minmax(0,1fr));
      gap:22px;
    }}
    .review-shot {{
      margin:0;
      aspect-ratio:4 / 5;
      border-radius:26px;
      overflow:hidden;
      border:1px solid rgba(255,255,255,.08);
      background:rgba(10,18,32,.76);
      box-shadow:0 18px 48px rgba(0,0,0,.22);
    }}
    .review-shot img {{
      width:100%;
      height:100%;
      display:block;
      object-fit:cover;
      background:#0d1524;
    }}
    .activity-gallery-block {{
      margin-top:34px;
    }}
    .activity-gallery-head {{
      max-width:780px;
      margin-bottom:20px;
    }}
    .activity-gallery-head h3 {{
      margin:14px 0 0;
      color:var(--text);
      font-size:34px;
      line-height:1.12;
    }}
    .activity-gallery-head p {{
      margin:14px 0 0;
      color:var(--muted-2);
      font-size:17px;
      line-height:1.95;
    }}
    .activity-gallery {{
      display:grid;
      grid-template-columns:repeat(2, minmax(0,1fr));
      gap:22px;
    }}
    .activity-shot {{
      margin:0;
      min-height:300px;
      border-radius:28px;
      overflow:hidden;
      border:1px solid rgba(255,255,255,.08);
      background:rgba(10,18,32,.76);
      box-shadow:0 18px 48px rgba(0,0,0,.22);
    }}
    .activity-shot img {{
      width:100%;
      height:100%;
      display:block;
      object-fit:cover;
      background:#0d1524;
    }}
    .consult-shell {{
      margin-top:72px;
    }}
    .promo-panel {{
      display:grid;
      grid-template-columns:minmax(0,1.08fr) minmax(380px,.92fr);
      gap:28px;
      padding:34px;
      border-radius:32px;
      border:1px solid rgba(240,179,93,.16);
      background:
        radial-gradient(circle at 0 0, rgba(240,179,93,.14), transparent 28%),
        radial-gradient(circle at 100% 0, rgba(103,212,255,.10), transparent 24%),
        linear-gradient(180deg, rgba(255,255,255,.03), transparent 100%),
        rgba(10,18,32,.84);
      box-shadow:0 24px 64px rgba(0,0,0,.28);
    }}
    .promo-kicker {{
      display:inline-flex;
      align-items:center;
      min-height:32px;
      padding:0 12px;
      border-radius:999px;
      background:rgba(240,179,93,.10);
      border:1px solid rgba(240,179,93,.18);
      color:var(--amber);
      font:700 12px/1 "Noto Sans SC",sans-serif;
      letter-spacing:.14em;
    }}
    .promo-panel h3 {{
      margin:18px 0 0;
      max-width:760px;
      color:var(--text);
      font-family:"Rajdhani","Noto Sans SC",sans-serif;
      font-size:clamp(34px,4vw,56px);
      line-height:.98;
      letter-spacing:-.03em;
    }}
    .promo-summary {{
      margin:18px 0 0;
      max-width:720px;
      color:var(--muted-2);
      font-size:18px;
      line-height:1.95;
    }}
    .promo-meta {{
      display:flex;
      flex-wrap:wrap;
      gap:12px;
      margin-top:22px;
    }}
    .promo-meta span {{
      min-height:34px;
      display:inline-flex;
      align-items:center;
      padding:0 14px;
      border-radius:999px;
      background:rgba(255,255,255,.04);
      border:1px solid rgba(255,255,255,.08);
      color:var(--muted-2);
      font-size:13px;
    }}
    .promo-offers {{
      display:grid;
      gap:16px;
      align-content:start;
    }}
    .promo-offer {{
      padding:22px 22px 20px;
      border-radius:24px;
      border:1px solid rgba(255,255,255,.08);
      background:rgba(255,255,255,.03);
    }}
    .promo-offer.primary {{
      background:
        linear-gradient(135deg, rgba(240,179,93,.14), rgba(240,179,93,.04)),
        rgba(255,255,255,.03);
      border-color:rgba(240,179,93,.2);
    }}
    .promo-offer.secondary {{
      background:
        linear-gradient(135deg, rgba(103,212,255,.10), rgba(103,212,255,.03)),
        rgba(255,255,255,.03);
      border-color:rgba(103,212,255,.18);
    }}
    .promo-badge {{
      color:var(--amber);
      font:700 12px/1 "Noto Sans SC",sans-serif;
      letter-spacing:.12em;
    }}
    .promo-offer h4 {{
      margin:14px 0 0;
      color:var(--text);
      font-size:28px;
      line-height:1.12;
    }}
    .promo-offer p {{
      margin:14px 0 0;
      color:var(--muted-2);
      font-size:16px;
      line-height:1.85;
    }}
    .promo-footnote {{
      padding:14px 16px;
      border-radius:18px;
      border:1px dashed rgba(255,255,255,.12);
      color:var(--muted);
      font-size:14px;
      line-height:1.8;
      background:rgba(255,255,255,.02);
    }}
    .consult-grid {{
      grid-template-columns:minmax(0,1.06fr) minmax(340px,.94fr);
      align-items:stretch;
      margin-top:34px;
    }}
    .consult-main h3 {{
      margin:16px 0 0;
      font-size:38px;
      line-height:1.08;
      font-family:"Rajdhani","Noto Sans SC",sans-serif;
      color:var(--text);
    }}
    .contact-row {{ display:flex; flex-wrap:wrap; gap:14px; margin-top:22px; }}
    .contact-pill {{
      min-height:40px; display:inline-flex; align-items:center; padding:0 16px; border-radius:999px;
      background:rgba(255,255,255,.04); border:1px solid var(--line); color:var(--text); font-size:14px;
    }}
    .contact-pill strong {{ margin-left:8px; color:var(--green); font-family:"IBM Plex Mono",monospace; }}
    .consult-side {{ display:flex; flex-direction:column; align-items:center; justify-content:center; text-align:center; min-height:100%; }}
    .consult-side .card-tag {{
      color:var(--text);
      font-size:14px;
      letter-spacing:.08em;
    }}
    .consult-qr-grid {{
      display:grid;
      grid-template-columns:repeat(2, minmax(0,1fr));
      gap:14px;
      width:100%;
      max-width:420px;
    }}
    .consult-qr-item {{
      display:flex;
      flex-direction:column;
      align-items:center;
      gap:10px;
    }}
    .consult-side img {{ width:164px; max-width:100%; border-radius:20px; border:1px solid var(--line); box-shadow:var(--shadow); }}
    .qr-note {{ margin:16px 0 0; color:var(--muted); line-height:1.85; font-size:15px; }}
    .closing {{
      margin-top:72px; padding:40px 42px; display:grid; grid-template-columns:minmax(0,1fr); gap:18px; align-items:center;
      background:
        radial-gradient(circle at 0 0, rgba(103,212,255,.10), transparent 28%),
        radial-gradient(circle at 100% 0, rgba(240,179,93,.14), transparent 26%),
        rgba(10,18,32,.78);
      border-color:rgba(240,179,93,.18);
    }}
    .closing h3 {{ margin:14px 0 0; font-family:"Rajdhani","Noto Sans SC",sans-serif; font-size:46px; line-height:.98; color:var(--text); }}
    @media (max-width: 1100px) {{
      .hero, .problem-grid, .audience-grid, .route-map, .mentor-card, .who-grid, .outcome-grid, .review-wall, .activity-gallery, .promo-panel, .consult-grid, .closing {{
        grid-template-columns:1fr;
      }}
      .consult-qr-grid {{
        grid-template-columns:repeat(2, minmax(0,164px));
        justify-content:center;
      }}
      .hero {{ min-height:auto; }}
      .hero-visual {{ min-height:620px; }}
      .band {{ padding:0 28px; }}
      .hero-copy {{ padding-top:0; }}
      .mentor-photo-shell {{ min-height:360px; }}
      .video-shell {{ inset:22px; }}
      .video-embed,
      .video-link-cover {{ min-height:340px; }}
      .video-meta {{ grid-template-columns:1fr; }}
      .route-map::before,
      .route-step::before {{
        display:none;
      }}
      .route-step:nth-child(odd),
      .route-step:nth-child(even) {{
        margin-left:0;
        margin-right:0;
      }}
    }}
    @media (max-width: 720px) {{
      .band {{ padding:0 18px; }}
      .hero h1 {{ font-size:64px; }}
      .section h2 {{ font-size:42px; }}
      .problem-lead h3, .closing h3 {{ font-size:34px; }}
      .hero-manifesto {{ font-size:18px; padding-left:16px; max-width:100%; }}
      .video-embed,
      .video-link-cover {{ min-height:240px; padding:22px; }}
      .video-link-copy h4 {{ font-size:15px; }}
      .faq-item summary {{ font-size:19px; padding-right:40px; }}
    }}
  </style>
  <main class="page">
    <section class="band">
      <div class="band-inner hero">
        <div class="hero-copy">
        <div class="eyebrow">高阶课程</div>
        <h1>{esc(page["title"])}</h1>
        <p class="hero-tagline">{esc(page["hero_tagline"])}</p>
        <p class="hero-manifesto">大资金长期年化20%收益是如何做到的？</p>
        {hero_notice_html}
        </div>
        <div class="hero-visual">
        <div class="video-shell">
          <div class="video-screen">
            <div class="video-embed">
              <a class="video-link-cover" {video_cover_style} href="https://www.bilibili.com/video/BV1tqjzzEExG/?p=1" target="_blank" rel="noopener noreferrer" aria-label="打开期权重盾班介绍视频">
                <span class="video-link-play">▶</span>
                <span class="video-link-copy">
                  <span class="video-mini">卖方介绍视频</span>
                </span>
              </a>
            </div>
          </div>
          <div class="video-meta">
            <div class="video-card">
              <span class="label">方法核心</span>
              <span class="value">先盾后矛</span>
              <span class="sub">先防守，再进攻</span>
            </div>
            <div class="video-card">
              <span class="label">执行原则</span>
              <span class="value">规则大于情绪</span>
              <span class="sub">用规则替代情绪，用框架替代冲动</span>
            </div>
          </div>
        </div>
        </div>
        <a class="hero-scroll-cue" href="#modules" aria-label="继续向下查看课程内容">
          <span>SCROLL</span>
          <i>↓</i>
        </a>
      </div>
    </section>

    <section class="band section">
      <div class="band-inner">
      <div class="section-head">
        <div class="section-kicker">认知重塑</div>
        <h2>别再以为卖方就是很危险”</h2>
        <p class="section-intro">重盾班想做一件事：把期权从“危险投机”重塑成有纪律、有策略、有逻辑的交易方法。建立机构化的风险经营思维。</p>
      </div>
      <div class="problem-grid">
        <article class="problem-lead">
          <div>
            <div class="card-tag"></div>
            <h3>你以为卖方在赚权利金，机构其实在经营风险。</h3>
            <p>当你开始用保险公司的视角理解卖方，你会更清楚：哪些权利金该收，哪些行情根本不该碰，为什么很多“高胜率卖方”最后还是会爆掉。</p>
          </div>
          {problem_image_html}
        </article>
        <article class="problem-list">
          <ol>{pains_html}</ol>
          {problem_side_image_html}
        </article>
      </div>
      </div>
    </section>

    <section class="band section">
      <div class="band-inner">
      <div class="section-head">
        <div class="section-kicker">课程定位</div>
        <h2>为什么这门课叫“重盾班”</h2>
        <p class="section-intro">因为真正长期活下来的卖方，不是最激进的人，而是最会先立防线的人。</p>
      </div>
      <div class="audience-grid">{audiences_html}</div>
      </div>
    </section>

    <section class="band section">
      <div class="band-inner">
      <div class="section-head">
        <div class="section-kicker">讲师介绍</div>
        <h2>业内顶级操盘手主讲</h2>
        <p class="section-intro"></p>
      </div>
      <div class="mentor-stack">{instructors_html}</div>
      </div>
    </section>

    <section class="band section" id="modules">
      <div class="band-inner">
      <div class="section-head">
        <div class="section-kicker">课程大纲</div>
        <h2>实战内容，打造卖方盾牌</h2>
        <p class="section-intro">六大学习模块，手把手带你从期权新手变老司机。</p>
      </div>
      <div class="route-map">{modules_html}</div>
      </div>
    </section>

    <section class="band section">
      <div class="band-inner">
      <div class="section-head">
        <div class="section-kicker">适合谁学</div>
        <h2>这门课适合谁，也不适合谁</h2>
        <p class="section-intro">这不是“轻松赚钱”的许诺，而是一门更适合认真交易者的卖方课程。</p>
      </div>
      <div class="who-grid">
        <article class="who-card fit">
          <div class="card-tag">适合人群</div>
          <h3>适合这类交易者</h3>
          <ul class="who-list">{fit_html}</ul>
        </article>
        <article class="who-card unfit">
          <div class="card-tag">不适合人群</div>
          <h3>不适合这类期待</h3>
          <ul class="who-list">{not_fit_html}</ul>
        </article>
      </div>
      </div>
    </section>

    <section class="band section">
      <div class="band-inner">
      <div class="section-head">
        <div class="section-kicker">学习结果</div>
        <h2>你最后带走的，不只是理论</h2>
        <p class="section-intro">学完重盾班，能掌握一套带进实盘里的卖方工作流，让你知道每一步该如何判断、取舍与执行。</p>
      </div>
      <div class="outcome-grid">{outcomes_html}</div>
      </div>
    </section>

    <section class="band section">
      <div class="band-inner">
      <div class="section-head">
        <div class="section-kicker">Q&A</div>
        <h2>常见问题解答</h2>
        <p class="section-intro"></p>
      </div>
      <div class="faq-list">{faqs_html}</div>
      </div>
    </section>

    <section class="band section" id="consult">
      <div class="band-inner">
      <div class="section-head">
        <div class="section-kicker">往期学员评价</div>
        <h2>看看真实反馈</h2>
        <p class="section-intro">往期学员的真实评价截图，广受好评，也帮助许多学员长期获利。</p>
      </div>
      <div class="review-wall">{reviews_html}</div>
      {activities_section_html}
      <div class="consult-shell">
      {promo_html}
      <div class="consult-grid">
        <article class="consult-main">
          <h3>参加期权重盾班</h3>
          <p>线上课程，咨询报名或其他细节，欢迎直接添加客服。</p>
          <div class="contact-row">
            <div class="contact-pill">微信<strong>{esc(page["wechat_id"])}</strong></div>
            <div class="contact-pill">电话<strong>{esc(page["phone"])}</strong></div>
            <div class="contact-pill">地址<strong>{esc(page["address"])}</strong></div>
          </div>
        </article>
        <article class="consult-side">
          <div class="consult-qr-grid">
            <div class="consult-qr-item">
              <img src="{esc(page["wechat_qr"])}" alt="客服二维码1">
            </div>
            <div class="consult-qr-item">
              <img src="{esc(page["wechat_qr_secondary"])}" alt="客服二维码2">
            </div>
          </div>
          <p class="qr-note">扫码添加客服微信<br>添加时备注“期权重盾班”会更快对接。</p>
        </article>
      </div>
      </div>
      </div>
    </section>

    <section class="band">
      <div class="band-inner closing">
      <div>
        <div class="section-kicker">最后一步</div>
        <h3>别再把卖方当运气，开始用机构的方式理解期权</h3>
        <p>真正值得学的，不只是“卖什么”，而是“为什么卖、何时卖、怎么活着卖下去”。如果你也想把卖方从模糊印象变成可执行流程，现在就去咨询课程。</p>
      </div>
      </div>
    </section>
  </main>
"""


with st.sidebar:
    show_navigation()
inject_sidebar_toggle_style(mode="high_contrast")

st.markdown(
    """
    <style>
    .stApp {
        background:
            radial-gradient(1200px 720px at 82% -10%, rgba(103, 212, 255, 0.12), transparent 56%),
            radial-gradient(900px 520px at 8% 0%, rgba(240, 179, 93, 0.10), transparent 48%),
            linear-gradient(160deg, #050913, #091323 58%, #101a2e) !important;
    }

    [data-testid="stHeader"] {
        background: transparent !important;
    }

    [data-testid="stDecoration"] {
        display: none;
    }

    [data-testid="stMainBlockContainer"] {
        max-width: 100% !important;
        padding: 0 !important;
    }

    .stHtml {
        width: 100% !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

page_markup = build_page_html()
if hasattr(st, "html"):
    st.html(page_markup)
else:
    st.markdown(page_markup, unsafe_allow_html=True)
