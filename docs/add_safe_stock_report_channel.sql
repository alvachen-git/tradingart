-- Add/upgrade channel: safe_stock_report
-- Target price: 800 points / month

INSERT INTO content_channels (
    code,
    name,
    icon,
    description,
    is_active,
    is_premium,
    sort_order,
    price_monthly,
    price_points_monthly
)
SELECT
    'safe_stock_report',
    '小爱选股晚报',
    '📉',
    '跟踪资金回流与底部转折机会，筛选可买、观察和已买跟踪标的。',
    1,
    1,
    0,
    NULL,
    800
FROM content_channels
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    icon = VALUES(icon),
    description = VALUES(description),
    is_active = 1,
    is_premium = 1,
    sort_order = VALUES(sort_order),
    price_points_monthly = 800;
