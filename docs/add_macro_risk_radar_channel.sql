-- Add/upgrade channel: macro_risk_radar
-- Target price: 500 points / month

-- MySQL
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
    'macro_risk_radar',
    '宏观周报',
    '🌍',
    '每周聚焦宏观主线，结合收益率、通胀、就业与联储流动性做跨资产解读。',
    1,
    1,
    COALESCE(MAX(sort_order), 5) + 1,
    NULL,
    500
FROM content_channels
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    icon = VALUES(icon),
    description = VALUES(description),
    is_active = 1,
    is_premium = 1,
    price_points_monthly = 500;
