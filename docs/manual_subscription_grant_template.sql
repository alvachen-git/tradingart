-- 手工开通模板（请先替换变量）
-- 必填: user_id / channel_code / days / source_note / operator

-- 1) 查询频道 ID
SELECT id, code, name
FROM content_channels
WHERE code = '{{channel_code}}' AND is_active = 1;

-- 2) 执行开通（沿用统一来源字段）
-- 若已有订阅记录，建议优先使用 scripts/manual_grant_subscription.py，避免手写续期逻辑出错。
INSERT INTO user_subscriptions
(user_id, channel_id, is_active, expire_at, source_type, source_ref, source_note, granted_at, operator)
VALUES
(
  '{{user_id}}',
  {{channel_id}},
  1,
  DATE_ADD(NOW(), INTERVAL {{days}} DAY),
  'manual',
  CONCAT('manual:{{operator}}:', DATE_FORMAT(NOW(), '%Y%m%d%H%i%s')),
  '{{source_note}}',
  NOW(),
  '{{operator}}'
)
ON DUPLICATE KEY UPDATE
  is_active = 1,
  expire_at = CASE
      WHEN expire_at IS NOT NULL AND expire_at > NOW()
      THEN DATE_ADD(expire_at, INTERVAL {{days}} DAY)
      ELSE DATE_ADD(NOW(), INTERVAL {{days}} DAY)
  END,
  source_type = 'manual',
  source_ref = CONCAT('manual:{{operator}}:', DATE_FORMAT(NOW(), '%Y%m%d%H%i%s')),
  source_note = '{{source_note}}',
  operator = '{{operator}}',
  granted_at = COALESCE(granted_at, NOW()),
  updated_at = NOW();
