-- 点数付费系统 v1.1 数据库迁移
-- 执行前请先备份数据库

CREATE TABLE IF NOT EXISTS user_points (
    user_id       VARCHAR(50) NOT NULL,
    balance       INT         NOT NULL DEFAULT 0,
    total_earned  INT         NOT NULL DEFAULT 0,
    total_spent   INT         NOT NULL DEFAULT 0,
    updated_at    DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id),
    CHECK (balance >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户点数账户';


CREATE TABLE IF NOT EXISTS points_transactions (
    id            BIGINT       NOT NULL AUTO_INCREMENT,
    user_id       VARCHAR(50)  NOT NULL,
    type          ENUM('topup','spend','refund','admin_grant') NOT NULL,
    amount        INT          NOT NULL,
    balance_after INT          NOT NULL,
    ref_id        VARCHAR(100) DEFAULT NULL,
    description   VARCHAR(255) DEFAULT NULL,
    biz_id        VARCHAR(100) DEFAULT NULL COMMENT '业务幂等键',
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_user_id (user_id),
    INDEX idx_created_at (created_at),
    UNIQUE KEY uq_points_txn_user_type_biz (user_id, type, biz_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='点数流水记录';


CREATE TABLE IF NOT EXISTS points_orders (
    id                 VARCHAR(64)    NOT NULL COMMENT '我方订单号',
    user_id            VARCHAR(50)    NOT NULL,
    package_name       VARCHAR(100)   NOT NULL,
    points_amount      INT            NOT NULL,
    rmb_amount         DECIMAL(10,2)  NOT NULL,
    paid_rmb_amount    DECIMAL(10,2)  DEFAULT NULL COMMENT '回调实付金额',
    alipay_trade_no    VARCHAR(64)    DEFAULT NULL,
    notify_payload_hash VARCHAR(64)   DEFAULT NULL COMMENT '回调payload哈希',
    notified_at        DATETIME       DEFAULT NULL,
    status             ENUM('pending','paid','failed','refunded') NOT NULL DEFAULT 'pending',
    paid_at            DATETIME       DEFAULT NULL,
    created_at         DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_points_orders_alipay_trade_no (alipay_trade_no),
    INDEX idx_user_id (user_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='支付宝充值订单';


ALTER TABLE content_channels
    ADD COLUMN IF NOT EXISTS price_points_monthly INT DEFAULT NULL
    COMMENT '月订阅点数价格（NULL表示不支持点数购买）'
    AFTER price_monthly;


UPDATE content_channels
SET price_points_monthly = 500
WHERE code IN (
    'daily_review',
    'evening_report',
    'expiry_option_radar',
    'daily_report',
    'broker_position_report',
    'fund_flow_report'
)
  AND (price_points_monthly IS NULL OR price_points_monthly = 0);


-- ==========================================
-- 订阅权限来源追踪（user_subscriptions）
-- ==========================================
ALTER TABLE user_subscriptions
    ADD COLUMN IF NOT EXISTS source_type VARCHAR(50) DEFAULT NULL COMMENT '来源类型：points_purchase/trial/manual/legacy_migrated...';

ALTER TABLE user_subscriptions
    ADD COLUMN IF NOT EXISTS source_ref VARCHAR(100) DEFAULT NULL COMMENT '来源引用：biz_id/脚本批次/接口标识';

ALTER TABLE user_subscriptions
    ADD COLUMN IF NOT EXISTS source_note VARCHAR(255) DEFAULT NULL COMMENT '来源说明';

ALTER TABLE user_subscriptions
    ADD COLUMN IF NOT EXISTS granted_at DATETIME DEFAULT NULL COMMENT '最近一次授予时间';

ALTER TABLE user_subscriptions
    ADD COLUMN IF NOT EXISTS operator VARCHAR(100) DEFAULT NULL COMMENT '操作人/系统标识';

-- 若已存在同名索引可忽略错误；建议在迁移脚本中做存在性检查后执行
ALTER TABLE user_subscriptions
    ADD UNIQUE KEY uq_user_subscriptions_user_channel (user_id, channel_id);


-- ==========================================
-- 新用户试用幂等表（每账号每试用码仅一次）
-- ==========================================
CREATE TABLE IF NOT EXISTS user_trial_grants (
    id           BIGINT       NOT NULL AUTO_INCREMENT,
    user_id      VARCHAR(50)  NOT NULL,
    trial_code   VARCHAR(100) NOT NULL,
    channel_id   INT          NOT NULL,
    days         INT          NOT NULL,
    granted_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_note  VARCHAR(255) DEFAULT NULL,
    operator     VARCHAR(100) DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_trial_user_code (user_id, trial_code),
    INDEX idx_trial_user (user_id),
    INDEX idx_trial_channel (channel_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新注册试用发放记录（幂等）';
