-- 邀请积分系统 v1
-- 目标：邀请人拉新注册后获得固定积分奖励（默认 300 点）

CREATE TABLE IF NOT EXISTS user_invite_codes (
    user_id       VARCHAR(100) NOT NULL,
    invite_code   VARCHAR(64)  NOT NULL,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id),
    UNIQUE KEY uq_user_invite_code (invite_code),
    KEY idx_invite_code_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户邀请码表';


CREATE TABLE IF NOT EXISTS user_invite_relations (
    id               BIGINT       NOT NULL AUTO_INCREMENT,
    inviter_user_id  VARCHAR(100) NOT NULL,
    invitee_user_id  VARCHAR(100) NOT NULL,
    invite_code      VARCHAR(64)  NOT NULL,
    reward_points    INT          NOT NULL DEFAULT 300,
    status           VARCHAR(32)  NOT NULL DEFAULT 'pending_reward',
    register_ip_hash VARCHAR(128) DEFAULT NULL,
    device_hash      VARCHAR(128) DEFAULT NULL,
    reject_reason    VARCHAR(100) DEFAULT NULL,
    created_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    rewarded_at      DATETIME     DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_invitee_user (invitee_user_id),
    KEY idx_inviter_status (inviter_user_id, status),
    KEY idx_created_at (created_at),
    KEY idx_ip_device (register_ip_hash, device_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='邀请关系与奖励状态';


CREATE TABLE IF NOT EXISTS invite_landing_events (
    id               BIGINT       NOT NULL AUTO_INCREMENT,
    invite_code      VARCHAR(64)  NOT NULL,
    inviter_user_id  VARCHAR(100) DEFAULT NULL,
    event_type       VARCHAR(32)  NOT NULL,
    session_id       VARCHAR(120) DEFAULT NULL,
    invitee_user_id  VARCHAR(100) DEFAULT NULL,
    register_ip_hash VARCHAR(128) DEFAULT NULL,
    device_hash      VARCHAR(128) DEFAULT NULL,
    extra_json       TEXT         DEFAULT NULL,
    created_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_code_type_created (invite_code, event_type, created_at),
    KEY idx_session_created (session_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='邀请落地页漏斗事件';
