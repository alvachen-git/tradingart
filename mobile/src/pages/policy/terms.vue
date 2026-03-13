<template>
  <scroll-view class="page" scroll-y>
    <view class="wrap">
      <text class="title">爱波塔用户服务协议</text>
      <text class="date">生效日期：2026-03-13</text>

      <text class="section-title">1. 服务说明</text>
      <text class="section-text">
        爱波塔小程序提供行情信息展示、训练工具与智能问答服务。平台内容仅用于信息展示与学习参考，不构成任何投资建议。
      </text>

      <text class="section-title">2. 账号使用</text>
      <text class="section-text">
        你应妥善保管账号和密码，不得将账号提供给他人使用。因账号保管不当导致的风险和损失，由账号持有人自行承担。
      </text>

      <text class="section-title">3. 用户行为规范</text>
      <text class="section-text">
        你不得利用本服务从事违法违规活动，不得上传、传播违法信息，不得恶意攻击系统或干扰平台正常运行。
      </text>

      <text class="section-title">4. 免责声明</text>
      <text class="section-text">
        平台展示的行情、资讯、分析结果及 AI 回答，均不构成投资、法律或财务建议。你应独立判断并自行承担决策风险。
      </text>

      <text class="section-title">5. 协议变更</text>
      <text class="section-text">
        我们可能根据业务与合规要求更新本协议。更新后将通过页面公示，继续使用服务视为你已阅读并同意更新后的协议内容。
      </text>
    </view>
  </scroll-view>
</template>

<style scoped>
.page {
  min-height: 100vh;
  background: #0b1121;
}

.wrap {
  padding: 28rpx 30rpx 44rpx;
}

.title {
  display: block;
  font-size: 38rpx;
  font-weight: 800;
  color: #f5c518;
}

.date {
  display: block;
  margin-top: 10rpx;
  margin-bottom: 26rpx;
  font-size: 23rpx;
  color: #6f7f95;
}

.section-title {
  display: block;
  margin-top: 18rpx;
  margin-bottom: 8rpx;
  font-size: 28rpx;
  font-weight: 700;
  color: #d7deea;
}

.section-text {
  display: block;
  font-size: 25rpx;
  line-height: 1.75;
  color: #9aa7ba;
}
</style>
