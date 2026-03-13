<template>
  <scroll-view class="page" scroll-y>
    <view class="wrap">
      <text class="title">爱波塔隐私政策</text>
      <text class="date">生效日期：2026-03-13</text>

      <text class="section-title">1. 收集的信息</text>
      <text class="section-text">
        为实现登录和服务安全，我们会收集并处理：账号标识、登录凭证（token）、设备基础信息和操作日志。我们不会在小程序端收集与服务无关的个人敏感信息。
      </text>

      <text class="section-title">2. 信息使用目的</text>
      <text class="section-text">
        我们仅将信息用于身份验证、会话保持、服务稳定性分析与安全防护。不会将你的个人信息出售或无关共享给第三方。
      </text>

      <text class="section-title">3. 信息存储与保护</text>
      <text class="section-text">
        登录态信息会在本地安全存储并通过 HTTPS 传输。我们采取访问控制和日志审计等措施保护数据安全。
      </text>

      <text class="section-title">4. 信息共享与披露</text>
      <text class="section-text">
        除法律法规要求或监管机关要求外，我们不会向外部披露你的个人信息。若发生合并、重组等场景，将依法保障你的合法权益。
      </text>

      <text class="section-title">5. 你的权利</text>
      <text class="section-text">
        你可通过退出登录停止后续数据处理。若你希望查询、更正或删除相关账号信息，可通过平台公告的联系方式与我们联系。
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
