"""启动手机端 Mobile API（端口 8001）"""
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "mobile_api:app",
        host="0.0.0.0",
        port=8001,
        workers=1,
        reload=False,
    )