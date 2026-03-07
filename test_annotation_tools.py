"""快速测试 chart_annotation_tools 的两个工具"""
from chart_annotation_tools import draw_pattern_annotation_chart, draw_forecast_chart

print("=" * 50)
print("Tool A: draw_pattern_annotation_chart")
print("=" * 50)
result = draw_pattern_annotation_chart.invoke({'query': '豆粕'})
print(result)

print()
print("=" * 50)
print("Tool B: draw_forecast_chart")
print("=" * 50)
result = draw_forecast_chart.invoke({
    'query': '螺纹钢',
    'support': 3700,
    'resistance': 3900,
    'note': '测试注释'
})
print(result)
