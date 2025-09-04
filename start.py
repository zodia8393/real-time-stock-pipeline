import os

# 프로젝트 구조 생성
directories = [
    "src/kafka",
    "src/database", 
    "src/api",
    "src/monitoring",
    "src/utils",
    "config"
]

for directory in directories:
    os.makedirs(directory, exist_ok=True)
    # __init__.py 파일 생성
    with open(f"{directory}/__init__.py", "w") as f:
        f.write("")

print("✅ 프로젝트 구조 생성 완료")
