"""
Topic10: API 调用与 SQLite 数据库管理
"""

from setuptools import setup, find_packages

# 读取 README 作为 PyPI 长描述
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# 读取运行依赖（忽略空行和注释）
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [
        line.strip() for line in fh if line.strip() and not line.startswith("#")
    ]

setup(
    name="topic10-api-database",
    version="1.0.0",
    author="Topic10 Team",
    author_email="team@example.com",
    description="FRED API 数据获取与 SQLite 数据库管理",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/topic10_api_database",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "topic10-build=topic10_workflow:main",
            "topic10-update=update_db:main",
        ],
    },
)
