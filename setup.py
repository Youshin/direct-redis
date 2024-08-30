"""
direct_redis 패키지의 설정 스크립트입니다.

이 스크립트는 setuptools를 사용하여 direct_redis 패키지를 설치하고 배포하기 위한 설정을 정의합니다.
패키지 메타데이터, 의존성, 그리고 기타 설치 관련 정보를 포함하고 있습니다.
"""

import setuptools

# README.md 파일을 읽어 long_description으로 사용
with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

setuptools.setup(
    name="direct_redis",
    version="0.0.1",
    license="MIT",
    author="Youshin",
    author_email="youshin1411@gmail.com",
    description="Serialize any python datatypes and does redis actions using redis-py",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/youshin/direct-redis",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=["redis==4.6.0"],
)
