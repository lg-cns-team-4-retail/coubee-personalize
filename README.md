# 🚀 배포 패키지 생성 (AWS Lambda)
이 프로젝트를 AWS Lambda에 배포하려면 아래의 두 가지 압축 파일(zip)을 준비해야 합니다.

python_layer.zip: 의존성 패키지 (Lambda Layer용)

scripts/{files}.zip: 실행 코드 (Lambda 함수용)

# 📦 1. 파이썬 의존성 패키지 준비 (python.zip)
Lambda Layer로 사용할 파이썬 라이브러리들을 준비합니다. 아래 명령어는 Lambda 실행 환경(manylinux2014_x86_64)과 Python 3.11 버전에 맞는 바이너리 패키지들을 다운로드하여 ./python 폴더에 설치합니다.

## 1. 패키지 설치

아래 bash 명령어를 터미널에서 실행하세요.
```
pip install -r requirements.txt --platform manylinux2014_x86_64 --implementation cp --python-version 3.11 --only-binary=:all: --target ./python
```
참고: 위 명령어를 실행하면 현재 디렉토리에 python 폴더가 생성되고, requirements.txt에 명시된 모든 패키지가 해당 폴더 내에 설치됩니다.

## 2. ZIP 파일로 압축
설치가 완료된 python 폴더를 python.zip 파일로 압축합니다.

# 📜 2. 실행 스크립트 준비 (scripts.zip)
Lambda 함수가 실행할 메인 코드인 scripts 폴더 전체를 scripts.zip 파일로 압축합니다.

# ✅ 최종 결과물
위 과정을 모두 마치면 프로젝트 루트 디렉토리에 다음과 같이 두 개의 배포용 zip 파일이 생성됩니다.

python_layer.zip

scripts.zip

이제 이 파일들을 사용하여 AWS Lambda 함수와 Layer를 생성하거나 업데이트할 수 있습니다.
