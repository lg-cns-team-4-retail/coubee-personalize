# 🚏주요 기능
- **Extract**: 데이터베이스에서 `User_interaction 데이터`와 `User데이터`를 Extract할 수 있습니다.
- **Transfer**: **Extract**한 데이터를 **Personalize스키마** 형식에 맞게 변환할 수 있습니다.
- **Load**: 추출한 데이터셋을 **CSV형식**으로 **S3버킷**에 저장할 수 있습니다.
- **DatasetImport**: Personalize훈련을 위한 데이터셋을 S3버킷에서 추출할 수 있습니다.
- **SolutionCreate**: 생성된 데이터셋으로 개인화 딥러닝 솔루션을 생성할 수 있습니다.
- **BatchInference**: 생성된 Solution과 추출한 `UserId`로 배치 추론을 생성할 수 있습니다.
- **배치 추론된 아이템 ETL**: 배치 추론된 아이템을 추출(Extract)하여 변환(Transfer)해서 저장(Load)할 수 있습니다.

# ETL 아키텍처 
<img width="1371" height="741" alt="personalize다이어그램-페이지-13 drawio" src="https://github.com/user-attachments/assets/16ff90eb-e3be-4e02-8673-4da499d543db" />

# 🛠️기술 스택
| 구분 | 기술 | 설명 |
| :-:  | :-: |
| Core | `Python 3.11` | 파이썬과 AWS Boto SDK를 기반으로 AWS Personalize접근성 강화 |
| 개발 도구 | Visual Studio Code (무료) |

# 🚀 배포 패키지 생성 (AWS Lambda)
이 프로젝트를 AWS Lambda에 배포하려면 아래의 두 가지 압축 파일(zip)을 준비해야 합니다.

python_layer.zip: 의존성 패키지 (Lambda Layer용)

scripts/{files}.zip: 실행 코드 (Lambda 함수용)

## 1. 파이썬 의존성 패키지 준비 (python.zip)
Lambda Layer로 사용할 파이썬 라이브러리들을 준비합니다. 아래 명령어는 Lambda 실행 환경(manylinux2014_x86_64)과 Python 3.11 버전에 맞는 바이너리 패키지들을 다운로드하여 ./python 폴더에 설치합니다.

### 1. 패키지 설치

아래 bash 명령어를 터미널에서 실행하세요.
```
pip install -r requirements.txt --platform manylinux2014_x86_64 --implementation cp --python-version 3.11 --only-binary=:all: --target ./python
```
참고: 위 명령어를 실행하면 현재 디렉토리에 python 폴더가 생성되고, requirements.txt에 명시된 모든 패키지가 해당 폴더 내에 설치됩니다.

### 2. ZIP 파일로 압축
- 생성된 python 폴더에 common 디렉토리를 넣어줍니다.
- 설치가 완료된 python 폴더를 python.zip 파일로 압축합니다.

## 2. 실행 스크립트 준비 (scripts.zip)
Lambda 함수가 실행할 메인 코드인 각 import 파일들을 zip파일로 압축 합니다.

## 3. 최종 결과물
위 과정을 모두 마치면 프로젝트 루트 디렉토리에 다음과 같이 두 개의 배포용 zip 파일이 생성됩니다.
```
python.zip
scripts/{files}.zip
```
위 파일들을 사용하여 AWS Lambda 함수와 Layer를 생성하거나 업데이트할 수 있습니다.
