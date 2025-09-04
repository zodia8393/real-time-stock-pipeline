# Real-Time Stock Pipeline

## 📝 프로젝트 개요
실시간 한국 주식 데이터 수집, 저장, 시각화 및 활용을 위한 엔드투엔드 데이터 파이프라인입니다.  
네이버 금융의 실시간 주가/거래량 정보를 자동 크롤링하여 Kafka로 스트리밍,  
Consumer가 PostgreSQL·Redis·InfluxDB에 동시 저장한 뒤 Grafana에서 실시간으로 시각화합니다.

***

## 📈 주요 기능
- **실시간 수집**: 네이버 증권에서 1분마다 주가/거래량 자동 크롤링
- **고성능 스트리밍**: Kafka 기반 데이터 전송, 확장형 메시지 파이프라인
- **분산 저장**:
  - **PostgreSQL**: 관계형 DB에 종합 정보 저장
  - **Redis**: 실시간 캐시, 빠른 조회·알림
  - **InfluxDB**: 시계열 데이터, 실시간 변화 추이 분석
- **시각화**: Grafana에서 실시간/과거 주가·거래량 트렌드 대시보드
- **자동화/운영**: Docker Compose 기반, 장애/재시도/로그관리 완비

***

## 🛠️ 기술스택
- **언어/프레임워크**: Python, BeautifulSoup, kafka-python-ng
- **서버/데이터베이스**: Kafka, PostgreSQL, Redis, InfluxDB
- **시각화·모니터링**: Grafana, Prometheus
- **인프라**: Docker, Docker Compose

***

## ⚡ 프로젝트 구조
```
네이버 크롤러(Producer) → Kafka → Consumer → PostgreSQL, Redis, InfluxDB → Grafana
```

***

## 📋 설치 및 실행 방법

### 1. 준비
- Docker, Docker Compose 설치
- InfluxDB, PostgreSQL, Redis, Grafana 토큰/비번 준비 (.env에서 설정)
- 모든 코드/설정/데이터는 한 디렉토리에서 관리

### 2. 실행
```bash
# 환경변수(.env) 설정
# 조직, 버킷, 토큰 등 명확히 입력

docker compose up -d --build
```
- 초기 실행 이후 로그/대시보드(Grafana)에서 실시간 데이터 상태 확인

### 3. Grafana (시각화)
- [http://localhost:3000](http://localhost:3000) 접속
- InfluxDB 데이터 소스(Organization, Bucket, Token) 연결
- 주가/거래량 등 다양한 실시간 패널 및 대시보드 추가

***

<img width="1916" height="1033" alt="image" src="https://github.com/user-attachments/assets/cf383cb3-948b-4613-b186-1de3563b95ee" />
<img width="1909" height="905" alt="image" src="https://github.com/user-attachments/assets/6d862db9-1b22-4550-b7e8-6a37ead8db41" />

***

## 🔖 기여/라이선스

- **Issues, PR 적극 환영**
- 코드 및 개선 제안은 [GitHub Issues](https://github.com/your-username/real-time-stock-pipeline/issues)에서 요청해주세요.
- 본 프로젝트는 MIT 라이선스입니다.

***

## 📞 Contact & Author

- Email: chohj_1019@naver.com / chash8393@gmail.com
- GitHub: [https://github.com/zodia8393](https://github.com/zodia8393)
- Location: Cheonan, Korea

***

## 💡 참고사항·유의점

- 대량/빈번 크롤링은 네이버 정책에 맞게 간격조절 필수
- InfluxDB, Grafana 등 토큰·설정 정확히 적용
- Docker Compose 환경에서 모든 서비스 자동 실행 (Linux/Mac/WSL 추천)

***

## ⭐️ 프로젝트 가치

“실시간 데이터 수집–분석–시각화까지  
엔드투엔드 현대 데이터 서비스의 모든 흐름을  
직접 개발 · 운영 · 배포한 실제 사례입니다.”

***
