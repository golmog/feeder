## Feeder 플러그인 매뉴얼

**Feeder**는 웹 크롤링을 통한 토렌트/마그넷 수집부터 맞춤형 RSS 피드 발행, 그리고 멀티 다운로더 엔진과 대용량 클라우드(Google Drive, Rclone, CD2 등)를 아우르는 자동화 다운로드·이송 파이프라인을 제공하는 FlaskFarm(FF) 전용 통합 미디어 관리 플러그인입니다.

기존 `rss2`의 수집·피드 기능을 바탕으로 **수집(CRAWL) ➔ 피드(FEED) ➔ 다운로드(DOWNLOAD)** 모듈을 구성합니다. **Celery 비동기 작업, YAML 기반 수집기·피드·다운로드 프로필, HTTP/FlareSolverr/Remote Selenium 수집, 다운로더·이송 핸들러 확장, Google Drive 계정 풀, 실시간 SSE 대시보드**를 제공합니다.

**문서 기준:** `info.yaml`의 버전 **1.2.5**와 현재 구현. 각 모듈의 기본 설정과 스케줄은 FF 설정 DB에, 수집기·피드·엔진·프로필·계정 목록은 `feeder_settings.yaml`에 저장됩니다.

---

### 1. 주요 특징

#### 1) 수집 계층 (CRAWL)

* **선택 가능한 수집 방식:**
  * 일반 HTTP 수집은 `curl_cffi`를 먼저 사용하고, 실패하거나 사용할 수 없으면 표준 `requests` 세션으로 폴백합니다.
  * FlareSolverr 사용 시 초기 인가를 먼저 요청하고, 실패하면 HTTP 경로를 시도합니다. 차단 감지 후 프록시 전환과 FlareSolverr 재인가도 수행합니다.
  * FlareSolverr 결과는 쿠키 유무만으로 판단하지 않습니다. API의 `status`·`message`와 반환 HTML을 함께 확인해 **챌린지 미감지 / 해결 응답 / 챌린지·차단 화면 잔존 / API·통신 오류 / HTML 없음·응답 형식 오류**를 로그로 구분합니다. `Challenge not detected!`와 정상 HTML을 받았다면 쿠키가 없어도 성공이며, Selenium 사전 인가도 계속 진행합니다. 메시지가 감지 여부를 알려주지 않으면 임의로 해결 성공·실패를 단정하지 않습니다. 일반 페이지에 Turnstile 위젯 코드가 있다는 이유만으로 차단 화면으로 취급하지 않습니다. 쿠키 값과 HTML 원문은 결과 진단 로그에 출력하지 않습니다.
  * Selenium 사용 시 Remote WebDriver 경로를 사용합니다. 일반 HTTP 수집의 마지막 단계로 항상 실행되는 구조는 아닙니다.
* **프록시 로테이션:** 단일 프록시 또는 쉼표·줄바꿈으로 구분한 복수 프록시를 지원합니다. 차단·응답 실패와 게시판 전환 시 프록시를 바꿉니다. HTTP 경로의 재시도 수는 등록 프록시 수와 재시도 설정값 중 큰 값을 사용합니다.
* **페이지 기반 수집:** 기본 최대 5페이지이며 수집기별로 덮어쓸 수 있습니다. 증분 수집은 최근 수집 ID에 도달하면 조기 종료하고, **항상 최대 페이지 수집**을 켜면 기수집 게시물을 건너뛰며 설정 페이지까지 탐색합니다.
* **마그넷·ed2k·첨부파일:** 링크 추출과 `.torrent` 첨부파일의 info hash 분석을 지원합니다. 토렌트 메타데이터는 `torrent_info` 플러그인(m2i API) 또는 qBittorrent Web API로 조회합니다. ed2k 링크에는 토렌트 메타데이터 조회를 수행하지 않습니다.
* **중복 제어:** 동일 사이트·게시판의 게시물 ID를 확인하며, 마그넷 중복 허용을 끄면 다른 게시판에서 이미 수집한 해시도 제외합니다.
* **커스텀 사이트 훅 (`feeder_custom/sites`):** 세션 초기화, 페이지 로드 후 처리, 상세 링크 추출을 확장할 수 있습니다. 로그인·압축파일 분석 등 사이트별 로직은 해당 훅에서 구현합니다.

#### 2) 피드 가공 및 발행 계층 (FEED)

* **RSS 2.0 생성:** 여러 내부 게시판과 외부 RSS URL을 하나의 피드로 조합합니다. 동기화 중복 판정은 **피드 이름 + 원본 URL**을 기준으로 합니다.
* **화질·정규식 필터:** `2160p+`, `>=1080p`, `720p-1080p` 등의 화질 조건과 `regexp.reject`, `accept`, `reject_excluding`, 별도 `accept_all` 설정을 지원합니다.
* **동기화와 출력 제한 분리:** FEED DB 동기화에는 RSS API/XML의 최대 항목 수나 XML 보관 기간을 적용하지 않습니다. API 응답과 XML 파일은 각자의 설정에 따라 실제 `<item>` 수를 제한합니다.
* **공유용 XML 파일:** 기본 14일·최대 100항목을 출력하며, 저장 경로와 파일명을 지정할 수 있습니다. 파일 생성은 전역 생성 옵션과 개별 피드 옵션을 함께 켜야 합니다. UI의 개별 강제 생성은 이 옵션과 관계없이 실행합니다.
* **API Key 처리:** 정적 XML에는 API Key를 넣지 않습니다. 정적 파일을 배포할 웹 서버는 별도로 준비하며, XML 안의 FF 첨부파일 다운로드 링크에 필요한 인증은 FF API 설정에 따릅니다.

#### 3) 다운로드 및 클라우드 파이프라인 (DOWNLOAD)

* **다운로더 우선순위 체인:**
  * 내장 엔진은 **qBittorrent**와 **CloudDrive2(CD2)**입니다. **AllDebrid** 및 기타 엔진은 `feeder_custom/engines`의 확장 스크립트가 설치되어 있어야 선택할 수 있습니다.
  * 프로토콜 미지원·작업 추가 실패·재시작 불가·지연 타임아웃에는 다음 엔진을 시도합니다. 비활성 엔진이나 큐 한도 도달은 대기하고, 원격 목록 누락 유예 초과는 해당 엔진 내 실패로 기록합니다.
  * `max_active_tasks`는 AllDebrid에만 적용합니다. 원격에 이미 존재하는 작업은 새로 등록하지 않고 연동할 수 있습니다.
* **이송 핸들러:** `local`(로컬 이동), `rclone_simple`(일반 리모트 복사 후 최종 이동), `gdrive_rotation`(Google Drive 계정 풀/기본 리모트), `remote_worker`(원격 서버 복사)를 내장하며 커스텀 핸들러를 추가할 수 있습니다.
* **원격 워커:** Rclone 소스 리모트 → 목적지 리모트 복사를 별도 Linux 서버에서 실행하고 Feeder가 서버 측 폴더 이동으로 마무리합니다. 로컬 스테이징을 생략하며, 기존 업로드 큐·동시 전송 수·중단 예약·차단 정리에 연결됩니다. 작업 ID를 보존해 응답 유실이나 FF 재시작 뒤 중복 전송을 방지하고, SSE 알림과 REST 재조회로 진행률을 확인합니다. 특정 서버 제공업체에 종속되지 않습니다.
* **로컬 스테이징과 다이렉트 전송:** 원격 소스를 로컬 버퍼로 먼저 받거나 Rclone으로 목적지에 직접 전송합니다. 다이렉트 모드도 별도 원격 워커를 사용하지 않으면 FF 호스트에서 전송 프로세스가 실행됩니다.
* **DB 이력 기준 폴더명 분리:** 이전 DB 이력에 같은 이름이 있으면 파일이 사라졌더라도 `_[해시 앞 6자리]`를 붙입니다. 원본 파일명과 출력 폴더명을 별도 보존하며, 같은 이름을 이유로 다운로드를 대기시키지 않습니다.
* **Google Drive 계정 풀:** 작은 작업은 설정 임계값(기본 14GB)에 따라 MyDrive를 경유하고 큰 작업은 공유 드라이브로 직행합니다. 계정 풀이 OFF면 계정 목록 없이 기본 업로드 리모트로 공유 드라이브에 직행합니다.
* **원격 릴레이 API:** 작업 선점·완료 보고 API를 제공합니다. 릴레이용 이송 핸들러와 원격 워커는 별도로 준비해야 합니다.
* **이력 가져오기와 고아 작업 흡수:** 기존 SQLite DB·마그넷 텍스트의 완료 이력 가져오기와, 엔진에만 남아 있는 작업을 프로필에 연결하는 기능을 제공합니다.

#### 4) 실시간 대시보드 및 운영 편의성

* **SSE 대시보드:** 큐 상태, 엔진 진행률, Rclone 전송량·속도·ETA를 갱신합니다. Rclone의 `MiB`, `GiB` 등 IEC 단위도 처리합니다.
* **24시간 통계:** 최근 성공 종료 용량과 Google Drive 사용량 기록을 표시합니다. Google Drive 사용량은 전송 시도 시 기록한 파일 크기 합계로, 성공 전송 바이트만 집계한 값과 차이가 있을 수 있습니다. SSE 통계는 30초 간격으로 다시 계산합니다.
* **작업 제어:** 재시도, 수동 완료·실패, 삭제/취소를 지원합니다. **중단·차단**은 해당 콘텐츠의 전송을 즉시 중단하고 로컬 스테이징·원격 임시 업로드 파일을 정리합니다. 차단 이력은 다운로드 항목과 별도로 보관하여 같은 해시의 재등록을 막으며, 큐/리스트의 **콘텐츠 차단 목록**에서 정리 상태 확인·재시도·차단 해제가 가능합니다. 최종 보관 파일은 삭제하지 않습니다. 이송 핸들러를 찾지 못하면 `waiting_transporter`로 원본을 보존하고 설정 복구를 기다립니다.
* **재시작 복구:** 이전 실행의 스테이징·업로드·릴레이 전송 상태를 대기 상태로 되돌립니다. 필요한 DB 컬럼·인덱스는 플러그인 로드 시 추가합니다.
* **동시성 제어:** SQLite WAL·busy timeout 설정, 프로세스 내 실행 락과 DB 런타임 락을 사용합니다. 스테이징·Google Drive Rclone 전송 전에는 DB 세션을 반환합니다.
* **내장 편집기:** 사이트 JSON, YAML, 커스텀 Python 스크립트를 편집하며 JSON 정렬·창 크기 조절·전체화면을 지원합니다.

#### 5) 처음 사용하는 순서

1. **CRAWL 설정:** 사이트 규칙을 등록하고 수집기에 게시판·최대 페이지·프록시/브라우저 옵션을 지정한 뒤 사이트 테스트와 수동 수집을 실행합니다.
2. **FEED 설정:** 수집 게시판 또는 외부 RSS를 소스로 등록하고 필터를 설정합니다. 피드 동기화 후 목록과 RSS API 응답을 확인합니다.
3. **DOWNLOAD 설정:** 엔진을 등록하여 연결을 테스트하고, 프로필에 대상 피드·우선순위 체인·목적지를 지정합니다. Rclone 사용 시 FF와 Celery 워커 환경에서 실행 파일·설정 파일·원본 경로에 접근할 수 있어야 합니다.
4. **Google Drive 사용 시:** 공유 드라이브 ID와 경로를 설정합니다. 계정 풀 사용 시 계정별 리모트 또는 Impersonate 설정과 계정 목록을 준비합니다. 14GB·700GB·3TB는 플러그인의 기본 설정값이며 실제 서비스의 저장 공간·권한·쿼터를 보장하는 값은 아닙니다.
5. 각 모듈의 **자동 & DB정리**에서 실행 주기와 자동 시작을 설정합니다. 기본 주기는 CRAWL/FEED 10분, DOWNLOAD 5분이며 FF 스케줄러의 Interval/Cron 형식을 사용합니다.

#### 6) 처리 개수 제한의 범위

| 계층 | 설정 | 적용 범위 |
| :--- | :--- | :--- |
| CRAWL | 최대 수집 페이지 수 (`crawl_max_page`, 수집기의 `max_page`) | 게시판별 최대 탐색 페이지. 일반 수집에는 별도의 게시물 개수 상한을 적용하지 않습니다. |
| FEED API | 실시간 RSS 피드 최대 수 (`feed_count` / `feed_feed_count`) | RSS API가 반환하는 `<item>` 수. 기본 100개. |
| FEED XML | 피드 보관 최대 항목 수 (`rss_file_items` / `feed_rss_file_items`) | XML 파일에 넣는 `<item>` 수. 기본 100개. API 제한과 독립적입니다. |
| DOWNLOAD | Google Drive 업로드 최대 큐 (`download_max_staging_items`) | 스테이징 중·직접 업로드를 포함한 업로드 대기/진행·로컬 버퍼 잔여 작업을 ID 기준으로 중복 없이 합산합니다. 기본 10개, 0은 무제한. |
| AllDebrid | `max_active_tasks` | 해당 AllDebrid 엔진의 진행 중 다운로드 큐. 0은 무제한. |

<br>
테스트 수집 개수(기본 3개·목록 1페이지), 화면 페이지 크기, 병렬 워커 수는 각각 테스트·표시·동시 실행 범위입니다. CD2의 `cd2_max_pages`는 상태 목록 조회 범위를 제한하는 별도 보호 설정입니다. FEED XML 보관 기간, 다운로드 대상 기간(`sync_hours`), DB 자동 정리는 개수 제한과 별도로 적용합니다.

---

### 2. 메뉴 및 UI 구성

Feeder는 기능별로 3대 모듈 및 하위 탭으로 명확하게 구분되어 있습니다.

```text
FEEDER
├── CRAWL (수집)
│   ├── 설정 및 관리 (기본 설정, 사이트 관리, 수집기 관리, 자동 & DB정리)
│   └── 수집 리스트 (게시글 탐색, 마그넷/ed2k 복사, 메타데이터 조회, 직접 다운로드 큐 등록)
├── FEED (피드)
│   ├── 설정 및 관리 (기본 설정, 피드 관리, 자동 & DB정리)
│   └── 피드 리스트 (피드별 수집 결과 조회, 마그넷 복사, 직접 다운로드 큐 등록)
└── DOWNLOAD (다운로드)
    ├── 설정 및 관리 (기본 설정, 다운로더 엔진 관리, 다운로드 라우팅 프로필, Google Drive 계정 풀, 자동 & DB정리)
    ├── 다운로드 큐 및 대시보드 (실시간 SSE 게이지 바, 속도 배지, 작업 제어, 고아 작업 흡수)
    └── 다운로드 리스트 (전체 상태 및 완료/실패 이력 검색, 재시도, 완료·실패·삭제 처리)
```

| 모듈 | 페이지/탭 | 주요 기능 |
| :--- | :--- | :--- |
| **CRAWL** | **수집 리스트** | 수집된 게시글 목록 확인, 마그넷/ed2k 복사, 첨부파일 직접 다운로드, 토렌트 메타데이터 조회, 리스트에서 원하는 프로필/엔진을 지정하여 즉시 다운로드 큐 등록. |
| | **설정 및 관리** | 크롤링 주기, 최대 페이지, 프록시, FlareSolverr, Selenium, 토렌트 정보 취득 설정, 사이트 JSON 규칙 관리, 수집기(Crawler)별 게시판 및 스케줄 빈도 설정. |
| **FEED** | **피드 리스트** | 생성된 피드별 발행 결과 조회, 마그넷 복사, 다운로드 큐 직접 추가. |
| | **설정 및 관리** | 전역 화질/정규식 필터, 피드(Feed)별 소스(내부 수집기+외부 RSS) 조합, Flexget 화질 및 정규식 필터, 무인증 공유용 RSS XML 파일 생성 및 보관 기간 설정. |
| **DOWNLOAD** | **다운로드 큐 및 대시보드** | 활성 작업 실시간 모니터링(대기, 엔진 다운로드, 로컬 스테이징, GDrive 업로드), 실시간 속도/진행률/24시간 업로드 배지, 작업 강제완료/재시도/삭제, 고아 작업 큐 흡수. |
| | **다운로드 리스트** | 이송 핸들러 복구 대기·이동 실패를 포함한 상태 검색, 프로필 변경 재시도, 수동 완료·실패·삭제. |
| | **설정 및 관리** | 다운로더 엔진(`BaseDownloadEngine`) 등록, 다운로드 라우팅 프로필(`BaseTransporter`) 구성, Google Drive SA 계정 풀 등록 및 24시간 통계 확인, 과거 이력 마이그레이션. |

---

### 3. 사이트 수집 규칙 (JSON) 작성 가이드

사이트 관리 메뉴의 **사이트 직접 추가** 또는 **규칙 수정**에서 사용하는 사이트별 크롤링 설정 스키마입니다.

<br>
#### 전체 JSON 구조 예시

```json
{
  "NAME": "example_site",
  "TORRENT_SITE_URL": "https://example.com",
  "DELAY": 1.5,
  "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
  "COOKIE": "member_key=abcdef123456; safe_mode=off",
  "BOARD_URL_RULE": "{URL}/bbs/board.php?bo_table={BOARD_NAME}&page={PAGE}",
  "SUBCAT_URL_RULE": "{URL}/forum.php?mod=forumdisplay&fid={BOARD_NAME}&filter=typeid&typeid={SUBCAT}&page={PAGE}",
  "XPATH_LIST_TAG": {
    "XPATH": "//tbody/tr[%s]//td[2]//a",
    "INDEX_START": 1,
    "INDEX_STEP": 1,
    "TITLE_REGEX": "(?P<title>.*)"
  },
  "BOARD_LIST": {
    "special_board": "SPECIAL_XPATH_TAG"
  },
  "SPECIAL_XPATH_TAG": {
    "XPATH": "//ul[@class='post-list']/li[%s]/a",
    "INDEX_START": 1,
    "INDEX_STEP": 1
  },
  "ID_REGEX": [
    "wr_id=(?P<id>\\d+)",
    "thread-(?P<id>\\d+)-"
  ],
  "SELENIUM_WAIT_TAG": "//tbody/tr[1]",
  "SELENIUM_DETAIL_WAIT_TAG": "//div[@class='view-content']",
  "SELENIUM_TIMEOUT": 25,
  "MAGNET_REGEX": [
    "magnet:\\?xt=urn:btih:([a-zA-Z0-9]+)",
    "magnet:?xt=urn:btih:%s"
  ],
  "DOWNLOAD_REGEX": "href=\"(?P<url>[^\"]+download\\.php[^\"]+)\".*?>(?P<filename>[^<]+\\.torrent)<",
  "EXTRA": [
    "USE_PROXY",
    "USE_FLARESOLVERR",
    "USE_SELENIUM",
    "USE_TORRENT_INFO",
    "USING_POST_CHAR_ID",
    "ONLY_FILE",
    "MAGNET_ONLY_ONE_LAST"
  ],
  "DESCRIPTION": [
    "사이트 안내 및 게시판 ID 입력 안내:",
    "- 일반 게시판: movie_kor, ent, 103",
    "- 서브 카테고리 수집 시: fid:typeid (예: 166:875)"
  ]
}
```

#### JSON 최상위 필드 설명

| 필드명 | 필수 여부 | 기본값 | 상세 설명 |
| :--- | :---: | :---: | :--- |
| `NAME` | **필수** | - | 사이트 식별명. 커스텀 스크립트 훅(`site_{NAME}.py`)과의 매핑 키로 사용됩니다. |
| `TORRENT_SITE_URL` | **필수** | - | 대상 사이트 도메인 주소 (끝에 `/` 제외). |
| `DELAY` | 선택 | `2.0` | 요청 간 대기 시간(초). 수집기 `delay` → 사이트 `DELAY` → CRAWL 기본 딜레이 순으로 적용합니다. |
| `USER_AGENT` | 선택 | 기본 크롬 UA | 사이트 요청에 사용할 User-Agent. 우회 인가에서 받은 User-Agent가 있으면 해당 값이 우선할 수 있습니다. |
| `COOKIE` | 선택 | - | 고정 세션 쿠키 문자열 (`name=val; name2=val2`). |
| `BOARD_URL_RULE` | 선택 | 그누보드 기본 | 기본 게시판 목록 URL 템플릿. `{URL}`, `{BOARD_NAME}`, `{PAGE}` 치환자 지원. |
| `SUBCAT_URL_RULE` | 선택 | - | 분류 필터(서브 카테고리) URL 템플릿. `{URL}`, `{BOARD_NAME}`, `{FID}`, `{SUBCAT}`, `{TYPEID}`, `{PAGE}` 치환자 지원. |
| `XPATH_LIST_TAG` | **필수** | - | 목록 페이지에서 각 게시글의 링크 태그(`<a>`)를 순회 추출하기 위한 XPath 규칙 딕셔너리. |
| `ID_REGEX` | 선택 | 표준 정규식 | 상세 페이지 URL에서 고유 게시글 ID를 추출하는 정규식 (`(?P<id>...)` 그룹 필수). 문자열 또는 문자열 배열 지원. |
| `SELENIUM_WAIT_TAG` | 선택 | `body` | Selenium으로 목록 로드 시 대기할 요소의 XPath. |
| `SELENIUM_DETAIL_WAIT_TAG` | 선택 | `body` | Selenium으로 상세 페이지 로드 시 대기할 요소의 XPath. |
| `SELENIUM_TIMEOUT` | 선택 | 기본 설정값 | 해당 사이트에만 적용할 Selenium 로딩 타임아웃(초). |
| `SELENIUM_REMOTE_URL` | 선택 | CRAWL 기본 설정 | 사이트별 Remote WebDriver 주소. 미지정 시 `crawl_selenium_remote_url`을 사용합니다. |
| `BOARD_LIST` | 선택 | - | 게시판 ID를 별도의 목록 XPath 규칙 키에 연결하는 딕셔너리. |
| `MAGNET_REGEX` | 선택 | 자동 정규식 | 본문에서 마그넷 주소를 추출하기 위한 정규식 규칙 `[매칭정규식, 치환포맷]`. |
| `DOWNLOAD_REGEX` | 선택 | - | 첨부파일 다운로드 주소와 파일명을 추출하는 정규식 (`(?P<url>...)`, `(?P<filename>...)` 그룹 필수). |
| `EXTRA` | 선택 | `[]` | 사이트별 특수 플래그 배열 (`USE_PROXY`, `USE_FLARESOLVERR`, `USE_SELENIUM`, `USE_TORRENT_INFO`, `USING_POST_CHAR_ID`, `ONLY_FILE`, `MAGNET_ONLY_ONE_LAST`). |
| `DESCRIPTION` | 선택 | - | 웹 UI 사이트 목록 테이블에 표시할 안내 문구. |

<br>
`XPATH_LIST_TAG`의 `XPATH`, `INDEX_START`, `INDEX_STEP`으로 목록을 순회하고, 선택적인 `TITLE_REGEX`의 `title` 그룹으로 제목을 가공합니다. 제목 원문은 링크 태그의 텍스트에서 얻으며 `TITLE_XPATH`는 현재 사용하지 않습니다. `BOARD_LIST`는 게시판 ID를 별도 XPath 규칙 키에 연결합니다.

`USING_POST_CHAR_ID`는 숫자 ID 기준 조기 종료를 사용하지 않을 때, `ONLY_FILE`은 첨부파일 중심 게시물을 저장할 때 사용합니다. 정기 수집의 프록시·Selenium·메타데이터 사용 여부는 수집기 설정을 확인하세요. 사이트 옵션만 켜고 수집기의 해당 옵션을 끈 경우 같은 동작을 기대할 수 없습니다.

---

### 4. 통합 설정 파일 (`feeder_settings.yaml`) 가이드

플러그인 상단의 **YAML 편집** 버튼을 누르거나 `{path_data}/db/feeder_settings.yaml` 파일을 수정하여 수집기, 피드, 다운로더 엔진, 다운로드 프로필, Google Drive 계정 목록을 관리할 수 있습니다. 사이트 JSON 규칙과 각 모듈의 기본 설정·스케줄은 FF DB에 별도로 저장되므로 이 YAML에 전부 포함되지는 않습니다.

<br>
#### 전체 YAML 구조 예시

AllDebrid 부분은 설치한 확장 엔진의 스키마에 맞게 조정하세요. 아래 `use_rss_file`만으로 정기 XML 생성이 켜지지는 않으며 FEED 기본 설정의 전역 스위치도 필요합니다.

```yaml
GLOBAL:
  quality: 1080p+
  regexp:
    reject:
      - \btrailer\b: {from: title}
      - \bWEBSCR\b: {from: title}
      - \bTS\b: {from: title}
      - \bCam\b: {from: title}

CRAWLERS:
  - id: 1
    site: sukebei
    boards:
      - board: '2_2'
    interval: 1
    max_page: 5
    delay: 2.0
    max_retries: 3
    enabled: true
    use_proxy: false
    use_flaresolverr: false
    use_selenium: false
    use_torrent_info: true

  - id: 2
    site: sehuatang
    boards:
      - board: '103'
      - board: '166'
        subcat: '875'
    interval: 2
    enabled: true
    use_proxy: true
    proxy_url: 'http://gluetun:8888'
    use_flaresolverr: true
    use_selenium: true
    use_torrent_info: false

FEEDS:
  - id: 1
    name: "sukebei_4k"
    sources:
      - site: sukebei
        board: '2_2'
    quality: 2160p+
    feed_count: 100
    rss_file_items: 50
    rss_file_days: 14
    rss_file_path: /data/rss
    use_rss_file: true
    rss_file: sukebei_4k.xml

  - id: 2
    name: "asian_movies"
    sources:
      - site: sukebei
        board: '2_2'
      - site: sehuatang
        board: '103'
      - type: rss
        site: 외부RSS
        board: 'https://example.com/rss.xml'
        url: 'https://example.com/rss.xml'
    quality: 1080p+
    accept_all: false
    use_rss_file: true
    rss_file: asian_movies.xml

DOWNLOADERS:
  - name: qbit_main
    engine_type: qbittorrent
    enabled: true
    url: 'http://127.0.0.1:8080'
    username: admin
    password: mypassword
    save_path: '/data/downloads'
    category: feeder
    stalled_timeout_hours: 12
    missing_timeout_hours: 24

  - name: ad
    engine_type: alldebrid  # 별도 확장 엔진 스크립트 설치 필요
    enabled: true
    apikey: 'YOUR_ALLDEBRID_API_KEY'
    remote_name: 'ad'
    rclone_base_path: 'magnets'
    max_active_tasks: 20
    stalled_timeout_hours: 6
    missing_timeout_hours: 24

  - name: cd2
    engine_type: cd2
    enabled: true
    cd2_addr: '127.0.0.1'
    cd2_port: 19798
    cd2_token: 'YOUR_CD2_JWT_TOKEN'
    cd2_virtual_path: '115open/云下载'
    cd2_mount_path: '/mnt/cd2'
    cd2_max_pages: 1
    cd2_timeout: 30
    stalled_timeout_hours: 24
    missing_timeout_hours: 24

DOWNLOAD_PROFILES:
  - name: default_profile
    sync_hours: 72
    feeds:
      - '*'
    priority_chain:
      - ad
      - cd2
      - qbit_main
    destination:
      type: gdrive_rotation
      use_local_staging: false
      staging_path: 'default'
      upload_path: 'incoming/default'
      complete_path: 'uploads/default'
      shared_drive_id: '0ABC123456789'
    destination_by_engine:
      qbit_main:
        type: local
        complete_path: '/data/library/default'

GDRIVE_ACCOUNTS:
  - username: 'user01@example.com'
    mydrive_rclone_id: '0AIoBxxxxxx_FolderID1'
    remote_name: 'gdrive_user01'
  - username: 'user02@example.com'
    mydrive_rclone_id: '0AIoByyyyyy_FolderID2'
    remote_name: 'gdrive_user02'
```

#### 원격 워커 설정

별도 Linux 서버에서 **Rclone 소스 리모트 → 목적지 리모트** 복사를 실행합니다. 파일 데이터는 원격 서버에서 전송하며, FF는 작업 등록·상태 확인·최종 폴더 이동을 담당합니다. **Feeder Remote Worker 0.7.0 이상**, Python 3.10 이상, systemd 사용자 서비스와 Tailscale을 사용할 수 있는 서버가 필요합니다.

##### 설치 전 준비

* 설치 스크립트는 Python 3.10 이상과 가상환경·pip 생성 가능 여부를 확인합니다. 이미 설치된 Python을 그대로 사용하며, 특정 버전으로 바꾸지 않습니다. Debian/Ubuntu에서는 누락된 `python3`, venv 지원, `curl`, `ca-certificates`를 apt로 설치하며, 이때만 sudo 권한이 필요합니다. Python이 없으면 배포판 기본 `python3`·`python3-venv`를 설치하고, Python이 이미 있으면 그 버전에 맞는 venv 패키지(예: `python3.13-venv`)를 준비합니다. 기존 도구와 워커 가상환경은 보존합니다. **시스템 pip는 별도로 설치하지 않습니다.** 워커 전용 가상환경에 Flask·Waitress·requests를 설치하며, 기존 가상환경에 pip만 없으면 ensurepip로 보완합니다. 다른 배포판은 필요한 도구를 미리 준비하세요. 현재 `python3`가 3.10 미만이거나 별도 설치한 Python에 맞는 venv 패키지가 배포판에 없으면 안내에 따라 해당 Python 환경을 먼저 준비해야 합니다.
* FF와 워커를 같은 Tailscale 네트워크에 연결하고, 워커 서버의 Rclone에 실제 전송할 소스·목적지 리모트를 설정합니다. 소스 리모트는 Feeder 다운로드 엔진이 넘기는 원본 경로에 접근할 수 있어야 합니다. 기존 wiserain MOD를 사용 중이라면 그대로 유지합니다. 설치 프로그램은 Rclone이나 리모트 인증을 설치·교체하지 않습니다.
* **현재 지원 범위:** 소스는 Rclone의 `webdav` 타입, 목적지는 `drive` 타입입니다. 설치 프로그램과 작업 API가 이 종류를 검증하므로 다른 타입의 리모트는 아직 사용할 수 없습니다.
* FF에서도 최종 폴더 이동을 위해 같은 이름의 Drive 리모트가 필요합니다. 공유 드라이브와 `backend chpar`를 지원하는 Rclone 구성을 준비합니다.
* TCP 8000을 공용 인터넷에 개방할 필요가 없습니다. Tailscale 정책에서 FF → 워커 접속을 허용합니다.

##### 한 줄 설치

**워커 서버의 일반 사용자 계정**으로 실행합니다. 기본 설치 위치는 `$HOME/feeder-worker`입니다.

아래 `curl | bash` 명령은 스크립트를 받기 위한 curl이 먼저 있어야 합니다. curl이 없고 wget이 있다면 `wget -qO- https://raw.githubusercontent.com/golmog/feeder/main/worker/install.sh | bash`로 시작할 수 있으며, 이후 필요한 curl은 스크립트가 준비합니다.

```bash
curl -fsSL https://raw.githubusercontent.com/golmog/feeder/main/worker/install.sh | bash
```

설치 프로그램은 Tailscale IPv4와 Rclone 설정(`$HOME/.config/rclone/rclone.conf`)을 확인합니다. 소스·목적지 리모트는 Feeder가 작업별로 전달하므로 설치 시 고정 목록을 지정하지 않습니다. 자동 확인이 어려운 환경은 옵션을 지정하세요. 아래 주소·경로는 예시입니다.

```bash
curl -fsSL https://raw.githubusercontent.com/golmog/feeder/main/worker/install.sh | bash -s -- \
  --bind 100.64.0.10 --port 8000 \
  --rclone-config "$HOME/.config/rclone/rclone.conf" \
  --rclone-binary /usr/bin/rclone
```

`--install-dir`로 설치 위치를 변경할 수 있습니다. 기존 `.env`, `config/`, `venv/`, 작업 DB와 Rclone 설정은 보존합니다. 기존 `.env`가 있으면 새 옵션으로 덮어쓰지 않으므로, 설정 변경은 서비스를 중지한 뒤 해당 파일에서 수행하세요. 설치 실패 시 원인을 해결하고 같은 명령을 다시 실행할 수 있습니다.

사용자 서비스는 부팅 후에도 동작하도록 linger를 활성화합니다. 권한이 부족하면 설치 안내에 나온 `sudo loginctl enable-linger <사용자명>`을 관리자가 실행한 뒤 다시 설치하세요. 자동 준비 후에도 가상환경 생성 오류가 나면 사용 중인 `python3`와 venv·ensurepip 지원 버전이 일치하는지 확인하세요.

##### 토큰 등록과 연결 확인

1. 설치가 끝나면 **워커 주소와 인증 토큰**이 터미널에 표시됩니다. 토큰은 최초 설치 시 워커에서 자동 생성하며, 재설치해도 기존 값을 유지합니다. 워커에서는 `.env`에 권한 600으로 보관합니다.
2. Feeder의 **DOWNLOAD → 기본 설정 → 원격 워커**에서 주소와 토큰을 입력합니다. 신규 설정의 기본값은 비어 있으며, 입력칸의 주소는 예시입니다.
3. **연결 테스트**를 눌러 접속·인증·버전·실행/대기 작업 수를 확인합니다. 저장하지 않은 입력값도 테스트할 수 있으며, 테스트는 전송 작업을 만들지 않습니다.
4. **연결 설정 저장**을 누릅니다. 이 버튼은 상단의 일반 설정 저장과 별도로 동작합니다. Feeder는 토큰을 설정 DB에 저장하고 화면에는 다시 표시하지 않습니다. 이후 토큰 칸을 비워두면 저장된 값을 사용합니다.
5. 프로필의 이송 핸들러를 **원격 워커**로 선택하고 목적지 설정을 저장합니다. 기존 프로필은 자동 변경되지 않습니다.

기존 인증 파일 방식의 설정은 FF 재시작 또는 첫 워커 사용 시 설정 DB로 이전합니다. 원본 파일은 보존하며 이후에는 DB의 토큰을 사용합니다. FF 설정 DB와 백업에도 인증 정보가 들어 있으므로 비공개로 보관하세요. 작업 스냅샷·SSE 주소·연결 테스트 응답에는 토큰을 넣지 않습니다.

설치 안내를 다시 확인하려면 워커 서버에서 다음을 실행합니다. 이 명령은 토큰을 터미널에 표시하므로 출력을 공유하지 마세요.

```bash
cd "$HOME/feeder-worker"
venv/bin/python deploy/setup_install.py --root "$PWD" --show-connection
```

프로필의 기본 목적지는 다음과 같이 지정합니다. `remote_name`에는 설치할 때 지정한 목적지 리모트 이름을 넣습니다. 엔진별 목적지를 쓰는 경우에는 같은 내용을 `destination_by_engine`의 해당 엔진 이름 아래에 지정하세요. 기존 프로필은 자동 변경하지 않습니다.

```yaml
destination:
  type: remote_worker
  use_gdrive_pool: false
  remote_name: destination_remote
  shared_drive_id: '목적지_드라이브_ID'
  upload_path: 'incoming/my-profile'
  complete_path: 'uploads/my-profile'
```

* 원격 서버와 FF의 Rclone에 **같은 이름의 목적지 리모트**가 필요합니다. 리모트 이름을 변환하지 않습니다. 원본 경로가 `source_remote:...`이면 워커의 Rclone에도 해당 소스 리모트가 설정되어 있어야 합니다. 워커는 매 작업의 리모트 이름과 타입을 실제 Rclone 설정에서 확인합니다.
* 리모트의 계정·권한은 실제 원본과 목적지에 접근할 수 있게 구성합니다. 리모트 인증 파일은 자동 동기화하지 않습니다.
* 임시·최종 경로는 프로필에 설정한 값을 그대로 사용합니다. 특정 폴더명 제한은 없으며, 두 경로는 서로 겹치지 않게 지정합니다. 사전 정리는 원격 워커 프로필의 설정 경로도 확인하며, 진행 중이거나 종료가 확인되지 않은 워커 작업의 임시 경로와 계정은 보호합니다.
* 원격 핸들러는 프로필/전역 스테이징 옵션과 관계없이 로컬 스테이징을 생략합니다. 동일한 **Google Drive 업로드 최대 큐**를 소비하고, **Google Drive 업로드 동시 전송 수**에 맞춰 작업을 제출합니다. 워커 자체의 별도 동시 작업 제한은 없습니다. 연결 테스트에도 Feeder의 동시 전송 설정값을 표시합니다.
* 파일 데이터는 원격 서버와 소스·목적지 저장소 사이에서 흐릅니다. Feeder가 실행하는 최종 `backend chpar`는 서버 측 폴더 이동이며 홈 서버에서 파일을 다시 내려받지 않습니다. 이 단계가 실패하면 기존 `move_failed` 복구 흐름을 사용합니다.
* 프로필의 **계정 풀 로테이션**에서 상위 설정 사용 / 계정 풀 사용 / 기본 리모트 사용을 선택합니다. 계정 풀을 사용하는 경우 하위의 **Google Workspace 위임**에서 상위 설정 사용 / 위임 사용 / 위임 사용 안 함을 선택합니다. 두 항목 모두 기본값은 상위 설정 사용입니다. 위임을 사용하면 SA 리모트와 선택된 계정 이메일을, 사용하지 않으면 선택된 계정의 `remote_name`을 사용합니다. 계정 풀을 끄면 프로필의 Drive 리모트를 사용합니다. 계정 선택·사용량·업로드 한도는 Feeder에서 관리합니다.
* 로테이션에 사용하는 모든 계정 리모트와 위임용 SA 리모트는 **워커와 FF 양쪽에 같은 이름으로 준비**합니다. Feeder가 선택한 리모트를 작업에 담아 보내므로 워커 `.env`에 목록을 추가할 필요가 없습니다. 위임용 리모트는 각 서버에서 사용할 수 있는 서비스 계정 인증 파일과 Workspace 위임 권한이 필요합니다. 인증 파일 자체를 작업 요청으로 보내지는 않습니다.
* 계정 풀을 사용하는 작은 작업은 기존 MyDrive 경유 정책을 따릅니다. 워커가 선택한 계정의 MyDrive에 복사한 뒤 Feeder가 공유 드라이브와 최종 폴더로 서버 측 이동을 수행합니다. 큰 작업은 공유 드라이브로 직접 복사합니다. 작업이 시작되면 계정·리모트·위임 이메일을 저장하여 통신 장애나 설정 변경 후에도 같은 전송으로 복구합니다.
* 계정 풀·위임 방식은 워커 0.6.0 이상이 필요합니다. 기존 청크/인증 설정은 유지하며 전송 제외 패턴과 검증한 위임 이메일만 요청에 포함합니다. 홈 서버의 임의 Rclone 추가 옵션은 워커에서 실행하지 않습니다.
* **AllDebrid 사용 시:** Feeder에서 원격 전송을 예약했거나 원격 종료가 미확인된 동안에는 홈 서버의 같은 서비스 스테이징·업로드를 보류합니다. 다른 플러그인이나 외부 프로그램의 접속은 이 제어의 대상이 아닙니다.
* 중단 예약은 현재 전송·최종 이동까지 마무리하고 다음 원격 등록을 막습니다. 즉시 차단·삭제·재시도는 원격 종료 확인을 먼저 수행합니다. 통신 장애로 종료를 확인하지 못하면 임시 파일/원본 삭제를 보류하며 차단 목록에서 정리를 재시도할 수 있습니다.
* 원격 전송 실패는 스냅샷을 보존하고 항목을 실패로 표시합니다. 항목 재시도 시 종료 확인과 임시 파일 정리 후 새 시도 ID를 생성합니다. 이미 복사가 완료됐다면 다시 업로드하지 않고 최종 이동을 재개합니다.
* 워커 0.7.1부터 전송 실패 시 Rclone 종료 코드와 최근 오류 최대 8개를 저장합니다. Feeder의 실패 항목, 다운로드 대시보드 SSE 활동 로그, FF 플랫폼 로그 및 워커 서비스 로그에서 확인할 수 있습니다. 인증 값과 HTTP URL은 가리고 각 오류는 400자로 제한합니다. 0.7.2부터 Rclone 오류에 HTML 응답이 포함되면 머리말·스크립트·스타일을 제외한 본문을 추출한 뒤 길이를 제한합니다. 로컬 Rclone 전송 실패도 SSE와 플랫폼 로그에 표시합니다. Rclone이 파일 크기 불일치만 보고한 경우 다운로드 서버의 HTML 오류 본문까지 자동 조회하지는 않습니다.

##### 운영·업데이트·복구

동시 업로드 작업 수는 Feeder의 **Google Drive 업로드 동시 전송 수**에서 변경합니다. 워커는 제출받은 작업을 실행하며 별도 상한을 두지 않습니다. Rclone의 작업 내부 파일 전송은 `--transfers 1`, `--checkers 2`, `--multi-thread-streams 0`, 버퍼 8MiB를 기본 사용하며 기존 리모트의 청크 설정을 유지합니다. 서비스의 메모리 상한은 768MiB이므로 서버 자원에 맞춰 Feeder의 동시 전송 수를 지정하세요. SSE가 끊겨도 전송은 계속되며 Feeder는 REST 조회로 상태를 확인합니다.

```bash
systemctl --user status feeder-worker
journalctl --user -u feeder-worker -n 50 --no-pager
```

업데이트나 서버 재시작 전에는 Feeder에서 **작업 중단**을 예약하고 현재 업로드가 끝났는지 확인하세요. 워커의 실행·대기 작업이 모두 비어 있을 때 다음 순서로 업데이트합니다.

```bash
systemctl --user stop feeder-worker
curl -fsSL https://raw.githubusercontent.com/golmog/feeder/main/worker/install.sh | bash
```

기본 경로 외에 설치했다면 이전과 같은 `--install-dir`을 사용합니다. 설치 프로그램은 실행 중 서비스의 덮어쓰기를 거부하며, 이전 소스는 설치 폴더의 `backups/source-*`에 보관합니다. 업데이트 후 Feeder에서 연결 테스트를 하고 작업을 재개하세요. Feeder 플러그인 코드도 변경됐다면 FF 재시작이 필요합니다.

워커 서버에서 다음 명령으로 전송 없이 설정·Rclone 버전·DB 무결성을 점검하거나 SQLite 온라인 백업을 만들 수 있습니다.

```bash
cd "$HOME/feeder-worker"
venv/bin/python -m app.manage check
venv/bin/python -m app.manage backup
```

DB 백업은 설치 폴더의 `data/backups/`에 저장됩니다(별도 DB 경로 설정 시 그 DB 폴더 아래). 복원은 서비스를 중지한 뒤 수행하며 SQLite의 DB·WAL·SHM 파일을 서로 다른 시점의 것으로 섞지 마세요. 복원 후에는 Feeder가 기존 작업 ID의 최신 상태를 다시 확인하게 합니다. 워커 재시작 시 대기는 복원하고, 안전하게 재연결할 수 없는 이전 실행 작업은 실패/취소로 정리합니다. 무한 자동 재시도는 하지 않습니다.

| 증상 | 확인할 사항 |
| :--- | :--- |
| 연결 실패 | 워커 서비스 상태, 양쪽 Tailscale 연결, 주소/포트, Tailscale 접근 정책 |
| HTTP 401 | 워커 `.env`의 토큰과 Feeder에 저장한 토큰 일치 여부 |
| 리모트/경로 오류 | 허용 리모트 이름·종류, 인증 파일 권한, FF와 워커의 Drive 리모트 일치 여부 |
| 업로드가 시작되지 않음 | Feeder 작업 중단 상태, 업로드 큐 제한, 기존 원격 작업의 종료 확인 대기 여부 |
| 복사는 완료됐지만 최종 이동 실패 | FF의 Rclone 설정, 공유 드라이브 권한, 최종 경로와 `move_failed` 복구 상태 |

토큰을 바꾸려면 전송을 마치고 서비스를 중지한 뒤 워커 `.env`의 `WORKER_API_TOKEN`을 새 무작위 값으로 바꿉니다. `python3 -c 'import secrets; print(secrets.token_urlsafe(48))'`로 생성할 수 있습니다. 서비스를 다시 시작하고 Feeder에도 같은 토큰을 저장하세요. Rclone 인증 파일과 계정 JSON은 별도 준비하며 API로 자동 동기화하지 않습니다.

##### API 참고

모든 경로는 `/api/v1` 아래에 있으며 **health와 SSE를 포함한 모든 요청에** `Authorization: Bearer <토큰>`이 필요합니다. Rclone 원격 경로만 허용하며 로컬 경로·임의 실행 옵션은 받지 않습니다.

| 메서드·경로 | 기능 |
| :--- | :--- |
| `GET /health` | 버전, Rclone/DB 상태, 실행·대기 작업 수 |
| `POST /jobs` | `job_id`, `source`, `destination`, `config_version`, 선택적 `exclude_patterns`, `drive_impersonate`(위임 이메일)로 복사 등록 |
| `GET /jobs/{job_id}` | 상태, 전송 바이트, 속도, ETA, 오류·Rclone 종료 코드 확인 |
| `GET /jobs?status=running&limit=20&offset=0` | 상태별 작업 목록 조회 |
| `POST /jobs/{job_id}/cancel` | 대기는 즉시 취소, 실행 중이면 프로세스 종료 요청 |
| `GET /events` | 순차 이벤트 ID와 heartbeat를 제공하는 SSE 구독 |
| `GET /config` | 현재 설정과 버전 확인 (`?version=N`으로 이전 버전 조회) |
| `PUT /config` | 전체 실행 설정을 현재 버전 + 1로 갱신 |

`POST /jobs`는 성공 시 202, 입력 오류는 400, 동일 ID에 다른 내용은 409를 반환합니다. 같은 ID와 같은 내용의 재요청은 중복 실행하지 않습니다. 작업은 `queued → running → completed/failed/cancelled`로 바뀝니다. Rclone은 `copy`만 실행하며 원본 삭제와 완료 후 정리는 Feeder가 담당합니다.

취소 요청의 202는 종료 완료를 뜻하지 않습니다. 최신 상태가 종료 상태인지 확인한 뒤 부분 파일을 정리해야 합니다. 아직 등록되지 않은 ID의 취소는 404와 `X-Worker-Cancel-Tombstone: 1`로 이후 같은 ID의 등록도 차단합니다. 단순 404만으로 정리하지 않습니다.

SSE는 `job.queued`, `job.started`, `job.progress`, `job.completed`, `job.failed`, `job.cancelled`, `worker.status`를 보냅니다. 재연결은 마지막으로 처리한 `Last-Event-ID`와 `stream_id`를 함께 사용합니다. `resync_required`가 오면 스트림 기준 번호를 확보한 뒤 REST로 전체 상태를 재조회합니다. 이벤트는 변경 알림으로 사용하고 실제 상태는 REST의 최신 값으로 반영합니다. 기본 SSE 접속 상한은 Feeder 백엔드 1개입니다.

설정 API는 제한된 Rclone 실행 옵션을 지원합니다. 현재 전체 설정을 읽어 버전을 올려 제출하며 대기·실행 작업이 있으면 409로 거부합니다. 응답의 `worker`는 빈 객체입니다. 이전 DB의 `worker.max_concurrent_jobs`와 `.env`의 `WORKER_MAX_CONCURRENT_JOBS`는 호환을 위해 읽기만 허용하며 실행 제한에 사용하지 않습니다. 동시 작업 수는 Feeder에서 관리합니다. 이전 `WORKER_SOURCE_REMOTES`·`WORKER_DESTINATION_REMOTES`도 적용하지 않으며, 소스·목적지 리모트는 Feeder 작업에서 지정합니다. Rclone 리모트 정의와 인증 파일은 워커 서버에 준비하며 이 API로 바꾸지 않습니다.

#### YAML 섹션별 필드 설명

##### `GLOBAL` (피드 공통 필터)

* `quality`: 개별 피드에 화질 조건이 지정되지 않았을 때 사용할 기본 조건.
* `regexp`: 모든 피드에 적용할 `reject`, `accept`, `reject_excluding` 규칙 목록. 문자열 또는 `패턴: {from: 필드}` 형식을 사용합니다.
* `accept_all`: 앞선 거부·화질·필수 포함 조건을 통과한 미허용 항목까지 허용할지 지정합니다. `regexp` 안이 아닌 `GLOBAL` 또는 개별 피드의 최상위 키입니다.
* **필터 순서:** 전역/개별 `reject` → 화질 → 전역/개별 `reject_excluding` → 전역/개별 `accept` → `accept_all`. `accept`와 `accept_all`이 앞선 거부를 무효화하지 않습니다. `accept` 규칙이 없으면 앞 단계에서 거부되지 않은 항목을 기본 허용합니다.

##### `CRAWLERS` (수집기)

| 필드 | 설명 |
| :--- | :--- |
| `id`, `site` | 수집기 ID와 등록된 사이트의 `NAME`. UI에서 새 수집기를 저장하면 ID를 부여하며, 같은 사이트의 중복 수집기 추가는 제한합니다. |
| `boards` | `board`, 선택적인 `subcat` 목록. `166:875`처럼 입력한 게시판도 정규화한 `full_board_key`로 관리합니다. |
| `enabled` | 정기 실행 여부. 개별 수집기 수동 실행은 비활성 수집기도 지정할 수 있습니다. |
| `interval` | CRAWL 스케줄러 N회 실행마다 수집. 분 단위 주기는 CRAWL 기본 설정의 스케줄 항목에 지정합니다. |
| `max_page` | 개별 수집기의 최대 탐색 페이지. 미입력 시 CRAWL 기본 설정 사용. |
| `delay`, `max_retries` | 개별 요청 딜레이·최대 재시도 횟수. 미입력 시 각각 사이트/전역 딜레이, 전역 재시도 설정을 사용합니다. |
| `use_proxy`, `proxy_url` | 프록시 사용 여부와 단일/복수 주소. 개별 주소가 비어 있으면 기본 프록시 주소를 사용합니다. |
| `use_flaresolverr`, `use_selenium`, `use_torrent_info` | 이 수집기에서 사용할 우회·브라우저·메타데이터 옵션. 서비스 주소와 메타데이터 조회 방식은 CRAWL 기본 설정에 지정합니다. |

<br>
##### `FEEDS` (피드)

| 필드 | 설명 |
| :--- | :--- |
| `id`, `name` | 피드 ID와 고유 이름. RSS API는 둘 중 하나로 피드를 지정합니다. |
| `sources` | 내부 소스는 `site`, `board`, 선택적인 `subcat`; 외부 소스는 `type: rss`, `url`을 지정합니다. 외부 URL을 `board`에 넣는 기존 형식도 처리합니다. |
| `quality`, `regexp`, `accept_all` | 개별 필터. `quality`는 문자열 또는 조건 목록으로 지정할 수 있습니다. |
| `feed_count` | API RSS 응답 최대 항목 수. 미입력 시 `feed_feed_count`(기본 100). |
| `use_rss_file`, `rss_file`, `rss_file_path` | 정적 XML 생성 여부·파일명·저장 디렉터리. 파일명 기본값은 `{피드명}.xml`, 경로 미입력 시 FEED 기본 저장 경로 사용. |
| `rss_file_items`, `rss_file_days` | XML 최대 항목 수·보관 기간. 미입력 시 FEED 기본 설정(100개·14일). DB 항목을 삭제하거나 동기화 범위를 줄이는 값으로 사용하지 않습니다. |
| `use_proxy`, `proxy_url` | 외부 RSS 요청의 프록시. 개별 사용 여부가 명시되면 우선 적용하고, 주소가 비어 있으면 FEED 기본 주소를 사용합니다. |

<br>
XML 정기 생성에는 FEED 기본 설정의 **공유용 RSS XML 파일 별도 생성**도 켜고 저장 경로를 지정해야 합니다. DB 보관 기간은 **자동 & DB정리**의 별도 설정입니다.

<br>
##### `DOWNLOADERS` (다운로더 엔진 설정)
* `name`: 프로필 우선순위 체인에서 참조할 고유 영문 식별명.
* `engine_type`: 엔진 타입 (`qbittorrent`, `cd2`, `alldebrid` 또는 `feeder_custom/engines`에 등록된 커스텀 엔진 ID).
* `enabled`: 엔진 활성화 여부.
* `stalled_timeout_hours`: 해당 시간 동안 완료되지 않고 지연될 경우 다음 순위 엔진으로 자동 폴백할 시간(0 입력 시 무제한 대기).
* `missing_timeout_hours`: 원격 전체 목록에서 작업이 연속으로 보이지 않을 때 해당 엔진 내 실패로 처리하기까지의 유예 시간(기본 24시간, 0은 자동 실패 안 함). 조회 오류나 CD2 페이지 제한으로 전체 목록을 확인하지 못한 경우에는 누락으로 판정하지 않습니다. 다시 발견하면 유예 시간을 초기화합니다.
* `max_active_tasks`: AllDebrid의 동시 진행 큐 제한 (0은 무제한). 다른 엔진에는 적용하지 않습니다. 원격에 이미 존재하는 작업의 연동은 신규 큐 슬롯을 소비하지 않습니다.
* `cd2_max_pages`, `cd2_timeout`: CD2 상태 조회 최대 페이지(기본 1)와 요청 타임아웃(기본 30초). 마운트 경로는 `cd2_mount_path`와 `cd2_virtual_path`를 조합하며 이미 가상 경로가 포함된 마운트 경로도 처리합니다.
* 기타 필드는 각 엔진의 `CONFIG_SCHEMA`에 따라 동적으로 설정됩니다.

##### `DOWNLOAD_PROFILES` (다운로드 라우팅 프로필)
* `name`: 프로필 식별명.
* `sync_hours`: 최근 N시간 이내의 FEED DB 항목을 다운로드 큐 등록 대상으로 탐색합니다. 미입력 시 DOWNLOAD 기본 설정(기본 72시간), 0 이하이면 기간 제한 없이 탐색합니다. 이전 `sync_days`는 호환용으로 시간으로 환산합니다.
* `feeds`: 해당 프로필을 적용할 피드 목록 (`['*']`는 전체 적용).
* `priority_chain`: 순서대로 시도할 다운로더 엔진 식별명 배열 (`['ad', 'cd2', 'qbit_main']`).
* 자동 큐 등록은 FEED 항목마다 첫 마그넷을 우선 선택하고, 마그넷이 없으면 첫 링크(ed2k 등)를 선택합니다. 이미 등록된 info hash 또는 링크는 다시 추가하지 않습니다. 첨부파일은 RSS의 다운로드 링크로 제공합니다.
* 동일 폴더명은 실제 파일 존재 여부와 무관하게 DB 이력을 기준으로 해시 접미사(`_[hash]`)를 붙여 구분합니다. 원본 파일명과 출력 폴더명을 별도로 보존하며, 기존 `append_hash_on_conflict` 옵션으로 이 규칙을 끄지 않습니다.
* `destination`: 기본 이송 핸들러 설정 딕셔너리 (`type: local | rclone_simple | gdrive_rotation | remote_worker` 또는 커스텀 핸들러 ID).
  * `use_gdrive_pool`: `remote_worker`의 계정 풀 로테이션 사용 여부입니다. 라디오 버튼에서 **상위 설정 사용 / 계정 풀 사용 / 기본 리모트 사용**을 선택하며, YAML에서는 키 생략 / `true` / `false`에 해당합니다. 목적지 유형이 `gdrive_rotation`이면 계정 풀을 사용하므로 이 중복 옵션을 표시하지 않습니다. 상위 설정이나 이전에 저장한 값이 `false`여도 해당 목적지는 계정 풀을 사용합니다.
  * `use_impersonate`: 계정 풀을 사용할 때만 적용되는 하위 옵션입니다. **상위 설정 사용 / 위임 사용 / 위임 사용 안 함** 중 선택하며, YAML에서는 키 생략 / `true` / `false`입니다. 위임 사용 시 SA 리모트에서 계정 풀의 이메일로 위임하며, 사용하지 않으면 계정별 독립 리모트를 사용합니다. 목적지 유형이 `gdrive_rotation`이면 이 옵션을 바로 표시합니다. 기존 `gdrive_mode` 값도 읽으며, 프로필 화면에서 저장하면 새 옵션으로 변환됩니다. 단, `gdrive_rotation`은 이전 `direct` 값과 관계없이 계정 풀을 사용합니다.
  * `use_local_staging`: **상위 설정 사용 / 사용 / 사용 안 함** 중 하나를 라디오 버튼으로 선택합니다. YAML 키를 생략하면 상위 설정을 따르고, `true`이면 로컬 임시 다운로드, `false`이면 원격 소스에서 직접 전송합니다. `rclone_simple`과 `gdrive_rotation`에서 지원합니다.
  * `staging_path`: 로컬 스테이징 시 사용할 서브 디렉터리 경로.
  * `upload_path`: 리모트 내 임시 업로드 경로. `gdrive_rotation`의 미설정 시 기본값은 `incoming/default`이며 `rclone_simple`은 명시해야 합니다.
  * `complete_path`: 최종 보관 경로. `gdrive_rotation`의 기본값은 `uploads/default`이며 `rclone_simple`은 명시해야 합니다. `local`은 FF 파일시스템의 절대 경로 사용을 권장합니다. CD2 상대 경로는 엔진 마운트 루트와 결합합니다.
  * `shared_drive_id`: 목적지 공유 드라이브 ID.
* `destination_by_engine`: 엔진 **식별명**별 목적지 override. 기본 `destination`에 해당 엔진의 필드를 병합합니다. 엔진 타입명 대신 `qbit_main`, `cd2` 등 `DOWNLOADERS.name`을 사용합니다.
* `rclone_simple`은 `remote_name`, `upload_path`, `complete_path`를 사용합니다. `gdrive_rotation`은 `mydrive_remote_name`, `shared_drive_id`, 업로드/완료 경로를 확인합니다. 전송 옵션은 **엔진별 목적지 → 기본 목적지 → 프로필 공통 값 → 다운로더 엔진 → DOWNLOAD 기본 설정** 순으로 적용합니다. 엔진별 목적지에서도 같은 업로드 방식과 스테이징 옵션을 개별 지정할 수 있습니다.
* 원격 워커에서도 `use_gdrive_pool`로 계정 풀 사용 여부를, 그 하위의 `use_impersonate`로 위임 여부를 지정할 수 있습니다. 로컬 스테이징은 생략하며 Rclone 리모트 간 전송을 수행합니다. 이미 전송 설정이 저장된 항목은 그 설정으로 후속 이동·정리를 마칩니다.

##### `GDRIVE_ACCOUNTS` (Google Drive 계정 풀)
* `username`: 계정 식별자 (위임 사용 시 `--drive-impersonate` 대상 이메일 주소).
* `mydrive_rclone_id`: 1차 15GB 우회 업로드를 수행할 MyDrive 폴더의 Rclone ID.
* `remote_name`: 비위임(독립 리모트) 사용 시 `rclone.conf` 상의 개별 리모트 이름 (선택 사항).

##### 각 모듈의 기본 설정 (FF 설정 DB)

아래 값은 YAML의 `GLOBAL`에 옮겨 적는 항목이 아니라 각 모듈의 **기본 설정 / 자동 & DB정리** 화면에서 관리하는 값입니다.

<br>
| 설정 | 기본값 | 용도 |
| :--- | :--- | :--- |
| `crawl_interval`, `feed_interval`, `download_interval` | `10`, `10`, `5` | 모듈별 정기 실행 주기(분). Cron 형식도 지원. 자동 시작 기본 OFF. |
| `crawl_max_page`, `crawl_always_max_page` | `5`, OFF | 최대 탐색 페이지와 증분 수집 조기 종료 여부. |
| `crawl_crawler_delay`, `crawl_crawler_max_retries` | `2.0`, `3` | 기본 요청 딜레이(초)와 재시도 횟수. |
| `crawl_flaresolverr_url`, `crawl_selenium_remote_url` | 빈 값 | 선택한 우회/브라우저 서비스 주소. 서비스는 별도 실행해야 합니다. |
| `crawl_torrent_info_method` | `plugin` | 토렌트 메타데이터 조회 방식. `plugin`은 `torrent_info` 플러그인, `qbittorrent`는 `crawl_qb_url`·사용자명·비밀번호 설정을 사용합니다. |
| `feed_feed_count` | `100` | API RSS 최대 항목 수. |
| `feed_make_rss_file`, `feed_rss_file_path` | OFF, 빈 값 | 정적 XML 정기 생성 스위치와 기본 저장 경로. |
| `feed_rss_file_items`, `feed_rss_file_days` | `100`, `14` | XML 최대 항목 수와 출력 대상 기간(일). |
| `download_feed_sync_hours` | `72` | 다운로드 큐에 새로 등록할 FEED 항목의 기간(시간). |
| `download_use_local_staging`, `download_max_staging_items` | ON, `10` | 전역 스테이징 사용 여부와 Google Drive 업로드 최대 큐. 기존 최대 보관 수 설정 키·저장값은 그대로 사용합니다. |
| `download_upload_workers` | `2` | Google Drive 업로드와 로컬 스테이징에 공통으로 적용하는 동시 전송 수. 이전 `download_staging_workers` 값은 더 이상 사용하지 않습니다. |
| `download_rclone_conf_path`, `download_rclone_upload_remote_name` | 빈 값, 빈 값 | FF/워커가 읽을 설정 파일의 경로와 기본 업로드 리모트명. |
| `download_rclone_chunk_size`, `download_rclone_extra_options` | `256M`, `--timeout 30m` | Google Drive 업로드 청크 크기와 지원 경로에 적용할 추가 옵션. |
| `download_use_gdrive_pool`, `download_gdrive_use_impersonate` | OFF, OFF | 계정 풀 사용과 도메인 위임 사용 여부. |
| `download_gdrive_mydrive_remote_name`, `download_shared_drive_id` | `gdrive_sa`, 빈 값 | MyDrive 기본 리모트와 목적지 공유 드라이브 ID. |
| `download_mydrive_upload_threshold`, `download_gdrive_upload_limit`, `download_shared_drive_upload_limit` | `14GB`, `700GB`, `3TB` | MyDrive 경유 분기와 사용량 설정 한도. |
| `download_shared_drive_quota_reset_time` | `16:00` | 공유 드라이브 쿼터 차단 해제 시각(Asia/Seoul). |
| `crawl_db_auto_delete`, `feed_db_auto_delete`, `download_db_auto_delete` | 모두 OFF | 각 DB 자동 정리. `*_db_delete_day` 기본값은 30일. |

<br>
일부 Rclone 경로·옵션은 YAML의 소문자 `rclone` 섹션도 호환 경로로 읽지만, 새 설정은 우선 UI의 필드와 개별 프로필을 사용하세요. 모든 설정이 동일한 우선순위를 공유하는 구조는 아닙니다.


---

### 5. 다운로드 파이프라인 및 클라우드 연동 가이드

FEED는 수집 항목을 필터링해 DB에 적재하고, DOWNLOAD는 프로필의 대상 피드·기간·우선순위 체인을 기준으로 작업을 만듭니다. RSS API/XML의 출력 개수는 다운로드 큐 등록 개수를 제한하지 않습니다.

```text
[FEED DB → 프로필별 다운로드 대상 동기화 / 직접 다운로드 등록]
                         │
                         ▼
                     pending
                         │ 엔진 선택·원격 기존 작업 연동 또는 신규 등록
                         ▼
                    downloading
                         │ 완료 확인
             ┌───────────┼────────────────┐
             ▼           ▼                ▼
  pending_local_staging  downloaded    pending_relay
             │           │                │ 원격 워커 선점
       local_staging     │          relay_transferring
             │           │                │ 결과 보고
             └──────→ downloaded          └──→ completed / failed
                         │ 이송 핸들러 선택
          ┌──────────────┼─────────────────────┐
          ▼              ▼                     ▼
        local       rclone_simple       gdrive_rotation
          │              │                     │
          │       임시 업로드 → 최종 이동   pending_upload
          │              │                     │
          │              │                  uploading
          │              │                     │
          └───────→ completed ←──── 원격 이동·소스 정리

이송 핸들러 없음/초기화 실패 → waiting_transporter → 설정 복구 후 재이송
다운로드 추가/재시작 불가·지연 → 다음 엔진의 pending 또는 체인 소진 failed
원격 전체 목록 연속 누락 유예 초과 → 해당 엔진에서 failed
Google Drive 업로드 후 이동/정리 실패 → move_failed
```

#### 1) Google Drive 계정 풀 및 2단계 이동

* **계정 풀 ON:** 24시간 사용량이 적은 가용 계정을 할당합니다. 파일 크기가 MyDrive 임계값(기본 14GB) 이하이면 MyDrive를 경유하고, 초과하면 같은 선택 계정의 리모트로 공유 드라이브에 직행합니다. 두 경로 모두 작업 종료 시 계정 점유를 해제합니다.
* **계정 풀 OFF:** 계정 목록 없이 기본 업로드 리모트를 사용해 공유 드라이브로 직행합니다. MyDrive 경유를 위한 계정 할당은 수행하지 않습니다.
* **MyDrive 경유 순서:**
  1. 소스를 계정의 MyDrive `upload_path/출력폴더명`에 복사합니다.
  2. 인덱스 반영을 위해 대기한 뒤 공유 드라이브의 임시 경로로 `rclone move`합니다.
  3. 공유 드라이브 안에서 `rclone backend chpar`로 `complete_path` 아래로 이동합니다.
  4. 성공 후 로컬 소스 또는 스테이징 파일과 다운로더 작업 이력을 정리합니다.
  5. 정상 MyDrive 경유 후처리는 `rclone cleanup`도 호출합니다. 잔여 incoming 파일을 보존하는 순회 복구 경로에서는 이 명령을 실행하지 않습니다.
* **공유 드라이브 직행:** 공유 드라이브 임시 경로에 복사한 뒤 최종 경로로 `chpar`합니다.
* **리모트 선택:** 풀 ON·Impersonate ON은 MyDrive 기본 리모트와 계정 이메일을, 풀 ON·Impersonate OFF는 계정 `remote_name`(없으면 기본 리모트)을 사용합니다. 풀 OFF는 기본 업로드 리모트(없으면 MyDrive 기본 리모트)를 사용합니다. 이 흐름에서 선택한 리모트를 원격 이동에도 유지합니다.
* **권한과 제한:** 대상 공유 드라이브 ID, 경로, 계정 권한을 준비해야 합니다. 기본 개인 사용량 700GB·공유 드라이브 설정 한도 3TB·MyDrive 임계값 14GB는 Feeder의 설정값입니다. 서비스가 부과하는 제한과 별개이며, 임계값·계정 수만으로 전송 성공이나 쿼터 우회를 보장하지 않습니다.
* **프로필별 설정:** `mydrive_remote_name`, `shared_drive_id`, `upload_path`, `complete_path`를 확인하세요. `shared_remote_name`은 릴레이 목적지 구성에서 사용하며, 기본 Google Drive 풀 업로드는 위의 선택 리모트 규칙을 따릅니다.

#### 2) 로컬 스테이징·전송률·통계

* 스테이징 사용 여부는 **프로필(현재 엔진의 목적지 override 포함) → 엔진 → DOWNLOAD 기본 설정 → 코드 기본값** 순으로 해석합니다. CD2는 엔진 설정에서 기본 OFF를 제공하며, 프로필에서 명시적으로 변경할 수 있습니다.
* 스테이징 경로는 `스테이징 루트 / staging_path / 출력폴더명`입니다. `staging_path`가 비어 있으면 프로필 이름 또는 `default`를 사용합니다. 루트 미설정 시 `{path_data}/tmp/feeder/staging`을 사용합니다.
* `Google Drive 업로드 최대 큐`는 스테이징 중·업로드 대기·업로드 진행 작업과 **실패 등으로 로컬 버퍼에 남은 작업**을 ID 기준으로 한 번씩 계산합니다. 직접 업로드도 같은 큐를 사용합니다. 한도에 도달하면 새 스테이징·직접 업로드 큐 진입·새 슬롯이 필요한 이동 실패 재시도를 보류하며, 이미 슬롯을 차지한 작업의 업로드는 계속 진행합니다. 기존 큐가 변경한 한도를 초과해도 작업을 삭제하지 않고 처리하면서 줄입니다. 이 값은 작업 수 기준이며 디스크 여유 바이트를 직접 예약하지 않습니다.
* 대시보드의 `업로드` 카드는 `현재 큐 / 전체`로 표시합니다. 앞 숫자는 위 제한과 같은 집계이며 진행 중인 작업을 포함합니다. 뒤 숫자는 스테이징·업로드·이송 진입 대기와 진행 중 릴레이를 더한 전체 작업 수입니다. 예를 들어 `50 / 113`은 현재 큐 50개와 큐 밖 대기 63개를 뜻합니다. 기존 초과 작업이 남아 있다면 앞 숫자도 설정 한도보다 클 수 있습니다.
* 스테이징과 Google Drive 업로드 워커는 모두 `Google Drive 업로드 동시 전송 수`를 따릅니다(기본 2개). 동시 실행 수와 큐 개수 제한은 별도로 적용합니다.
* 대량 수집·피드 동기화·큐 순회·디렉터리 용량 계산·SSE 목록 구성 중에는 실행 컨텍스트별로 약 20ms마다 1ms씩 양보합니다. gevent 활성 환경에서는 gevent에, 일반 워커에서는 OS 스케줄러에 실행 기회를 주며 기존 수집 딜레이와 재시도 간격은 그대로 적용합니다.
* 작업 활동 로그의 시각은 브라우저 현지 시간의 `HH:mm:ss` 형식입니다. 각 단계의 순회 개수는 약 3초 간격과 단계 종료 시 갱신합니다. 다운로드 등록은 검토·신규 등록·기존 연동 수를 구분하며, 스테이징·업로드는 처리 종료·진행 중·미배정 수를 표시합니다. 이 집계가 그대로인 동안에는 같은 로그를 반복해서 남기지 않으며 SSE 수신 확인만 계속됩니다. 외부 엔진 조회·캐시 갱신·이송 호출 중에는 해당 요청을 기다리고 있음을 표시합니다. 검토 수와 처리 종료 수는 전송 성공 건수와 구분해서 확인하세요.
* Rclone 추가 옵션 기본값은 `--timeout 30m`, Google Drive 청크 크기는 `256M`입니다. 전송 제외 패턴은 스테이징과 Google Drive 업로드 명령에 적용합니다.
* 스테이징·Google Drive 전송률은 `--stats-one-line` 출력을 파싱해 IPC API로 웹 프로세스에 전달합니다. SSE는 약 3초 주기이며 원격 엔진 상태는 약 6초, 24시간 집계는 약 30초 간격으로 갱신합니다. 실제 주기는 엔진 응답 시간에 따라 길어질 수 있습니다.
* 큐 대시보드의 **작업 활동 로그**는 사전 정리, 피드·프로필 검토 진행, 이송·업로드 처리, 주기 완료·보류·오류를 표시합니다. 큐 통계와 별도의 SSE 연결로 약 3초마다 수신하며, SSE를 끄거나 지원하지 않는 브라우저에서는 폴링으로 조회합니다. 마지막 작업 활동 시각과 수신 시각은 구분해서 표시합니다. SSE 수신은 웹 연결 상태이며 Celery 작업의 생존을 보장하는 신호는 아닙니다. 최근 200개 기록은 웹 서버와 Celery가 공유하는 `db/feeder_download_activity.db`에 보관하여 새로고침·재연결 후에도 확인할 수 있습니다. **화면 비우기**는 현재 화면만 비우며 저장된 기록은 유지합니다. 자동 스크롤을 끄면 이전 기록을 읽을 수 있습니다. 인증 정보와 원본 오류·명령 출력은 이 패널에 전달하지 않으며, 상세 오류는 플랫폼 로그에서 확인합니다.
* 업로드 사용량은 전송 시도 시 파일 크기로 기록하므로 재업로드·실패 시도가 통계에 포함될 수 있습니다.

#### 3) 상태 조회와 실패·대기 정책

| 상태 | 의미와 처리 |
| :--- | :--- |
| `pending` | 엔진 추가 대기. 비활성 엔진 또는 AllDebrid 큐 한도 도달 시 대기 유지. |
| `downloading` | 엔진에서 진행 중. 상태는 작업 ID 또는 info hash로 매칭합니다. |
| `pending_local_staging` / `local_staging` | 로컬 수신 대기 / 수신 중. |
| `downloaded` | 다운로드 또는 스테이징 완료, 이송 라우팅 대기. |
| `waiting_transporter` | 목적지 ID 오타, 확장 스크립트 삭제·로드 실패, 생성 오류 등으로 핸들러를 사용할 수 없음. 원본을 보존하고 다음 실행에서 재확인합니다. 목록 필터와 수동 완료·실패·삭제를 지원합니다. |
| `pending_upload` / `uploading` | Google Drive 업로드 대기 / 실행 중. 복사 실패는 보존한 소스로 다음 주기에 재시도합니다. |
| `pending_relay` / `relay_transferring` | 외부 워커 선점 대기 / 릴레이 전송 중. |
| `move_failed` | Google Drive 업로드 후 이동·소스 정리 실패 이력. 아래 재시도 정책을 적용합니다. |
| `completed` | 최종 완료 또는 사용자가 수동 완료한 작업. |
| `failed` | 체인 소진, 원격 누락 유예 초과, 이송 실패 또는 수동 실패. |
| `cancelled` | 사용자 삭제/취소로 재등록 방지 이력을 유지하는 상태. |

<br>
* **원격 누락:** 전체 목록을 정상 조회했는데도 작업이 계속 없으면 `missing_timeout_hours`(기본 24시간) 후 해당 엔진에서 실패 처리합니다. 다음 엔진으로 자동 전환하지 않습니다. 조회 오류·CD2의 부분 목록은 누락 유예를 초기화하고, 다시 발견한 경우에도 초기화합니다.
* **CD2 조회 제한:** `cd2_max_pages` 기본값은 1입니다. 긴 115 히스토리를 매번 전체 조회하지 않도록 유지하는 보호 설정입니다. 범위 밖으로 밀린 작업을 찾기 위해 자동으로 추가 페이지를 조회하지 않습니다.
* **지연 타임아웃:** `stalled_timeout_hours`는 엔진 추가 시점 기준 경과 시간으로 판정하며, 전송 속도가 0인 시간만 누적하는 방식은 아닙니다. 초과 시 다음 엔진을 시도합니다.
* **`move_failed` 재시도:** 마지막 시도 후 1시간이 지난 미종료 작업을 확인합니다. 소스가 있으면 이전 원격 잔여 파일을 최종 경로로 보존한 후 재업로드합니다. 소스가 없으면 상태는 `move_failed`로 두고 완료 시각·설명을 기록해 성공 종료로 집계합니다. 원격 소스 조회 오류는 파일 없음으로 확정하지 않습니다.
* **리모트 잔여 파일:** 작업에 기록된 경로와 프로필의 `upload_path` → `complete_path` 경로를 이용해 최종 경로로 이동합니다. 순회 정리에서 잔여 파일을 `purge`하거나 휴지통을 비우지 않으며, 진행/대기 중인 업로드 계정은 순회 대상에서 보호합니다. 소스 없음 성공 종료와 원격 파일 복구 결과는 별도로 확인합니다. 새 업로드 전에 드라이브별 최상위 목록을 한 번씩 조회합니다. MyDrive는 계정별로, 공유 드라이브는 인증 계정 수와 관계없이 드라이브별로 중복을 제거합니다. `incoming` 등 설정된 스테이징 경로 필터로 조회 범위를 제한하고 해당 결과를 재사용하여 실제 파일이 남은 분류 경로만 이동합니다. 빈 분류·작업 폴더는 복구 이동 대상에서 제외합니다. 이동 완료 후 MyDrive에서는 파일이 남지 않은 최상위 스테이징 경로(`incoming` 등)에 `rclone rmdirs`를 실행합니다. 공유 드라이브에서는 프로필별 임시 경로(`incoming/프로필명` 등)에 `rclone rmdirs --leave-root`를 실행하여 그 하위 빈 폴더만 정리하고, 프로필 폴더와 그 상위 경로는 보존합니다. 공유 드라이브 복구 이동에서도 폴더 전체 이동을 비활성화하여 프로필 폴더 자체를 보존합니다. 파일이 남았거나 조회·이동이 실패한 경로는 빈 폴더 정리를 보류합니다. 개별 원격 복구·빈 폴더 정리 명령은 60초 내에 종료하며, 조회·이동·빈 폴더 정리 실패 시 로그를 남기고 새 업로드를 보류합니다.
* **수동 제어:** 수동 실패는 원본을 보존합니다. 삭제/취소는 실행 중 Rclone·원격 엔진 작업·접근 가능한 로컬 파일을 정리하고 `cancelled` 이력을 남깁니다. 이미 취소된 항목을 다시 삭제하면 DB에서 제거합니다. DB 이력을 지우면 해당 이력에 의존하던 재등록·이름 중복 방지 근거도 사라집니다.

#### 4) 설정 반영·재시작·DB 보관

* **정리 병렬 처리:** 사전 정리의 드라이브 목록 조회와 빈 폴더 정리는 각각 최대 4개씩 병렬 처리합니다. 업로드 동시 전송 수와는 별개이며, 같은 최종 경로로의 충돌을 피하도록 파일 복구 이동은 순차 처리합니다. 파일 복구 이동이 모두 끝난 뒤 빈 폴더를 정리하고, 정리가 끝나야 새 업로드를 시작합니다. 공유 드라이브에서 보존할 프로필 폴더 자체만 남았다면 추가 빈 폴더 정리는 생략합니다. 중단 요청 후에는 다음 정리 명령을 배정하지 않습니다.
* **작업 중단 예약:** 다운로드 큐 및 대시보드에서 `작업 중단 예약`을 누르면 새 작업 배정을 멈추고 이미 시작한 업로드의 최종 이동·소스 정리와 진행 중인 이송·스테이징을 마칩니다. 화면에 `작업 중단됨`이 표시된 뒤 FF를 재시작하세요. 이미 외부 다운로더에 등록한 다운로드와 원격 릴레이는 외부에서 계속 진행할 수 있습니다. 중단 상태는 DB에 저장되어 재시작 후에도 자동·수동 실행을 보류하며, `작업 재개`로 해제합니다. 종료 대기 중에는 `중단 예약 취소`로 계속 처리할 수 있습니다.
* **개별 실행:** 전체 작업이 완전히 중단되면 큐 항목 하단의 `개별 실행`이 활성화됩니다. 한 번에 한 항목만 현재 단계부터 가능한 후속 단계까지 처리하고 전체 중단은 유지합니다. 사전 정리·계정 선택·큐 제한은 기존 설정을 따르며, 다른 대기 항목을 등록·스테이징·업로드하지 않습니다. 외부 다운로드가 아직 끝나지 않았다면 이번 실행에서 상태를 확인한 뒤 대기 상태로 남깁니다. 실패 항목은 먼저 `재시도`로 대기 상태로 바꿔주세요. 진행 상황과 결과는 SSE 작업 활동 로그에서 확인할 수 있습니다.
* 활성 큐에는 현재 피드에 매칭되는 프로필과 엔진별 목적지 override를 다시 적용합니다. `feeds`의 `*`와 구체 피드가 겹치면 순서에 영향을 받으므로 프로필 적용 범위를 구분해 설정하세요.
* 재시작 시 `local_staging`, `uploading`, `relay_transferring`를 각 대기 상태로 되돌립니다. 업로드 단계 기록(`upload_state`)이 있으면 완료한 복사를 반복하지 않고 후처리를 재개할 수 있습니다. `move_failed`로 전환된 작업에는 앞 절의 소스 유무 재시도 정책을 적용합니다.
* `source_file_name`, `output_folder_name`, `remote_missing_since`, `upload_state` 등 필요한 컬럼·인덱스는 로드 시 추가합니다.
* DB 자동 정리는 모듈별로 켭니다. 기본 보관 기간은 30일이며 기본 자동 삭제는 OFF입니다. DOWNLOAD는 오래된 성공 종료 이력을 정리하고, CRAWL/FEED는 각 DB의 생성 시각 기준으로 정리합니다. XML 출력 보관 기간과는 독립적입니다.

---

### 6. 커스텀 스크립트 개발 가이드 (`feeder_custom`)

Feeder의 모든 핵심 계층은 `{path_data}/db/feeder_custom/` 디렉터리에 파이썬 스크립트를 추가하여 자유롭게 확장할 수 있습니다.

<br>
#### 1) 커스텀 사이트 훅 (`feeder_custom/sites/site_{NAME}.py`)
사이트 수집 라이프사이클의 필요한 단계만 구현합니다. `SITE_NAME`은 사이트 JSON의 `NAME`과 매칭하며 `DEFAULT_SITE_INFO`는 미등록 사이트의 기본 규칙으로 등록합니다. `on_extract_detail`을 제공하면 기본 마그넷 추출 대신 반환값을 사용하므로, 기본 추출을 유지하려면 해당 메서드를 생략하거나 명시적으로 호출해야 합니다.

```python
# -*- coding: utf-8 -*-
from feeder.setup import logger

class CustomSiteHook:
    SITE_NAME = "custom_site"

    DEFAULT_SITE_INFO = {
        "NAME": "custom_site",
        "TORRENT_SITE_URL": "https://example.com",
        "DELAY": 1.5,
        "BOARD_URL_RULE": "{URL}/board/{BOARD_NAME}?page={PAGE}",
        "XPATH_LIST_TAG": {"XPATH": "//tbody/tr[%s]//td[2]//a", "INDEX_START": 1, "INDEX_STEP": 1},
        "ID_REGEX": "id=(?P<id>\\d+)"
    }

    @classmethod
    def on_init_session(cls, site_info: dict, scheduler_cfg) -> None:
        """크롤링 시작 전 세션 초기화 (Selenium 스텔스 브라우저 기동 등)"""
        pass

    @classmethod
    def on_page_loaded(cls, driver, url: str) -> None:
        """페이지 로드 직후 팝업 닫기나 성인 인증 레이어 클릭"""
        pass

    @classmethod
    def on_extract_detail(cls, detail_html: str, item: dict, site_info: dict, scheduler_cfg) -> list[str]:
        """본문 HTML 수신 후 마그넷/ed2k 리스트 추출 및 반환"""
        return []
```

#### 2) 커스텀 다운로드 엔진 (`feeder_custom/engines/engine_{ID}.py`)
`BaseDownloadEngine`을 상속받아 새로운 다운로더(PikPak, Real-Debrid, 시놀로지 DownloadStation 등)를 추가합니다.

```python
# -*- coding: utf-8 -*-
from feeder.util_download import BaseDownloadEngine

class CustomEngine(BaseDownloadEngine):
    ENGINE_ID = "custom"
    ENGINE_NAME = "커스텀 다운로더"
    OUTPUT_TYPE = "local"              # "local", "cloud_storage", "remote_cloud"
    SUPPORTED_PROTOCOLS = ["magnet"]   # "magnet", "ed2k"
    status_list_complete = True       # 현재 응답이 전체 목록일 때만 True

    CONFIG_SCHEMA = [
        {"name": "api_url", "label": "API 주소", "type": "text", "default": "http://127.0.0.1:9091"},
        {"name": "api_key", "label": "API Key", "type": "password"}
    ]

    def add_magnet(self, link: str, title: str = None, upload_path: str = None) -> tuple[bool, str, str]:
        """마그넷 추가 (성공여부, 작업ID, 에러메시지) 반환"""
        raise NotImplementedError("실제 엔진의 작업 추가 API를 구현하세요")

    def get_status(self, task_ids: list[str] = None) -> tuple[list[dict], str]:
        """
        작업 상태 목록 조회
        반환 규격: [{'task_id': str, 'hash': str, 'status': 'completed'|'downloading'|'error', 'filename': str, 'file_size': int, 'source_path': str}]
        """
        raise NotImplementedError("상태 조회와 목록 완전성 판정을 구현하세요")

    def delete_task(self, task_id: str) -> bool:
        """작업 삭제"""
        raise NotImplementedError("엔진 작업/히스토리 삭제를 구현하세요")

    def restart_task(self, task_id: str) -> bool:
        """에러/중단 작업 재시작 (선택 사항)"""
        return False

    def test_connection(self) -> tuple[bool, str]:
        """연결 테스트"""
        raise NotImplementedError("연결 확인을 구현하세요")
```

엔진 구현 시 다음 계약을 지켜야 합니다.

* `get_status()`의 `task_id`·`hash`는 추가 시 반환한 작업 ID 및 원본 링크의 해시와 매칭되어야 합니다. 완료 항목에는 실제 파일명·용량·사용 가능한 `source_path`를 제공하세요. CD2처럼 파이프라인에서 마운트를 별도로 찾는 내장 엔진은 예외입니다.
* 전체 목록을 성공적으로 확인한 경우에만 `status_list_complete = True`로 설정합니다. 페이지/필터로 일부만 반환하면 `False`, 조회 오류는 반환 튜플의 오류 문자열로 전달해야 원격 누락 오판을 피할 수 있습니다.
* 커스텀 엔진에도 공통 `missing_timeout_hours` 설정 필드가 추가됩니다. `restart_task()`가 `False`이면 엔진 오류 시 다음 우선순위로 전환할 수 있습니다.
* 스크립트는 로드 시 실행됩니다. UI 저장 후 엔진 목록·연결 테스트로 등록 여부를 확인하세요. 위 예제의 `NotImplementedError` 부분은 실제 동작으로 교체해야 합니다.

#### 3) 커스텀 이송 핸들러 (`feeder_custom/transporters/trans_{ID}.py`)
`BaseTransporter`를 상속받아 다운로드 완료 후 특정 스토리지로 전송하거나 텔레그램 알림을 발송하는 후처리기를 추가합니다.

```python
# -*- coding: utf-8 -*-
from feeder.setup import logger
from feeder.util_download import BaseTransporter

class CustomTransporter(BaseTransporter):
    TRANSPORTER_ID = "custom_trans"
    TRANSPORTER_NAME = "커스텀 이송 핸들러"

    CONFIG_SCHEMA = [
        {"name": "target_path", "label": "대상 경로", "type": "text", "default": "/data/media"}
    ]

    def transport(self, item, source_path: str, dest_config: dict) -> tuple[bool, str, str]:
        """
        이송 실행
        반환: (성공여부, 다음상태['completed'|'failed'|'pending_upload' 등], 메시지)
        """
        logger.info(f"이송 실행: {item.title} -> {dest_config.get('target_path')}")
        raise NotImplementedError("실제 이송 성공을 확인한 뒤 상태를 반환하세요")
```

출력 이름은 `item.output_folder_name`, 원본 이름은 `item.source_file_name`으로 구분합니다. 이송 핸들러 호출 전에 DB 세션을 반환하므로, 별도 조회가 필요한 구현은 자신의 세션 범위를 관리해야 합니다. 성공 반환은 실제 이송을 확인한 뒤에만 하세요.

원격 릴레이 핸들러는 `IS_RELAY_HANDLER = True`를 선언하고 `pending_relay`로 인계합니다. 대기 작업을 다시 기동하려면 선택적으로 `resume_relay(dest_config)`를 구현할 수 있습니다. 워커 실행 방법과 전송 로직은 해당 확장의 책임입니다.

---

### 7. API 엔드포인트 명세

아래 경로의 `/feeder`는 플러그인 패키지 경로입니다. RSS·첨부파일·SSE 요청은 FF의 API 인증 설정을 따르며, 릴레이 선점/보고는 시스템 API Key를 별도로 확인합니다. 피드 이름 등 쿼리 값은 URL 인코딩해서 전달하세요.

<br>
#### 1) 피드 및 첨부파일 API

* **RSS 피드 발행 (XML):**

  ```text
  GET /feeder/api/feed/rss?name={FEED_NAME}&apikey={APIKEY}
  GET /feeder/api/feed/rss?id={FEED_ID}&apikey={APIKEY}
  ```

  FEED DB에 동기화된 항목을 반환합니다. 피드의 `feed_count` 또는 FEED 기본 최대 수를 적용하며, 한 DB 항목이 여러 마그넷/첨부파일로 펼쳐져도 실제 RSS `<item>` 수가 상한을 넘지 않습니다. 피드가 없으면 404, 식별자 누락은 400입니다. GET 요청이 수집기를 실행하거나 외부 RSS를 새로 수집하지는 않습니다.

<br>
* **첨부파일 다운로드:**

  ```text
  GET /feeder/api/crawl/download?id={CRAWL_ITEM_ID}_{FILE_INDEX}&apikey={APIKEY}
  GET /feeder/api/feed/download?id={FEED_ITEM_ID}_{FILE_INDEX}&apikey={APIKEY}
  ```

  `FILE_INDEX`는 0부터 시작합니다. CRAWL과 FEED는 서로 다른 DB 항목 ID를 사용하므로 각 경로에 해당하는 ID를 전달해야 합니다. FEED RSS의 첨부파일 링크는 `/api/feed/download`를 사용합니다.

<br>
* **사이트 규칙 업데이트:**

  ```text
  POST /feeder/api/crawl/site_update?apikey={APIKEY}
  Content-Type: application/x-www-form-urlencoded
  content={UTF-8_JSON을_BASE64로_인코딩한_값}
  ```

  JSON의 `NAME`으로 사이트를 추가하거나 기존 규칙을 갱신합니다. 수집기·피드·프로필 설정을 함께 갱신하는 API는 아닙니다.

<br>
#### 2) 다운로드 및 릴레이 API

* **대시보드 SSE:**

  ```text
  GET /feeder/api/download/sse?apikey={APIKEY}
  ```

  `text/event-stream`의 `data:` JSON으로 `timestamp`, `counts`, `total_upload_24h`, `total_download_24h`, `active_list`를 전달합니다. 활성 항목에는 진행률·속도·ETA가 포함될 수 있고, 원격에만 남은 작업은 `engine_unmanaged`로 표시합니다. `counts.completed`는 소스 없음으로 성공 종료한 `move_failed`도 포함합니다.

* **다운로드 작업 활동 SSE:** `GET /feeder/api/download/activity?apikey={APIKEY}`로 최근 작업 기록과 수신 확인 신호를 전달합니다. `data:` JSON의 `entries`에는 `id`, `timestamp`, `stage`, `message`, `level`이 들어갑니다. 새 기록이 없어도 약 3초마다 빈 `entries`를 보내며, 재연결 시 `Last-Event-ID` 이후의 보관 중인 기록을 최대 200개 전달합니다. `snapshot=1&after={마지막_ID}`를 추가하면 SSE 대신 같은 형식의 JSON을 한 번 조회합니다. 이 경로에도 FF의 API 인증 설정이 적용됩니다.

<br>
* **원격 릴레이 작업 선점 (Claim):**

  ```text
  POST /feeder/api/download/relay_claim
  Content-Type: application/x-www-form-urlencoded
  apikey={APIKEY}
  ```

  `pending_relay` 한 건을 `relay_transferring`으로 바꾸고 반환합니다. 대기 작업이 없으면 `has_task: false`입니다. 반환 필드는 `ret`, `has_task`, `item`, `rclone_conf`, `sa_files`이며 `item`에는 다음 값이 포함됩니다.

<br>
  | 필드 | 의미 |
  | :--- | :--- |
  | `id`, `title`, `file_name`, `file_size` | 작업 식별·표시·용량 정보. |
  | `output_folder_name` | DB 이력에 따라 해시 접미사까지 확정한 출력 폴더명. 원본 `file_name`과 구분합니다. |
  | `is_single_file` | 단일 파일 여부. |
  | `remote_source_path` | 실제 원격 소스. 출력 이름으로 소스 경로를 재조합하지 마세요. |
  | `dest_path`, `dest_dir_path` | 최종 대상 경로와 대상 디렉터리. 단일 파일도 출력 폴더 아래에 배치합니다. |
  | `buffer_limit_bytes` | 커스텀 워커가 사용할 버퍼 한도. 목적지 설정 `buffer_limit_gb`의 기본값은 50입니다. |

  `rclone_conf`와 `sa_files`는 FF에서 읽을 수 있는 설정 및 서비스 계정 파일을 워커용으로 전달하는 값입니다. API만으로 워커를 생성하지 않으며, 실제 전송과 아래 결과 보고를 구현해야 합니다.

<br>
* **원격 릴레이 결과 보고 (Report):**

  ```text
  POST /feeder/api/download/relay_report
  Content-Type: application/json
  ```

  ```json
  {
    "apikey": "YOUR_FF_API_KEY",
    "id": 123,
    "success": false,
    "bytes_transferred": 0,
    "error_msg": "전송 실패 원인"
  }
  ```

  JSON boolean을 권장하며, 폼 전송의 `true`/`false`, `1`/`0`, `on`/`off`, `yes`/`no`도 처리합니다. 허용되지 않는 `success` 값은 400입니다. 성공 시 `completed`와 완료 시각을 기록하고 원격 다운로더 작업을 정리하며, 실패 시 `failed`와 오류 메시지를 기록합니다.

<br>
* **Rclone 진행률 보고 (IPC):**

  ```text
  POST /feeder/api/download/progress_update?apikey={APIKEY}
  Content-Type: application/json
  ```

  ```json
  {
    "item_id": 123,
    "action": "update",
    "data": {
      "progress": 50.0,
      "speed_str": "8 MiB/s",
      "download_speed": 8388608,
      "downloaded_bytes": 52428800,
      "total_bytes": 104857600,
      "eta": "6s"
    }
  }
  ```

  `action: clear`는 저장한 진행률을 지웁니다. 이 API는 화면용 진행률만 갱신하며 작업 완료 상태는 릴레이 결과 보고 또는 파이프라인이 처리합니다.

<br>
#### 3) 웹 UI의 작업 제어 요청

FF 웹 UI가 사용하는 AJAX 경로입니다. 웹 UI와 동일한 로그인·접근 설정에서 호출합니다.

```text
POST /feeder/ajax/download/item_action
Content-Type: application/x-www-form-urlencoded
id={TASK_ID}&action={ACTION}
```

| `action` | 동작 |
| :--- | :--- |
| `retry` | 프로필을 다시 적용해 재시도합니다. 선택적인 `profile_name`을 전달할 수 있습니다. 소스 경로·기존 오류에 따라 복구 경로를 선택합니다. |
| `force_complete` | 최종 완료로 기록합니다. 실제 파일을 전송하는 명령은 아닙니다. |
| `force_fail` | 실행 중 Rclone을 중단하고 실패로 기록합니다. 원본 파일은 보존합니다. |
| `delete` | 실행 중 전송·원격 엔진 작업·로컬 소스를 정리하고 `cancelled`로 보존합니다. 취소된 항목을 다시 삭제하면 DB에서 제거합니다. |

---

### 8. 문제 해결 (Troubleshooting)

| 증상 | 원인 및 해결 방법 |
| :--- | :--- |
| **AllDebrid가 엔진 목록에 없음** | 내장 엔진은 qBittorrent/CD2입니다. AllDebrid 확장 스크립트를 `feeder_custom/engines`에 설치하고 로드 오류와 엔진 목록을 확인하세요. 예제 YAML만 추가해서 엔진 구현이 설치되지는 않습니다. |
| **AllDebrid 추가 실패 (`File not available due to no peer`)** | 원격 작업 추가 또는 재시작이 거부되면 다음 우선순위 엔진을 시도할 수 있습니다. 체인 구성과 해당 엔진의 오류 메시지를 확인하세요. |
| **AllDebrid 큐가 가득 차 신규 등록이 안 됨** | `max_active_tasks` 또는 서비스의 큐 제한에 도달하면 `pending`으로 기다립니다. 원격에 이미 존재하는 동일 해시 작업은 연동 대상입니다. |
| **FEED가 최대 100개에서 더 동기화되지 않는 것으로 보임** | 현재 구현에서 API/XML 출력 상한은 DB 동기화 개수를 제한하지 않습니다. 실제 FEED 목록·필터·동기화 로그와 API/XML 각각의 최대 수를 확인하세요. |
| **정적 XML이 생성되지 않음** | FEED 전역 생성 스위치와 개별 `use_rss_file`, 저장 경로·쓰기 권한을 확인하세요. 피드의 개별 강제 생성으로 경로 문제를 점검할 수 있습니다. |
| **XML 항목이 사라졌는데 DB에는 남아 있음** | XML 출력 기간·최대 수와 DB 보관은 독립적입니다. DB 삭제는 각 모듈의 자동 정리 또는 수동 DB 정리 설정을 확인하세요. |
| **RSS 첨부파일이 다른 파일이거나 404/인증 오류** | 현재 생성한 FEED 링크는 `/api/feed/download`와 FEED 항목 ID를 사용합니다. 이전에 배포한 XML은 재생성하고, CRAWL/FEED ID 혼용·첨부파일 인덱스·원본 파일 유무·FF API 인증 설정을 확인하세요. 정적 XML에는 API Key가 들어가지 않습니다. |
| **Cloudflare 403 또는 Selenium 초기화 실패** | 수집기 옵션, 프록시 주소, FlareSolverr/Selenium 원격 주소와 서비스 접근성을 확인하세요. Selenium 모드는 별도 수집 경로이며, 사이트별 로그인·페이지 처리가 필요하면 훅을 구현합니다. |
| **CD2 작업이 목록에서 보이지 않음** | `cd2_max_pages` 기본값은 1이므로 이전 히스토리가 범위 밖에 있을 수 있습니다. 필요한 경우 값을 조정하되 긴 목록의 반복 조회 부하를 고려하세요. 부분 목록은 원격 누락 실패 판정에서 제외합니다. |
| **CD2 gRPC 타임아웃** | 다운로더의 `cd2_timeout`(기본 30초), CD2 주소·포트·토큰과 응답 상태를 확인하세요. CRAWL의 Selenium/요청 타임아웃과 다른 설정입니다. |
| **엔진 작업이 원격 목록 누락으로 실패함** | `missing_timeout_hours` 동안 전체 조회 성공 후에도 계속 매칭되지 않은 경우입니다. 해당 엔진의 실제 작업 ID·해시·원격 삭제 여부를 확인하세요. 자동으로 다음 엔진에 넘기지 않습니다. |
| **`waiting_transporter`에서 대기함** | 프로필의 목적지 타입, 커스텀 이송 스크립트의 ID·파일·로드 로그를 확인하세요. 복구 시 다음 실행에서 다시 이송합니다. 목록에서 수동 완료·실패·삭제도 가능합니다. |
| **동일 제목 작업의 출력 폴더명에 해시가 붙음** | DB에 같은 폴더명 이력이 있으면 적용하는 정상 동작입니다. 실제 파일을 삭제해도 DB 이력이 남아 있으면 이름을 분리합니다. `source_file_name`과 `output_folder_name`을 구분하세요. |
| **Google Drive 업로드 최대 큐 도달로 대기함** | 스테이징 중·직접 업로드를 포함한 업로드 대기/진행·실패 후 남은 로컬 버퍼가 같은 큐를 차지합니다. 기존 작업의 업로드·정리 상태를 확인하세요. 동시 전송 수와 큐 한도는 별개입니다. |
| **Google Drive 이동 실패 / `directory not found`** | 작업에 기록된 MyDrive·공유 드라이브 임시/최종 경로, 리모트와 권한을 확인하세요. `move_failed`는 소스가 있으면 재업로드하고, 소스가 없으면 상태 이력을 남겨 성공 종료합니다. 리모트 잔여 파일은 최종 경로로 이동해 보존합니다. |
| **`move_failed`가 완료 필터·성공 통계에 함께 보임** | 소스 없음으로 성공 종료하면서 원래 이동 실패 이력을 보존한 작업입니다. 완료 시각과 설명, 별도의 원격 파일 복구 결과를 확인하세요. |
| **계정 풀 OFF인데 Google Drive 업로드가 안 됨** | 계정 목록 대신 기본 업로드 리모트·공유 드라이브 ID·Rclone 설정 경로를 확인하세요. 풀 OFF 경로는 공유 드라이브 직행입니다. |
| **24시간 업로드 통계가 실제 성공 용량보다 큼** | 통계는 전송 시도 시 기록한 파일 크기를 합산하므로 실패·재업로드가 포함될 수 있습니다. 완료 작업 용량과 함께 확인하세요. |
| **재시작 후 전송 중 작업이 대기 상태로 바뀜** | 이전 실행의 스테이징·업로드·릴레이 상태를 복구하는 동작입니다. 큐와 로그를 확인한 뒤 다음 실행을 진행하세요. |
