## Feeder 플러그인 매뉴얼

**Feeder**는 웹 크롤링을 통한 토렌트/마그넷 수집부터 맞춤형 RSS 피드 발행, 그리고 멀티 다운로더 엔진과 대용량 클라우드(Google Drive, Rclone, CD2 등)를 아우르는 자동화 다운로드·이송 파이프라인을 제공하는 FlaskFarm(FF) 전용 통합 미디어 관리 플러그인입니다.

과거 SJVA3의 플러그인이었던 `rss2`의 설계 사상을 계승하고 현대화하여, **수집(CRAWL) ➔ 피드(FEED) ➔ 다운로드(DOWNLOAD)** 3대 핵심 모듈로 완전 디커플링하였으며, **Celery 비동기 워커, YAML 기반 통합 스케줄링, 4단계 보안 우회 엔진(`curl_cffi`, FlareSolverr, Remote Selenium), 플러그형 다운로더/트랜스포터 확장 구조, Google Drive SA 풀 15GB 우회 파이프라인, 실시간 SSE 대시보드**를 지원합니다.

---

### 1. 주요 특징

#### 1) 수집 계층 (CRAWL)
* **4단계 지능형 스크래핑 엔진:**
  * **1단계:** TLS/JA3 브라우저 지문 에뮬레이션(`curl_cffi`) 기반 초고속 수집 (HTTP Keep-Alive 세션 풀링).
  * **2단계:** 표준 `requests` 세션 자동 폴백.
  * **3단계:** Cloudflare Turnstile 및 5초 챌린지 대응을 위한 `FlareSolverr` 스마트 우회 (도메인별 토큰 캐싱).
  * **4단계:** 동적 렌더링 및 봇 탐지 우회를 위한 스텔스 `Remote Selenium` 연동 (단일 브라우저 프로세스 영속 세션 유지).
* **프록시 로테이션:** 단일 프록시뿐 아니라 쉼표(`,`) 또는 줄바꿈으로 복수 프록시 등록 시 차단 감지 및 게시판 전환 시 자동 로테이션.
* **마그넷 & ed2k & 첨부파일 다중 지원:** 토렌트 BTIH 마그넷 주소뿐 아니라 ed2k 링크, `.torrent` 파일 파싱을 통한 마그넷 역추출 지원.
* **토렌트 메타데이터 분석:** `torrent_info` 플러그인(m2i API) 또는 `qBittorrent Web API`와 연동하여 마그넷 해시로부터 원본 파일명 및 용량을 선제 분석.
* **커스텀 사이트 훅 (`feeder_custom/sites`):** 단순 정규식/XPath로 해결되지 않는 복잡한 사이트(성인 확인 게이트 클릭, 암호화 링크, 다중 분할 압축 zip/rar 첨부파일 해제 및 인코딩별 텍스트 분석)를 순수 파이썬 코드로 유연하게 확장.

#### 2) 피드 가공 및 발행 계층 (FEED)
* **표준 RSS 2.0 피드 생성:** 태스크별 개별 피드 및 다중 사이트/게시판/외부 RSS 통합 믹싱(Multiplexing) 피드 지원 (동일 마그넷 중복 자동 단일화).
* **외부 원격 RSS 소스 통합:** 내부 크롤러 수집 게시판뿐만 아니라 외부 RSS 피드 URL을 소스로 지정하여 필터링 후 재발행 가능.
* **Flexget 규격 정규식 및 화질 필터:** 제목·파일명 기반 해상도 판별(`quality: 2160p+`, `>=1080p`, `720p-1080p` 등) 및 정규식(`regexp: reject/accept/reject_excluding/accept_all`) 엔진 내장.
* **외부 배포용 무인증 RSS XML 파일 생성:** API Key 노출 없는 정적 XML 자동 생성, 보관 기간(기본 14일) 및 최대 항목 수 자동 정리(Retention).

#### 3) 다운로드 및 클라우드 파이프라인 (DOWNLOAD)
* **멀티 다운로더 우선순위 체인 (Priority Chain):**
  * 여러 다운로더 엔진을 순위별로 연결하여 앞선 엔진이 지연(Stalled Timeout)되거나 실패할 경우 차순위 엔진으로 자동 폴백(Fallback).
  * 지원 기본 엔진: **qBittorrent**(로컬 클라이언트), **CloudDrive2 (CD2)**(gRPC 기반 115 오프라인 다운로드), **AllDebrid**(오프라인 고속 캐시 수득 및 WebDAV 연동).
  * 파이썬 스크립트 기반 커스텀 다운로더 엔진(`feeder_custom/engines`) 동적 로드 지원.
* **유연한 이송 핸들러 (Transporter):**
  * **단순 경로 이동 (`local`):** 다운로드 완료 후 로컬 디스크/엔진 루트 내 최종 정리 및 중복 해시 충돌 방지 폴더링.
  * **일반 Rclone 업로드 (`rclone_simple`):** 로컬 스테이징 또는 원격 다이렉트 스트리밍을 통한 원격 리모트 업로드 및 최종 이동.
  * **Google Drive 계정 풀 (`gdrive_rotation`):** 대규모 SA 계정 풀 기반 업로드 및 쿼터 관리.
  * 커스텀 이송 핸들러(`feeder_custom/transporters`) 동적 로드 지원.
* **로컬 스테이징 vs 다이렉트 전송 모드:**
  * 로컬 디스크 공간 보호를 위한 최대 보관 수 제어 및 병렬 다운로드 워커 지원.
  * 로컬 디스크를 전혀 거치지 않는 다이렉트(on-the-fly) 스트리밍 전송 지원.
* **Google Workspace Impersonate & 15GB MyDrive 우회 아키텍처:**
  * 내 드라이브 임계값 설정(기본 14GB) 이하 파일은 개인 SA의 MyDrive로 1차 업로드 후, 공유 드라이브 임시로 서버사이드 이동(`move`), 이후 공유 드라이브 완료 폴더로 원자적 부모 폴더 포인터 변경(`chpar`)을 수행하여 일일 750GB 제한 우회.
  * 24시간 안전 업로드 한도(개인 700GB, 공유 드라이브 3TB) 및 403 쿼터 초과 자동 감지/차단 로테이션.
* **원격 릴레이 시스템 (Colab / VPS 무트래픽 워커):**
  * 로컬 대역폭을 소모하지 않고 원격 헤드리스 노드가 `/api/download/relay_claim` 및 `/api/download/relay_report`를 통해 클라우드 간 다이렉트 전송 수행.
* **과거 이력 흡수 (마이그레이션):**
  * 기존 오프라인 다운로더의 SQLite DB(`.db`) 또는 마그넷 텍스트 목록을 불러와 완료(`completed`) 이력으로 즉시 등록하여 재다운로드 방지.

#### 4) 실시간 대시보드 및 운영 편의성
* **SSE(Server-Sent Events) 실시간 대시보드:** Rclone `--stats-one-line` IPC 텔레메트리를 파싱하여 큐 화면 새로고침 없이 전송 속도, 진행률(ETA), 활성 작업 게이지 바 실시간 노출.
* **24시간 업로드 통계:** 30초 TTL 메모리 캐시를 적용하여 DB 부하 없이 작업 큐 및 계정 풀 페이지에서 24시간 총 업로드량 실시간 확인.
* **미등록 고아 작업 큐 흡수 (Adopt Orphan):** 엔진(AllDebrid, CD2 등)에는 파일이 남아있으나 Feeder DB에 등록되지 않은 잔여 작업을 원클릭으로 감지하여 프로필을 지정해 파이프라인으로 흡수.
* **안정적인 동시성 제어:** SQLite WAL 저널 모드 및 60초 `busy_timeout` 강제 적용, Celery 워커 간 원자적 DB 런타임 락(`_runtime_lock`), 대용량 Rclone 실행 중 DB 락 해제(Zero-Lock).
* **내장 Monokai Ace Editor:** 사이트 JSON 규칙, 파이썬 커스텀 스크립트 작성/편집, JSON Prettier 자동 정렬, 창 크기 조절 및 전체화면 지원.

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
    └── 다운로드 리스트 (전체 다운로드 완료/실패 이력 검색, 재시도, 완료 처리)
```

| 모듈 | 페이지/탭 | 주요 기능 |
| :--- | :--- | :--- |
| **CRAWL** | **수집 리스트** | 수집된 게시글 목록 확인, 마그넷/ed2k 복사, 첨부파일 직접 다운로드, 토렌트 메타데이터 조회, 리스트에서 원하는 프로필/엔진을 지정하여 즉시 다운로드 큐 등록. |
| | **설정 및 관리** | 크롤링 주기, 최대 페이지, 프록시, FlareSolverr, Selenium, 토렌트 정보 취득 설정, 사이트 JSON 규칙 관리, 수집기(Crawler)별 게시판 및 스케줄 빈도 설정. |
| **FEED** | **피드 리스트** | 생성된 피드별 발행 결과 조회, 마그넷 복사, 다운로드 큐 직접 추가. |
| | **설정 및 관리** | 전역 화질/정규식 필터, 피드(Feed)별 소스(내부 수집기+외부 RSS) 조합, Flexget 화질 및 정규식 필터, 무인증 공유용 RSS XML 파일 생성 및 보관 기간 설정. |
| **DOWNLOAD** | **다운로드 큐 및 대시보드** | 활성 작업 실시간 모니터링(대기, 엔진 다운로드, 로컬 스테이징, GDrive 업로드), 실시간 속도/진행률/24시간 업로드 배지, 작업 강제완료/재시도/삭제, 고아 작업 큐 흡수. |
| | **다운로드 리스트** | 최종 완료 및 실패 이력 검색, 실패 작업 프로필 변경 재시도, 이력 삭제. |
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
    "TITLE_XPATH": "./text()",
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
| `DELAY` | 선택 | `2.0` | 페이지 요청 간 대기 시간(초). 미입력 시 전역 설정값이 적용됩니다. |
| `USER_AGENT` | 선택 | 기본 크롬 UA | 사이트 요청 헤더 및 Selenium 드라이버에 적용할 User-Agent. |
| `COOKIE` | 선택 | - | 고정 세션 쿠키 문자열 (`name=val; name2=val2`). |
| `BOARD_URL_RULE` | 선택 | 그누보드 기본 | 기본 게시판 목록 URL 템플릿. `{URL}`, `{BOARD_NAME}`, `{PAGE}` 치환자 지원. |
| `SUBCAT_URL_RULE` | 선택 | - | 분류 필터(서브 카테고리) URL 템플릿. `{URL}`, `{BOARD_NAME}`, `{FID}`, `{SUBCAT}`, `{TYPEID}`, `{PAGE}` 치환자 지원. |
| `XPATH_LIST_TAG` | **필수** | - | 목록 페이지에서 각 게시글의 링크 태그(`<a>`)를 순회 추출하기 위한 XPath 규칙 딕셔너리. |
| `ID_REGEX` | 선택 | 표준 정규식 | 상세 페이지 URL에서 고유 게시글 ID를 추출하는 정규식 (`(?P<id>...)` 그룹 필수). 문자열 또는 문자열 배열 지원. |
| `SELENIUM_WAIT_TAG` | 선택 | `body` | Selenium으로 목록 로드 시 대기할 요소의 XPath. |
| `SELENIUM_DETAIL_WAIT_TAG` | 선택 | `body` | Selenium으로 상세 페이지 로드 시 대기할 요소의 XPath. |
| `SELENIUM_TIMEOUT` | 선택 | 기본 설정값 | 해당 사이트에만 적용할 Selenium 로딩 타임아웃(초). |
| `MAGNET_REGEX` | 선택 | 자동 정규식 | 본문에서 마그넷 주소를 추출하기 위한 정규식 규칙 `[매칭정규식, 치환포맷]`. |
| `DOWNLOAD_REGEX` | 선택 | - | 첨부파일 다운로드 주소와 파일명을 추출하는 정규식 (`(?P<url>...)`, `(?P<filename>...)` 그룹 필수). |
| `EXTRA` | 선택 | `[]` | 사이트별 특수 플래그 배열 (`USE_PROXY`, `USE_FLARESOLVERR`, `USE_SELENIUM`, `USE_TORRENT_INFO`, `USING_POST_CHAR_ID`, `ONLY_FILE`, `MAGNET_ONLY_ONE_LAST`). |
| `DESCRIPTION` | 선택 | - | 웹 UI 사이트 목록 테이블에 표시할 안내 문구. |

---

### 4. 통합 설정 파일 (`feeder_settings.yaml`) 가이드

플러그인 상단의 **YAML 편집** 버튼을 누르거나 `{path_data}/db/feeder_settings.yaml` 파일을 수정하여 모든 수집기, 피드, 다운로더 엔진, 다운로드 프로필, 구글 드라이브 계정 풀을 단일 파일에서 선언적으로 관리할 수 있습니다.

<br>
#### 전체 YAML 구조 예시

```yaml
GLOBAL:
  quality: 1080p+
  append_hash_on_conflict: true
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

  - name: ad
    engine_type: alldebrid
    enabled: true
    apikey: 'YOUR_ALLDEBRID_API_KEY'
    remote_name: 'ad'
    rclone_base_path: 'magnets'
    max_active_tasks: 20
    stalled_timeout_hours: 6

  - name: cd2
    engine_type: cd2
    enabled: true
    cd2_addr: '127.0.0.1'
    cd2_port: 19798
    cd2_token: 'YOUR_CD2_JWT_TOKEN'
    cd2_virtual_path: '115open/云下载'
    cd2_mount_path: '/mnt/cd2/115open/云下载'
    stalled_timeout_hours: 24

DOWNLOAD_PROFILES:
  - name: default_profile
    sync_hours: 72
    feeds:
      - '*'
    priority_chain:
      - ad
      - cd2
      - qbit_main
    append_hash_on_conflict: true
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
        complete_path: 'uploads/default'

GDRIVE_ACCOUNTS:
  - username: 'user01@example.com'
    mydrive_rclone_id: '0AIoBxxxxxx_FolderID1'
  - username: 'user02@example.com'
    mydrive_rclone_id: '0AIoByyyyyy_FolderID2'
    remote_name: 'gdrive_user02'
```

#### YAML 섹션별 필드 설명

##### `DOWNLOADERS` (다운로더 엔진 설정)
* `name`: 프로필 우선순위 체인에서 참조할 고유 영문 식별명.
* `engine_type`: 엔진 타입 (`qbittorrent`, `cd2`, `alldebrid` 또는 `feeder_custom/engines`에 등록된 커스텀 엔진 ID).
* `enabled`: 엔진 활성화 여부.
* `stalled_timeout_hours`: 해당 시간 동안 완료되지 않고 지연될 경우 다음 순위 엔진으로 자동 폴백할 시간(0 입력 시 무제한 대기).
* `max_active_tasks`: 해당 엔진의 동시 진행 큐 제한 (0은 무제한).
* 기타 필드는 각 엔진의 `CONFIG_SCHEMA`에 따라 동적으로 설정됩니다.

##### `DOWNLOAD_PROFILES` (다운로드 라우팅 프로필)
* `name`: 프로필 식별명.
* `sync_hours`: 최근 N시간 이내에 수집된 게시글만 다운로드 대상으로 탐색 (기본값: 72시간).
* `feeds`: 해당 프로필을 적용할 피드 목록 (`['*']`는 전체 적용).
* `priority_chain`: 순서대로 시도할 다운로더 엔진 식별명 배열 (`['ad', 'cd2', 'qbit_main']`).
* `append_hash_on_conflict`: 동일한 폴더명이 이미 존재할 때 해시(`_[hash]`)를 붙여 충돌을 방지할지 여부.
* `destination`: 기본 이송 핸들러 설정 딕셔너리 (`type: local | rclone_simple | gdrive_rotation`).
  * `use_local_staging`: 로컬 디스크로 임시 다운로드 후 전송할지(`true`), 원격 소스에서 목적지로 다이렉트(on-the-fly) 스트리밍할지(`false`) 여부.
  * `staging_path`: 로컬 스테이징 시 사용할 서브 디렉터리 경로.
  * `upload_path`: 클라우드 임시 수신 경로 (기본: `incoming/{profile_name}`).
  * `complete_path`: 최종 보관 경로 (기본: `uploads/{profile_name}`).
  * `shared_drive_id`: 목적지 공유 드라이브 ID.
* `destination_by_engine`: 특정 엔진 전용 목적지 오버라이드 딕셔너리 (예: 로컬 클라이언트인 `qbit_main`은 클라우드로 올리지 않고 로컬 완료 경로로 단순 이동).

##### `GDRIVE_ACCOUNTS` (Google Drive 계정 풀)
* `username`: 계정 식별자 (위임 사용 시 `--drive-impersonate` 대상 이메일 주소).
* `mydrive_rclone_id`: 1차 15GB 우회 업로드를 수행할 MyDrive 폴더의 Rclone ID.
* `remote_name`: 비위임(독립 리모트) 사용 시 `rclone.conf` 상의 개별 리모트 이름 (선택 사항).

---

### 5. 다운로드 파이프라인 및 클라우드 연동 가이드

Feeder는 대용량 트래픽 및 클라우드 서비스의 제한(Google Drive 일일 750GB 쿼터 등)을 우회하고 효율적으로 라이브러리를 구축할 수 있도록 강력한 파이프라인을 내장하고 있습니다.

```text
[피드 아이템 동기화 (sync_feed_items)]
       │
       ▼
[대기열 (pending)] ──> 우선순위 체인 1순위 엔진 선택
       │
       ├─ [엔진 다운로드 (downloading)] ──(타임아웃 발생 시)──> 차순위 엔진으로 자동 폴백
       │       │
       │       ▼ (다운로드 완료 감지)
       │
       ├─ [로컬 스테이징 활성화 시]
       │       ▼
       │   [로컬 스테이징 (local_staging)] ──> Rclone으로 로컬 임시 버퍼 수신
       │       ▼
       │   [다운로드 완료 (downloaded)]
       │
       └─ [다이렉트 스트리밍 모드 시 (로컬 스테이징 OFF)]
               ▼
           [다운로드 완료 (downloaded)] (원격 소스 경로 유지)
                   │
                   ▼ [이송 라우팅 (route_completed_downloads)]
       ┌───────────┴───────────────────────────────┐
       ▼                                           ▼
[단순 경로 이동 (local)]              [Google Drive 계정 풀 (gdrive_rotation)]
       │                                           │
       ▼                                           ▼
[최종 완료 (completed)]                    [업로드 대기 (pending_upload)]
                                                   │
                                                   ▼ [execute_upload (Zero-Lock)]
                                           1. 소스 ➔ MyDrive 임시 (incoming) 복사
                                           2. 3초 안정화 대기 (Eventual Consistency 방어)
                                           3. MyDrive 임시 ➔ 공유 드라이브 임시 (Across move)
                                           4. 공유 드라이브 임시 ➔ 공유 드라이브 완료 (chpar 원자적 이동)
                                           5. MyDrive 휴지통 비우기 (cleanup)
                                                   │
                                                   ▼
                                           [최종 완료 (completed)]
```

#### 1) Google Drive 15GB MyDrive 바이패스 및 2단계 이동 메커니즘
* **목적:** Google Workspace 공유 드라이브의 일일 750GB 업로드 한도를 우회하여 수 TB를 연속 업로드.
* **동작 순서:**
  1. 내 드라이브 임계값 설정(기본 14GB) 이하의 파일은 SA(서비스 계정) 풀에서 24시간 사용량이 가장 적은 계정의 15GB MyDrive 임시 폴더(`incoming/...`)로 1차 업로드.
  2. 파일시스템 인덱싱 안정화를 위해 3초 대기.
  3. `rclone move`를 통해 MyDrive 임시 폴더에서 목적지 공유 드라이브의 임시 폴더(`incoming/...`)로 서버사이드 이동.
  4. 같은 공유 드라이브 내에서 `rclone backend chpar`를 호출하여 완료 폴더(`uploads/...`)로 부모 포인터만 즉시 원자적 교체.
  5. MyDrive의 휴지통을 비워 15GB 공간을 즉시 원상 복구.
* **리모트 분리 원칙:**
  - 내 드라이브를 경유할 때는 전송부터 최종 이동까지 **`내 드라이브 SA 기본 리모트`** 단 하나만 일관되게 사용합니다.
  - 내 드라이브 임계값 설정 크기를 초과하는 대용량 파일이 공유 드라이브로 직행할 때만 **`기본 목적지 공유 드라이브 리모트`**를 단독 사용합니다.(도메인 위임 미사용시)

#### 2) Zero-Lock Rclone 전송 및 통계 브로드캐스트
* 장시간 지속되는 대용량 Rclone 업로드 중 SQLite DB가 잠기지 않도록, 서브프로세스 기동 직전 `db.session.remove()`로 세션을 반환합니다.
* Rclone의 `--stats-one-line` 출력을 실시간 정규식으로 파싱하여 SSE 스트림으로 UI에 브로드캐스트합니다.
* 24시간 총 업로드량 통계는 **30초 TTL 메모리 캐시**가 적용되어 DB 쿼리 부하 없이 실시간 배지에 반영됩니다.

---

### 6. 커스텀 스크립트 개발 가이드 (`feeder_custom`)

Feeder의 모든 핵심 계층은 `{path_data}/db/feeder_custom/` 디렉터리에 파이썬 스크립트를 추가하여 자유롭게 확장할 수 있습니다.

<br>
#### 1) 커스텀 사이트 훅 (`feeder_custom/sites/site_{NAME}.py`)
사이트 수집 라이프사이클에 개입하여 로그인 세션 유지, 챌린지 해결, 본문 내 복합 첨부파일 해제 등을 수행합니다.

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

    CONFIG_SCHEMA = [
        {"name": "api_url", "label": "API 주소", "type": "text", "default": "http://127.0.0.1:9091"},
        {"name": "api_key", "label": "API Key", "type": "password"}
    ]

    def add_magnet(self, link: str, title: str = None, upload_path: str = None) -> tuple[bool, str, str]:
        """마그넷 추가 (성공여부, 작업ID, 에러메시지) 반환"""
        return True, "task_12345", ""

    def get_status(self, task_ids: list[str] = None) -> tuple[list[dict], str]:
        """
        작업 상태 목록 조회
        반환 규격: [{'task_id': str, 'hash': str, 'status': 'completed'|'downloading'|'error', 'filename': str, 'file_size': int, 'source_path': str}]
        """
        return [], ""

    def delete_task(self, task_id: str) -> bool:
        """작업 삭제"""
        return True

    def restart_task(self, task_id: str) -> bool:
        """에러/중단 작업 재시작 (선택 사항)"""
        return False

    def test_connection(self) -> tuple[bool, str]:
        """연결 테스트"""
        return True, "연결 성공"
```

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
        return True, "completed", "이송 완료"
```

---

### 7. API 엔드포인트 명세

#### 1) 피드 및 첨부파일 API
* **RSS 피드 발행 (XML 스트림):**
  ```text
  GET /feeder/api/feed/rss?name={FEED_NAME}&apikey={APIKEY}
  GET /feeder/api/feed/rss?id={FEED_ID}&apikey={APIKEY}
  ```
* **첨부파일 직접 다운로드 스트림:**
  ```text
  GET /feeder/api/crawl/download?id={POST_ID}_{FILE_INDEX}&apikey={APIKEY}
  ```
* **사이트 규칙 원격 업데이트:**
  ```text
  POST /feeder/api/crawl/site_update
  (Body: content={BASE64_ENCODED_JSON})
  ```

#### 2) 다운로드 및 릴레이 API
* **대시보드 실시간 SSE 스트림:**
  ```text
  GET /feeder/api/download/sse?apikey={APIKEY}
  ```
  *(반환 데이터: `counts`, `total_upload_24h`, `active_list` 실시간 브로드캐스트)*
* **원격 릴레이 작업 선점 (Claim Task):**
  ```text
  POST /feeder/api/download/relay_claim
  (Body: apikey={APIKEY})
  ```
  *(반환 데이터: 대기 중인 `pending_relay` 작업 정보, Rclone 설정 및 SA 자격 증명 번들)*
* **원격 릴레이 작업 완료 보고 (Report Task):**
  ```text
  POST /feeder/api/download/relay_report
  (Body: apikey={APIKEY}, id={TASK_ID}, success=true/false, bytes_transferred={BYTES})
  ```
* **Rclone 전송률 실시간 보고 (IPC):**
  ```text
  POST /feeder/api/download/progress_update
  (Body: apikey={APIKEY}, item_id={ID}, data={PROGRESS_DICT}, action='update'|'clear')
  ```

---

### 8. 문제 해결 (Troubleshooting)

| 증상 | 원인 및 해결 방법 |
| :--- | :--- |
| **AllDebrid 추가 실패 (`File not available due to no peer`)** | 피어가 없는 토렌트입니다. Feeder는 재시작 거부 감지 시 우선순위 체인의 다음 엔진(CD2, qBittorrent 등)으로 자동 폴백합니다. 체인에 대체 엔진을 등록해 두세요. |
| **Google Drive 업로드 중 `directory not found`** | 내 드라이브 임시(`incoming`)에서 공유 드라이브 임시로 이동(`move`) 후 완료 폴더로 `chpar`하는 2단계 이동이 진행됩니다. 프로필의 `upload_path`와 `complete_path` 설정이 올바른지 확인하세요. |
| **계정 풀 사용량 분산** | 계정 풀은 24시간 사용량이 가장 적은 계정부터 고르게 할당(Least-Used)하므로 개별 수치가 낮게 보일 수 있습니다. 계정 풀 상단의 `24시간 총 업로드` 카드를 통해 전체 합산 수치를 확인하세요. |
| **Cloudflare 403 차단 발생** | FlareSolverr 및 Selenium 원격 드라이버가 동일한 프록시(예: Gluetun)를 경유하도록 설정되어 있는지 확인하세요. IP가 일치해야 발급된 `cf_clearance` 토큰이 유효합니다. |
| **CD2 오프라인 작업 목록 타임아웃** | 115 API 응답 지연 시 발생할 수 있습니다. 수집기 및 다운로더 설정에서 CD2 gRPC 타임아웃을 기본 30초 이상으로 넉넉히 지정하세요. |
