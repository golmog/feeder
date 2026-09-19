## Feeder 플러그인 매뉴얼

**Feeder**는 다양한 토렌트 게시판 사이트를 주기적으로 크롤링하여 마그넷(Magnet) 주소 및 토렌트 첨부파일을 자동 수집하고, 다운로더(qBittorrent, Transmission, Sonarr, Radarr 등)에서 구독 가능한 **표준 RSS 피드(showRSS 규격 호환)**로 발행하는 FlaskFarm(FF) 전용 플러그인입니다.

과거 SJVA3의 핵심 플러그인이었던 `rss2`의 설계 사상을 계승하고 현대화하여, **Celery 비동기 워커, YAML 기반 스케줄링, 4단계 보안 우회 엔진(`curl_cffi`, FlareSolverr, Remote Selenium), 커스텀 Python 스크립트 훅**을 통합했습니다.

---

### 1. 주요 특징

* **표준 RSS 2.0 피드 생성:** 게시판별 개별 피드 및 다중 사이트 중복 제거 통합 그룹 피드 지원.
* **4단계 지능형 스크래핑 엔진:**
  * **1단계:** TLS/JA3 브라우저 지문 에뮬레이션(`curl_cffi`) 기반 초고속 수집 (HTTP Keep-Alive 세션 풀링 지원).
  * **2단계:** 표준 `requests` 세션 자동 폴백.
  * **3단계:** Cloudflare Turnstile / 5초 챌린지 대응을 위한 `FlareSolverr` 스마트 우회 (도메인별 세션 재사용).
  * **4단계:** 자바스크립트 완전 렌더링 및 봇 탐지 우회 스텔스 `Remote Selenium` 연동 (단일 세션 재사용).
* **커스텀 사이트 훅 (`data/db/feeder_custom`):** 정규식/XPath만으로 수집이 불가능한 복잡한 사이트(성인 인증 게이트, 암호화 마그넷, 다중 분할 압축 zip/rar 첨부파일 분석 등)를 순수 파이썬 코드로 유연하게 확장.
* **토렌트 메타데이터 분석:** `torrent_info` 플러그인(m2i API) 또는 `qBittorrent Web API`와 연동하여 마그넷 해시로부터 원본 파일명 및 용량을 획득해 피드 제목으로 변환.
* **강력한 내장 웹 에디터:** Monokai 다크 테마, JSON Prettier 자동 정렬, 실시간 창 크기 조절(드래그 및 최대화), 자동 줄바꿈(Word Wrap) 토글을 지원하는 Ace Editor 내장.
* **임시 파일 자동 관리:** 다운로드 임시 파일 및 디버그 스크린샷을 플랫폼 임시 경로(`{path_data}/tmp/feeder`)에서 격리 처리하여 FlaskFarm 재시작 시 자동 청소.

---

### 2. 메뉴 및 UI 구성

| 메뉴 탭 | 설명 |
| :--- | :--- |
| **토렌트 리스트** | 수집된 게시글 목록, 마그넷 주소 복사, 첨부파일 직접 다운로드, PikPak 전송, 토렌트 메타데이터 모달 조회 지원. |
| **기본 설정** | 수집 주기, 최대 탐색 페이지, HTTP 프록시, FlareSolverr, Remote Selenium(타임아웃 설정 포함), 토렌트 메타데이터 취득 방식 설정. |
| **사이트 관리** | 사이트 규칙(JSON) 등록/수정/삭제, 게시판 단위 수집 테스트, 커스텀 Python 스크립트 관리. |
| **그룹화** | 여러 사이트의 서로 다른 게시판들을 하나의 그룹으로 묶고, 동일 마그넷 중복을 자동 제거한 통합 RSS 피드 발급. |
| **스케줄링 (YAML)** | 수집 대상 사이트 및 게시판 등록, 개별 주기(Interval), 프록시/FlareSolverr/Selenium 활성 여부 제어. |
| **자동 & DB정리** | Celery 스케줄러 자동 등록, 주기적 DB 자동 정리(Retention) 및 SQLite VACUUM 공간 회수. |

---

### 3. 사이트 설정 규칙 (JSON) 작성 가이드

사이트 관리 탭의 **사이트 직접 추가** 또는 **규칙 수정**에서 사용하는 사이트별 설정 스키마입니다.

<br>
#### 전체 JSON 구조 예시

```json
{
  "NAME": "example_site",
  "TORRENT_SITE_URL": "https://example.com",
  "DELAY": 1.5,
  "BOARD_URL_RULE": "{URL}/bbs/board.php?bo_table={BOARD_NAME}&page={PAGE}",
  "SUBCAT_URL_RULE": "{URL}/forum.php?mod=forumdisplay&fid={BOARD_NAME}&filter=typeid&typeid={SUBCAT}&page={PAGE}",
  "XPATH_LIST_TAG": {
    "XPATH": "//tbody/tr[%s]//td[2]//a",
    "INDEX_START": 1,
    "INDEX_STEP": 1,
    "TITLE_REGEX": "(?P<title>.*)"
  },
  "ID_REGEX": "wr_id=(?P<id>\\d+)",
  "SELENIUM_WAIT_TAG": "//tbody/tr[1]",
  "SELENIUM_DETAIL_WAIT_TAG": "//div[@class='view-content']",
  "MAGNET_REGAX": [
    "magnet:\\?xt=urn:btih:([a-zA-Z0-9]+)",
    "magnet:?xt=urn:btih:%s"
  ],
  "DOWNLOAD_REGEX": "href=\"(?P<url>[^\"]+download\\.php[^\"]+)\".*?>(?P<filename>[^<]+\\.torrent)<",
  "EXTRA": [
    "USE_FLARESOLVERR",
    "USE_SELENIUM"
  ],
  "DESCRIPTION": [
    "게시판 ID 입력 안내:",
    "- 한국 영화: movie_kor",
    "- 예능 프로그램: ent",
    "- 서브 카테고리(분류 필터) 수집 시: fid_typeid (예: 166_875)"
  ]
}
```

#### 주요 설정 필드 상세 설명

| 필드명 | 필수 여부 | 기본값 | 설명 |
| :--- | :---: | :---: | :--- |
| `NAME` | **필수** | - | 사이트를 식별하는 고유 이름 (소문자 영문 권장). 커스텀 스크립트와 1:1 매핑 키로 사용됩니다. |
| `TORRENT_SITE_URL` | **필수** | - | 사이트의 기본 도메인 주소 (끝에 `/` 제외). |
| `DELAY` | 선택 | `기본 설정값` | 해당 사이트 크롤링 시 상세 페이지 요청 간 대기 시간(초). 사이트 부하 및 차단 방지에 필수적입니다. |
| `BOARD_URL_RULE` | 선택 | 그누보드 기본 | 기본 게시판 목록 페이지 URL 템플릿. `{URL}`, `{BOARD_NAME}`, `{PAGE}` 치환자를 지원합니다. |
| `SUBCAT_URL_RULE` | 선택 | - | 게시판 내에 서브 카테고리(분류 필터)가 존재하여 기본 주소와 체계가 달라지는 경우 사용되는 URL 템플릿. `{URL}`, `{BOARD_NAME}`(또는 `{FID}`), `{SUBCAT}`(또는 `{TYPEID}`), `{PAGE}` 치환자를 지원합니다. |
| `XPATH_LIST_TAG` | **필수** | - | 목록 페이지에서 각 게시글의 링크(`<a>`)를 순회 추출하는 XPath 규칙 딕셔너리입니다. |
| `ID_REGEX` | 선택 | 자동 정규식 | 상세 페이지 URL에서 고유 게시글 ID를 추출하는 정규식 (`(?P<id>...)` 명명 그룹 필수). |
| `SELENIUM_WAIT_TAG` | 선택 | `body` | 셀레니움으로 **목록 페이지** 로드 시 렌더링 완료를 판정할 XPath 대기 태그. |
| `SELENIUM_DETAIL_WAIT_TAG` | 선택 | `body` | 셀레니움으로 **본문 상세 페이지** 로드 시 렌더링 완료를 판정할 XPath 대기 태그. |
| `SELENIUM_TIMEOUT` | 선택 | `기본 설정값` | 해당 사이트에만 별도로 적용할 셀레니움 타임아웃(초). 미지정 시 기본 설정의 타임아웃이 적용됩니다. |
| `MAGNET_REGAX` | 선택 | 자동 탐색 | 본문에서 마그넷 해시를 정규식으로 추출할 때 사용 `[정규식, 조립템플릿]`. |
| `DOWNLOAD_REGEX` | 선택 | - | 첨부파일 다운로드 주소와 파일명을 추출하는 정규식 (`(?P<url>...)`, `(?P<filename>...)` 필수). |
| `COOKIE` | 선택 | - | 로그인 또는 본인인증이 필요한 사이트에 고정 전달할 HTTP Cookie 문자열. |
| `EXTRA` | 선택 | `[]` | 사이트별 특수 동작 플래그 배열 (아래 표 참조). |
| `DESCRIPTION` | 선택 | - | UI 사이트 목록 테이블에 표시할 안내 문구 (문자열 또는 문자열 배열). |

<br>
##### 서브 카테고리(Subcategory) 지원 및 동작 규칙

Discuz! 포럼(색화당 등)이나 일부 토렌트 사이트는 동일 게시판 내에서 특정 서브 카테고리(`typeid` 등)를 선택할 경우 정적 URL 구조가 아닌 동적 쿼리스트링 구조로 변경됩니다. Feeder는 이를 플랫폼 차원에서 완벽히 지원합니다.

* **수집 테스트 창에서 입력:**
  * 기본 게시판 테스트: `103`, `2_2`, `movie_kor` (언더스코어 `_`는 일반 게시판 ID의 고유 문자로 온전히 보존되어 오작동하지 않습니다.)
  * 서브 카테고리 테스트: **`166:875`** (콜론 `:`을 구분자로 사용하여 게시판 ID `166`과 서브 카테고리 `875`를 자동 분리 후 `SUBCAT_URL_RULE`로 조합)
* **스케줄링 등록 시:**
  * `게시판 추가/수정` 모달에서 `게시판 ID`(`166`)와 `서브 카테고리 ID`(`875`)를 각각 입력하여 개별 등록 가능.
* **DB 격리 및 독립 피드 발행:**
  * 서브 카테고리가 지정된 게시판은 내부적으로 `166:875`라는 고유 복합 키로 관리되어, 전체 게시판 수집 데이터와 중복되지 않고 독립된 RSS 피드로 발행됩니다.

##### `EXTRA` 플래그 종류

* `"USE_FLARESOLVERR"`: Cloudflare 차단 사이트 수집 시 FlareSolverr 세션을 경유하여 챌린지를 우회합니다.
* `"USE_SELENIUM"`: 목록 및 본문 페이지를 원격 Selenium 드라이버를 통해 동적 렌더링합니다. (사이트 관리 배지 클릭으로도 ON/OFF 가능)
* `"USING_BOARD_CHAR_ID"`: 게시물 고유 ID가 숫자가 아닌 영문/해시 등 문자열 형태인 경우 적용합니다.
* `"ONLY_FILE"`: 본문에 마그넷이 없고 `.torrent` 첨부파일만 존재하는 게시판도 수집 대상으로 허용합니다.
* `"MAGNET_ONLY_ONE_LAST"`: 상세 페이지에 마그넷이 여러 개 존재할 때 가장 마지막 마그넷 1개만 수집합니다.

---

### 4. 커스텀 사이트 훅 스크립트 개발 가이드

정적 HTML 파싱만으로 해결되지 않는 복잡한 사이트는 파이썬 스크립트를 통해 크롤링 라이프사이클에 직접 개입할 수 있습니다.

* **저장 위치:** `{path_data}/db/feeder_custom/`
* **파일명 규칙:** `site_{식별자}.py` (예: `site_sehuatang.py`)
* **동작 원리:** 플러그인이 로드될 때 해당 디렉터리의 `.py` 파일들을 동적으로 스캔하여, 사이트 `NAME`과 훅 클래스의 `SITE_NAME`이 일치하면 자동으로 크롤링 훅을 바인딩합니다.

#### 훅 클래스 기본 구조

```python
# -*- coding: utf-8 -*-
"""
커스텀 사이트 훅 스켈레톤
"""
try:
    from feeder.setup import logger, P
except Exception:
    import logging
    logger = logging.getLogger("feeder")
    P = None

class CustomSiteHook:
    # 사이트 식별명 (site_info의 'NAME'과 대소문자 무관 일치해야 함)
    SITE_NAME = "custom_site"

    # 스크립트 최초 저장 시 사이트 관리 목록에 자동 등록될 기본 템플릿 JSON
    DEFAULT_SITE_INFO = {
        "NAME": "custom_site",
        "TORRENT_SITE_URL": "https://example.com",
        "DELAY": 1.5,
        "BOARD_URL_RULE": "{URL}/board/{BOARD_NAME}?page={PAGE}",
        "XPATH_LIST_TAG": {
            "XPATH": "//div[@class='list-item']//a",
            "INDEX_START": 1,
            "INDEX_STEP": 1
        },
        "ID_REGEX": "/view/(?P<id>\\d+)",
        "EXTRA": [
            "USE_SELENIUM"
        ],
        "SELENIUM_WAIT_TAG": "//div[@class='list-item']",
        "SELENIUM_DETAIL_WAIT_TAG": "//div[@class='post-content']",
        "DESCRIPTION": [
            "커스텀 훅 연동 사이트"
        ]
    }

    @classmethod
    def on_init_session(cls, site_info: dict, scheduler_cfg) -> None:
        """
        크롤링 시작 전 1회 호출
        - FlareSolverr를 통한 Cloudflare clearance 토큰 사전 발급
        - Selenium 기동, 성인 인증 버튼(.enter-btn 등) 통과
        - 세션 쿠키 획득 후 ScraperUtil에 브라우저 드라이버 바인딩 (세션 유지)
        """
        pass

    @classmethod
    def on_page_loaded(cls, driver, url: str) -> None:
        """
        Selenium으로 페이지(목록 또는 본문)를 로드한 직후 매번 호출
        - 돌발적으로 나타나는 팝업 창, 연령 확인 레이어 클릭 처리
        """
        pass

    @classmethod
    def on_extract_detail(cls, detail_html: str, item: dict, site_info: dict, scheduler_cfg) -> list[str]:
        """
        상세 페이지 HTML 수신 후 호출
        - 본문 텍스트 내 마그넷 / ed2k 정규식 추출
        - 권한 필요 첨부파일 다운로드 (.torrent, .txt, .zip, .rar)
        - 다중 분할 압축 해제 및 텍스트 파일(GBK/UTF-8) 재귀 분석
        - 최종 마그넷 링크 리스트(list[str]) 반환
        """
        return []
```

#### 실전 예제: 색화당(`site_sehuatang.py`) 동작 메커니즘

1. **`on_init_session`:**
   * FlareSolverr API를 호출하여 Turnstile 챌린지 통과 쿠키(`cf_clearance`)와 User-Agent를 획득.
   * 원격 Selenium을 시작하고 스텔스 옵션(`AutomationControlled` 비활성화, `navigator.webdriver = undefined` CDP 주입)을 적용한 뒤 쿠키를 주입.
   * 성인 확인 버튼(`.enter-btn`)을 자바스크립트로 클릭 통과 후 도메인 쿠키를 확정.
   * **활성 셀레니움 드라이버를 `ScraperUtil._selenium_driver`에 등록**하여 이후 모든 페이지를 단일 브라우저 세션으로 연속 탐색.
2. **`on_extract_detail`:**
   * 본문에 마그넷 텍스트가 있으면 즉시 추출.
   * 마그넷이 없고 첨부파일 링크가 있으면 `{path_data}/tmp/feeder/` 임시 경로에 다운로드.
   * `.zip` 및 `.rar`(분할 압축 지원)을 임시 해제하고, 인코딩별(`utf-8`, `gb18030`, `gbk` 등) 텍스트 파일과 `.torrent` 메타데이터에서 마그넷 해시를 추출하여 반환.

---

### 5. 네트워크 및 보안 우회 아키텍처

Feeder는 사이트 보안 수준과 필요 여부에 따라 리소스를 최소화하면서도, Cloudflare Turnstile 및 성인 확인 게이트를 완벽히 통과할 수 있도록 **"우선순위 기반 인가 및 하향 상속(Top-Down Clearance & Downward Inheritance)"** 아키텍처를 채택하고 있습니다.

```text
[Feeder 크롤러 엔진 (FeedScraper)]
       │
       ├── [모드 A] Selenium 완전 렌더링 모드 (USE_SELENIUM 활성화 시)
       │      │
       │      ├─ 1. FlareSolverr 사전 인가 (선택) ──> cf_clearance 토큰 및 일치 User-Agent 확보
       │      └─ 2. Remote Selenium 기동 ─────────> --headless=new + 1920x1080 스텔스 + 쿠키 주입
       │                                            │ (사이트 진입 → 목록 수집 → 상세 루프 전체)
       │                                            ▼
       │                                     [단일 브라우저 프로세스 영속 세션 유지 (Turnstile 재발생 원천 차단)]
       │
       └── [모드 B] HTTP 고속 모드 (기본 모드)
              │
              ├─ 1. 최우선 인가 (Authority) ───────> FlareSolverr (nodriver) 무세션 단건 요청
              │                                      │ (403 차단 없이 Turnstile 통과 및 토큰/UA 발급)
              │                                      ▼
              ├─ 2. 하향 동기화 (Inheritance) ─────> 발급된 cf_clearance 쿠키 & 동일 UA를 하위 세션에 복제
              │                                      │
              │                                      ▼
              ├─ 3. 초고속 수집 (Execution) ──────> curl_cffi 세션 풀 (동일 Proxy IP + 동일 UA + 토큰 탑재)
              │                                      │ (0.2~0.5초 속도로 본문 및 상세 페이지 연속 수집)
              │                                      ▼
              └─ 4. 자가 치유 (Self-Healing) ──────> 토큰 만료(403) 감지 시 인가 파기 후 FlareSolverr 자동 재인가
```

#### 주요 기술적 특징

* **인증 자격 증명의 1:1 완벽 일치 (Clearance Consistency):**
  * Cloudflare의 `cf_clearance` 토큰은 발급 당시의 **User-Agent** 및 **IP(프록시)**와 암호학적으로 결합되어 있습니다.
  * Feeder는 FlareSolverr가 챌린지를 풀었을 때의 `userAgent`를 도메인별로 캐싱하여 `curl_cffi`, `requests`, `Selenium` 전 엔진에 동일하게 주입하므로, "토큰-UA 불일치"로 인한 403 차단이 발생하지 않습니다.

* **최우선 인가(Top-Down Authority)로 불필요한 403 회피:**
  * FlareSolverr가 켜져 있는 사이트는 토큰 없이 무작위로 먼저 요청하여 403을 유발하지 않고, FlareSolverr가 먼저 토큰을 발급받아 하위 엔진으로 내려보냅니다.
  * 인가 이후의 수십 개 상세 페이지 요청은 브라우저 구동 없이 `curl_cffi`(Chrome TLS/JA3 지문 에뮬레이션)가 맡아 고속으로 대량 수집합니다.

* **단일 Selenium 브라우저 프로세스 영속성:**
  * Selenium을 사용하는 고난도 사이트(색화당 등)는 초기화 단계에서 띄운 원격 브라우저 드라이버를 닫지 않고 `FeedScraper._selenium_driver`에 묶어둡니다.
  * 크롤링이 끝날 때까지 단 하나의 브라우저 탭으로 목록과 본문을 연속 탐색하므로, 페이지 이동 시 세션이 단절되어 Turnstile이 다시 뜨는 현상을 근본적으로 차단합니다.

* **프록시 IP 일관성 (Proxy Synchronization):**
  * FlareSolverr 컨테이너, Selenium 원격 드라이버, HTTP 수집 세션 모두 동일한 프록시(예: Gluetun)를 경유하도록 구성하여 Cloudflare 차단 및 국내 ISP 차단(warning.or.kr)을 동시에 우회합니다.

* **임시 파일 자동 청소:**
  * 디버깅용 캡처 화면, 다운로드 임시 파일, 압축 해제 임시 디렉터리는 모두 플랫폼 임시 폴더(`{path_data}/tmp/feeder/`)에서 격리 처리되며, 크롤링 종료 시 즉시 파기되고 FlaskFarm 재시작 시 플랫폼 차원에서 완전히 비워집니다.

---

### 6. RSS 피드 사용 및 다운로더 연동

생성된 피드는 표준 RSS 2.0 및 showRSS 네임스페이스를 따르며, qBittorrent, Transmission, Sonarr, Radarr 등 모든 토렌트 클라이언트의 RSS 다운로더에 바로 등록할 수 있습니다.

<br>
#### 피드 URL 형식

* **단일 게시판 피드:**
  ```text
  http://{YOUR_HOST}:{PORT}/feeder/api/board?site={SITENAME}&board={BOARD_ID}&apikey={APIKEY}
  ```
  *(예: `http://192.168.1.100:9999/feeder/api/board?site=sehuatang&board=103&apikey=abcdef123456`)*

* **서브 카테고리 전용 피드:**
  ```text
  http://{YOUR_HOST}:{PORT}/feeder/api/board?site={SITENAME}&board={BOARD_ID}_{SUBCAT_ID}&apikey={APIKEY}
  ```
  *(예: `http://192.168.1.100:9999/feeder/api/board?site=sehuatang&board=166_875&apikey=abcdef123456`)*

* **스케줄러 ID 기반 피드:**
  ```text
  http://{YOUR_HOST}:{PORT}/feeder/api/board?id={SCHEDULER_ID}&apikey={APIKEY}
  ```
  *(예: `http://192.168.1.100:9999/feeder/api/board?id=1&apikey=abcdef123456`)*

* **다중 게시판 그룹 피드 (동일 마그넷 자동 중복 제거):**
  ```text
  http://{YOUR_HOST}:{PORT}/feeder/api/group?name={GROUP_NAME}&apikey={APIKEY}
  ```
  *(예: `http://192.168.1.100:9999/feeder/api/group?name=korean_drama&apikey=abcdef123456`)*

* **첨부파일 직접 다운로드 프록시 스트림:**
  ```text
  http://{YOUR_HOST}:{PORT}/feeder/api/download?id={BBS_ID}_{FILE_INDEX}&apikey={APIKEY}
  ```
  *(예: `http://192.168.1.100:9999/feeder/api/download?id=1284_0&apikey=abcdef123456`)*

---
