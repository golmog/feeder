# -*- coding: utf-8 -*-
import re
import time
from urllib.parse import unquote
from html import unescape
from types import SimpleNamespace
from lxml import html

from .setup import *
from .model_crawl import ModelCrawlSite, ModelCrawlItem
from .util_crawl import (
    CrawlConfigUtil, CrawlCustomManager, CrawlScraper, CrawlTorrentInfo,
    extract_info_hash, extract_info_hash_from_torrent
)


class TaskCrawlBase:

    @F.celery.task(bind=True, acks_late=False)
    def start(self, *args):
        logger.info(f"[CrawlTask] Celery Task 수신 인자: {args} (Task ID: {self.request.id})")
        delivery_info = getattr(self.request, 'delivery_info', {}) or {}
        is_redelivered = delivery_info.get('redelivered') or getattr(self.request, 'redelivered', False)

        if is_redelivered:
            logger.warning("[CrawlTask] 이전 세션 비정상 종료로 재전송된 고아 태스크 실행 취소")
            return

        trigger_type = "scheduler"
        target_crawler_id = None
        for arg in args:
            if isinstance(arg, str) and arg in ["scheduler", "manual"]:
                trigger_type = arg
            elif isinstance(arg, (int, str)) and str(arg).isdigit():
                target_crawler_id = int(arg)

        TaskCrawl.start(trigger_type=trigger_type, target_crawler_id=target_crawler_id)


class TaskCrawl:

    @staticmethod
    def start(trigger_type: str = "scheduler", target_crawler_id: int = None):
        TaskCrawl.run_crawl(trigger_type=trigger_type, target_crawler_id=target_crawler_id)

    @staticmethod
    def parse_board_info(board: str, subcat: str = None) -> tuple[str, str, str]:
        board_str = str(board).strip() if board else ''
        subcat_str = str(subcat).strip() if subcat else ''

        if 'fid=' in board_str and 'typeid=' in board_str:
            m_fid = re.search(r'fid=(?P<fid>\d+)', board_str)
            m_type = re.search(r'typeid=(?P<typeid>\d+)', board_str)
            if m_fid:
                board_str = m_fid.group('fid')
            if m_type:
                subcat_str = m_type.group('typeid')
        elif not subcat_str and ':' in board_str:
            parts = board_str.split(':', 1)
            board_str, subcat_str = parts[0].strip(), parts[1].strip()

        full_key = f"{board_str}:{subcat_str}" if subcat_str else board_str
        return board_str, subcat_str, full_key

    @staticmethod
    def build_board_url(site_info: dict, board: str, page: int, subcat: str = None) -> str:
        site_url = site_info.get('TORRENT_SITE_URL', '').rstrip('/')
        board_id, subcat_id, full_key = TaskCrawl.parse_board_info(board, subcat)

        if subcat_id and 'SUBCAT_URL_RULE' in site_info:
            rule = site_info['SUBCAT_URL_RULE']
            return rule.format(
                URL=site_url,
                BOARD_NAME=board_id,
                FID=board_id,
                SUBCAT=subcat_id,
                TYPEID=subcat_id,
                PAGE=page
            )

        rule = site_info.get('BOARD_URL_RULE', '{URL}/bbs/board.php?bo_table={BOARD_NAME}&page={PAGE}')
        try:
            return rule.format(
                URL=site_url,
                BOARD_NAME=board_id if (subcat_id and '{SUBCAT}' in rule) else (full_key if not subcat_id else board_id),
                FID=board_id,
                SUBCAT=subcat_id or '',
                TYPEID=subcat_id or '',
                PAGE=page
            )
        except KeyError:
            return rule.format(URL=site_url, BOARD_NAME=board_id, PAGE=page)

    @staticmethod
    def run_crawl(trigger_type: str = "scheduler", target_crawler_id: int = None):
        with F.app.app_context():
            P.ModelSetting.set('crawl_is_running', 'True')
            P.ModelSetting.set('crawl_running_start_time', str(int(time.time())))
            try:
                is_manual = (trigger_type == "manual")
                mode_label = "즉시 실행" if is_manual else "스케줄러 정기 실행"

                always_max_page = P.ModelSetting.get_bool('crawl_always_max_page')
                target_desc = f"개별 수집기(ID: {target_crawler_id})" if target_crawler_id else "전체 수집기"
                logger.info(f"[TaskCrawl] [{mode_label}] 크롤링 수집 작업 시작 ({target_desc}, 항상 최대 페이지 탐색: {'ON' if always_max_page else 'OFF'})")

                crawlers = CrawlConfigUtil.get_crawlers()
                if not crawlers:
                    logger.info("[TaskCrawl] 등록된 수집기(CRAWLERS)가 없습니다.")
                    return

                if target_crawler_id is not None:
                    crawlers = [c for c in crawlers if int(c.get('id', -1)) == int(target_crawler_id)]
                    if not crawlers:
                        logger.warning(f"[TaskCrawl] 대상 수집기(ID: {target_crawler_id})를 찾을 수 없습니다.")
                        return

                if not is_manual:
                    current_count = P.ModelSetting.get_int('crawl_scheduler_count') + 1
                    P.ModelSetting.set('crawl_scheduler_count', str(current_count))
                else:
                    current_count = P.ModelSetting.get_int('crawl_scheduler_count')

                try:
                    max_page = P.ModelSetting.get_int('crawl_max_page')
                except Exception:
                    max_page = 5

                total_crawled_count = 0
                for crawler in crawlers:
                    if not is_manual:
                        if not crawler.get('enabled', True):
                            logger.debug(f"[TaskCrawl] 비활성화된 수집기 건너뜀: {crawler.get('site')}")
                            continue

                        target_interval = int(crawler.get('interval', 1))
                        if target_interval > 1:
                            if (current_count % target_interval) != 0:
                                logger.info(f"[TaskCrawl] 스케쥴 빈도({target_interval}회당 1회) 미도래로 건너뜀: {crawler.get('site')}")
                                continue
                    else:
                        if target_crawler_id is None and not crawler.get('enabled', True):
                            logger.debug(f"[TaskCrawl] 비활성화된 수집기 건너뜀: {crawler.get('site')}")
                            continue

                    site_name = crawler.get('site')
                    site_entity = ModelCrawlSite.get(name=site_name)
                    if not site_entity:
                        logger.warning(f"[TaskCrawl] 등록되지 않은 사이트명: {site_name}")
                        continue

                    boards = crawler.get('boards', [])
                    if not boards:
                        logger.debug(f"[TaskCrawl] [{site_name}] 등록된 게시판이 없어 건너뜀")
                        continue

                    target_cfg = SimpleNamespace(
                        use_proxy=crawler.get('use_proxy', False),
                        proxy_url=crawler.get('proxy_url', '').strip(),
                        use_flaresolverr=crawler.get('use_flaresolverr', False),
                        use_selenium=crawler.get('use_selenium', False),
                        use_torrent_info=crawler.get('use_torrent_info', False),
                        delay=crawler.get('delay', ''),
                        max_retries=crawler.get('max_retries', '')
                    )

                    logger.info(f"[TaskCrawl] [{mode_label}] 사이트 크롤러 시작: [{site_name}] (대상 게시판={len(boards)}개, 최대 탐색={max_page}p)")

                    for b_idx, b in enumerate(boards):
                        if b_idx > 0:
                            CrawlScraper.rotate_proxy(scheduler_instance=target_cfg, reason="게시판 전환 부하 분산")

                        board_id = b.get('board')
                        subcat_id = str(b.get('subcat', '')).strip()
                        full_board_key = b.get('full_board_key') or board_id

                        last_bbs = ModelCrawlItem.get_last_item(site_name, full_board_key)
                        max_id = 0
                        extra = site_entity.info.get('EXTRA', []) if site_entity.info else []

                        if not always_max_page and 'USING_POST_CHAR_ID' not in extra and last_bbs and last_bbs.post_id:
                            max_id = last_bbs.post_id

                        target_cfg.subcat_id = subcat_id

                        if always_max_page:
                            logger.info(f"[TaskCrawl] [{site_name}] {full_board_key} [{mode_label}]: 항상 최대 페이지 탐색 (최대 {max_page}p 전체 탐색, 기수집건 스킵)")
                        else:
                            logger.info(f"[TaskCrawl] [{site_name}] {full_board_key} [{mode_label}]: 증분 탐색 (최근 수집 ID: {max_id})")

                        try:
                            crawled = TaskCrawl.execute_board_crawl(
                                site_entity.info,
                                board_id,
                                max_page=max_page,
                                max_id=max_id,
                                is_test=False,
                                max_count=0,
                                target_cfg=target_cfg
                            )

                            if crawled is None:
                                logger.error(f"[TaskCrawl] [{site_name}] 세션 초기화 실패로 수집 작업을 조기 중단합니다.")
                                break

                            if crawled:
                                total_crawled_count += len(crawled)
                                logger.info(f"[TaskCrawl] [{site_name}] {full_board_key}: {len(crawled)}개 항목 수집 완료")
                        except Exception as board_err:
                            logger.error(f"[TaskCrawl] [{site_name}] {full_board_key} 수집 중 오류: {board_err}")
                            logger.error(traceback.format_exc())
                        finally:
                            CrawlScraper.close_sessions()
                            crawl_delay = TaskCrawl.get_crawl_delay(site_entity.info, target_cfg=target_cfg)
                            if crawl_delay > 0:
                                time.sleep(crawl_delay)

                logger.info(f"[TaskCrawl] 전체 수집 작업 완료: 총 {total_crawled_count}개 게시물 처리됨")

            except Exception as e:
                logger.error(f"[TaskCrawl] 크롤링 수집 중 오류: {e}")
                logger.error(traceback.format_exc())
            finally:
                P.ModelSetting.set('crawl_is_running', 'False')
                CrawlScraper.close_sessions()

    @staticmethod
    def get_crawl_delay(site_info: dict = None, target_cfg=None) -> float:
        try:
            if target_cfg:
                cfg_delay = getattr(target_cfg, 'delay', None) or getattr(target_cfg, 'crawler_delay', None)
                if cfg_delay not in [None, '']:
                    return float(cfg_delay)

            global_delay = P.ModelSetting.get('crawl_crawler_delay')
            if global_delay not in [None, '']:
                return float(global_delay)

            return 2.0
        except Exception:
            return 2.0

    @staticmethod
    def execute_board_crawl(site_info: dict, board: str, max_page: int = 1, max_id: int = 0, is_test: bool = False, max_count: int = 0, target_cfg=None) -> list[dict]:
        bbs_list = []
        site_name = site_info.get('NAME', '')
        subcat_param = getattr(target_cfg, 'subcat_id', None) if target_cfg else None
        board_id, subcat_id, full_board_key = TaskCrawl.parse_board_info(board, subcat_param)

        xpath_dict = site_info.get('XPATH_LIST_TAG', {})
        if 'BOARD_LIST' in site_info:
            board_key = board_id.split('&')[0]
            if board_key in site_info['BOARD_LIST']:
                xpath_dict = site_info.get(site_info['BOARD_LIST'][board_key], xpath_dict)

        index_step = xpath_dict.get('INDEX_STEP', 1)
        index_start = xpath_dict.get('INDEX_START', 1)

        id_regexs = []
        raw_id_rule = site_info.get('ID_REGEX')
        if raw_id_rule:
            if isinstance(raw_id_rule, list):
                id_regexs.extend(raw_id_rule)
            else:
                id_regexs.append(raw_id_rule)

        standard_regexs = [
            r'tid=(?P<id>\d+)',
            r'thread-(?P<id>\d+)',
            r'wr_id=(?P<id>\d+)',
            r'/view/(?P<id>\d+)',
            r'/(?P<id>\d+)\.html',
            r'/(?P<id>\d+)$'
        ]
        for s_reg in standard_regexs:
            if s_reg not in id_regexs:
                id_regexs.append(s_reg)

        allow_duplicate_magnet = P.ModelSetting.get_bool('crawl_allow_duplicate_magnet')
        target_pages = 1 if is_test else max(1, max_page)
        stop_crawl = False
        crawl_delay = TaskCrawl.get_crawl_delay(site_info, target_cfg=target_cfg)

        if target_cfg is None:
            target_cfg = SimpleNamespace(
                use_proxy=P.ModelSetting.get_bool('crawl_use_proxy'),
                proxy_url=P.ModelSetting.get('crawl_proxy_url') or '',
                use_flaresolverr=P.ModelSetting.get_bool('crawl_use_flaresolverr'),
                use_selenium=P.ModelSetting.get_bool('crawl_use_selenium'),
                use_torrent_info=P.ModelSetting.get_bool('crawl_use_torrent_info'),
                delay='',
                max_retries=''
            )

        if not site_info.get('SELENIUM_REMOTE_URL'):
            site_info['SELENIUM_REMOTE_URL'] = P.ModelSetting.get('crawl_selenium_remote_url') or ''
        if not site_info.get('PROXY_URL') and target_cfg:
            site_info['PROXY_URL'] = getattr(target_cfg, 'proxy_url', '')

        try:
            hook = CrawlCustomManager.get_hook(site_name)
            if hook:
                logger.info(f"[{site_name}] 커스텀 사이트 훅 감지: {hook.__name__}")
                if hasattr(hook, 'on_init_session'):
                    logger.info(f"[{site_name}] 커스텀 훅 세션 초기화(on_init_session) 실행")
                    hook.on_init_session(site_info, target_cfg)

            if getattr(target_cfg, 'use_selenium', False) and CrawlScraper._selenium_driver is None:
                logger.error(f"[TaskCrawl] [{site_name}] 세션 생성 실패로 게시판({full_board_key}) 수집을 시작할 수 없습니다.")
                return None

            for cur_page in range(1, target_pages + 1):
                if cur_page > 1 and crawl_delay > 0:
                    time.sleep(crawl_delay)

                board_url = TaskCrawl.build_board_url(site_info, board_id, cur_page, subcat=subcat_id)
                logger.info(f"[TaskCrawl] [{cur_page}/{target_pages}p] 게시판 목록 URL 요청 시작: {board_url}")

                list_wait_tag = site_info.get('SELENIUM_WAIT_TAG', 'body')
                html_source = CrawlScraper.get_html(board_url, site_info=site_info, scheduler_instance=target_cfg, wait_tag=list_wait_tag)

                if not html_source:
                    logger.warning(f"[TaskCrawl] 게시판 HTML 수신 실패: {board_url}")
                    break

                tree = html.fromstring(html_source)
                list_xpath = xpath_dict.get('XPATH', '')
                base_xpath = list_xpath[:list_xpath.find('[%s]')] if '[%s]' in list_xpath else list_xpath
                elements = tree.xpath(base_xpath)

                raw_list = []
                for i in range(index_start, len(elements) + 1, index_step):
                    try:
                        target_tags = tree.xpath(list_xpath % i) if '[%s]' in list_xpath else [elements[i - 1]]
                        if not target_tags:
                            continue

                        target_tag = target_tags[-1]
                        title = target_tag.text_content().strip()
                        if 'TITLE_REGEX' in xpath_dict:
                            match = re.search(xpath_dict['TITLE_REGEX'], title)
                            if match:
                                title = match.group('title')

                        detail_url = target_tag.attrib.get('href', '')
                        if not detail_url.startswith('http'):
                            detail_url = f"{site_info.get('TORRENT_SITE_URL', '').rstrip('/')}/{detail_url.lstrip('/')}"

                        post_id = ''
                        for regex in id_regexs:
                            match_id = re.search(regex, detail_url)
                            if match_id and 'id' in match_id.groupdict():
                                post_id = match_id.group('id')
                                break

                        if not post_id:
                            url_no_query = detail_url.split('?')[0]
                            for regex in id_regexs:
                                match_id = re.search(regex, url_no_query)
                                if match_id and 'id' in match_id.groupdict():
                                    post_id = match_id.group('id')
                                    break

                        if not post_id and not is_test:
                            continue

                        raw_list.append({'id': post_id, 'title': title, 'url': detail_url})
                    except Exception as e:
                        logger.error(f"[TaskCrawl] [{site_name}] 목록 행 파싱 예외 (인덱스 {i}): {e}")

                if not raw_list:
                    break

                total_targets = min(len(raw_list), max_count) if (is_test and max_count > 0) else len(raw_list)
                logger.info(f"[TaskCrawl] [{site_name}] 상세 페이지 파싱 대상: {total_targets}개")

                detail_count = 0
                for idx, item in enumerate(raw_list):
                    if P.ModelSetting.get_bool('crawl_task_stop_flag'):
                        stop_crawl = True
                        break

                    if is_test and max_count > 0 and detail_count >= max_count:
                        bbs_list.append(item)
                        continue

                    post_id = item.get('id', '')

                    if not is_test and max_id > 0 and post_id:
                        try:
                            if int(post_id) <= max_id:
                                logger.info(f"[TaskCrawl] 기존 수집 완료 지점(ID: {max_id}) 도달 -> 수집 종료")
                                stop_crawl = True
                                break
                        except Exception:
                            pass

                    if not is_test and post_id:
                        existing = ModelCrawlItem.get(site=site_name, board=full_board_key, post_id=int(post_id)) if str(post_id).isdigit() else ModelCrawlItem.get(site=site_name, board=full_board_key, post_char_id=str(post_id))
                        if existing:
                            logger.debug(f"[TaskCrawl] 이미 수집 완료된 게시물 건너뜀: [{site_name}] {item['title'][:30]} (ID: {post_id})")
                            continue

                    if crawl_delay > 0:
                        time.sleep(crawl_delay)

                    logger.info(f"[TaskCrawl] [{cur_page}p - {idx+1}/{total_targets}] 상세 수집: ID={item['id']} / {item['title'][:35]}...")
                    detail_wait_tag = site_info.get('SELENIUM_DETAIL_WAIT_TAG', 'body')
                    detail_html = CrawlScraper.get_html(item['url'], site_info=site_info, scheduler_instance=target_cfg, referer=board_url, wait_tag=detail_wait_tag)

                    if detail_html:
                        detail_tree = html.fromstring(detail_html)
                        if hook and hasattr(hook, 'on_extract_detail'):
                            item['magnet'] = hook.on_extract_detail(detail_html, item, site_info, target_cfg)
                        else:
                            item['magnet'] = TaskCrawl.extract_magnets(detail_html, detail_tree, site_info)
                        item['download'] = TaskCrawl.extract_downloads(detail_html, site_info, item)

                        # 마그넷 부재 시 .torrent 첨부파일에서 마그넷 자동 변환
                        item['magnet'] = item.get('magnet') or []
                        item['download'] = item.get('download') or []
                        if not item['magnet'] and item['download']:
                            for d_item in item['download']:
                                f_link = d_item.get('link', '')
                                f_name = d_item.get('filename', '').lower()
                                if f_name.endswith('.torrent') and f_link:
                                    try:
                                        t_io = CrawlScraper.download_file_stream(f_link, referer=item['url'], scheduler_instance=target_cfg)
                                        if t_io:
                                            info_hash = extract_info_hash_from_torrent(t_io.getvalue())
                                            if info_hash:
                                                extracted_mag = f"magnet:?xt=urn:btih:{info_hash}"
                                                item['magnet'].append(extracted_mag)
                                                logger.info(f"[TaskCrawl] [{site_name}] .torrent 분석으로 마그넷 획득: {f_name} -> {extracted_mag}")
                                                break
                                    except Exception as ex:
                                        logger.debug(f"[TaskCrawl] [{site_name}] 첨부 토렌트 분석 실패 ({f_name}): {ex}")

                        item['torrent_info'] = CrawlTorrentInfo.get_torrent_info(item['magnet'], target_cfg)
                    else:
                        item['magnet'] = []
                        item['download'] = []
                        item['torrent_info'] = None

                    if not is_test and not allow_duplicate_magnet and item.get('magnet'):
                        if ModelCrawlItem.is_exist_magnet(item['magnet']):
                            logger.info(f"[TaskCrawl] 중복 마그넷이므로 수집 제외: {item['title'][:35]}...")
                            continue

                    # 수집 원본 DB 저장 (마그넷/파일이 있거나, 로그인 필요 상태인 경우)
                    if not is_test and (item.get('magnet') or 'ONLY_FILE' in site_info.get('EXTRA', []) or item.get('broadcast_status')):
                        TaskCrawl.save_single_item(site_name, full_board_key, item)

                    bbs_list.append(item)
                    detail_count += 1

                    if not is_test and max_count > 0 and len(bbs_list) >= max_count:
                        stop_crawl = True
                        break

                if stop_crawl:
                    break
        finally:
            CrawlScraper.close_sessions()

        return bbs_list

    @staticmethod
    def extract_magnets(page_html: str, tree, site_info: dict) -> list[str]:
        magnets = []
        seen_hashes = set()
        extra = site_info.get('EXTRA', [])

        magnet_rule = site_info.get('MAGNET_REGEX')

        if not magnet_rule:
            if tree is not None:
                elements = tree.xpath("//a[starts-with(@href,'magnet')]")
                for elem in elements:
                    href = elem.attrib.get('href', '').strip()
                    if not href:
                        continue
                    info_hash = extract_info_hash(href)
                    if info_hash:
                        if info_hash in seen_hashes:
                            continue
                        seen_hashes.add(info_hash)
                        magnets.append(f"magnet:?xt=urn:btih:{info_hash}")
                    else:
                        norm_mag = href.lower()[:60]
                        if norm_mag not in magnets:
                            magnets.append(norm_mag)

            if page_html:
                text_magnets = re.findall(r'magnet:\?xt=urn:btih:[a-zA-Z0-9]+', page_html, re.IGNORECASE)
                for raw_mag in text_magnets:
                    info_hash = extract_info_hash(raw_mag)
                    if info_hash:
                        if info_hash in seen_hashes:
                            continue
                        seen_hashes.add(info_hash)
                        magnets.append(f"magnet:?xt=urn:btih:{info_hash}")
                    else:
                        norm_mag = raw_mag.lower()[:60]
                        if norm_mag not in magnets:
                            magnets.append(norm_mag)

                text_ed2k = re.findall(r'ed2k://\|file\|[^|]+\|[0-9]+\|[a-fA-F0-9]{32}(?:\|[^|]*)?\|/', page_html, re.IGNORECASE)
                for raw_ed2k in text_ed2k:
                    ed2k_hash = extract_info_hash(raw_ed2k)
                    if ed2k_hash:
                        if ed2k_hash in seen_hashes:
                            continue
                        seen_hashes.add(ed2k_hash)
                        magnets.append(raw_ed2k)
                    elif raw_ed2k not in magnets:
                        magnets.append(raw_ed2k)
        else:
            if isinstance(magnet_rule, (list, tuple)) and len(magnet_rule) >= 2:
                pattern, template = magnet_rule[0], magnet_rule[1]
            elif isinstance(magnet_rule, (list, tuple)) and len(magnet_rule) == 1:
                pattern, template = magnet_rule[0], "%s"
            else:
                pattern, template = str(magnet_rule), "%s"

            for m in re.findall(pattern, page_html):
                extracted_str = m[0] if isinstance(m, tuple) else m
                formatted = (template % extracted_str).lower() if '%s' in template else extracted_str.lower()
                info_hash = extract_info_hash(formatted)
                if info_hash:
                    if info_hash in seen_hashes:
                        continue
                    seen_hashes.add(info_hash)
                    magnets.append(f"magnet:?xt=urn:btih:{info_hash}")
                else:
                    norm_mag = formatted[:60]
                    if norm_mag not in magnets:
                        magnets.append(norm_mag)

        if 'MAGNET_ONLY_ONE_LAST' in extra and magnets:
            magnets = [magnets[-1]]
        return magnets

    @staticmethod
    def extract_downloads(page_html: str, site_info: dict, post_item: dict) -> list[dict]:
        downloads = []
        seen_links = set()
        if 'DOWNLOAD_REGEX' not in site_info:
            return downloads

        matches = re.finditer(site_info['DOWNLOAD_REGEX'], page_html, re.MULTILINE)
        for m in matches:
            filename = unescape(unquote(m.group('filename').strip()))
            link = unescape(unquote(m.group('url').strip()))
            if not filename or not link:
                continue

            if not link.startswith('http'):
                link = f"{site_info.get('TORRENT_SITE_URL', '').rstrip('/')}/{link.lstrip('/')}"

            if link in seen_links:
                continue
            seen_links.add(link)

            downloads.append({'link': link, 'filename': filename})
        return downloads

    @staticmethod
    def save_single_item(site_name: str, board_name: str, item: dict):
        try:
            post_id = item.get('id', '')
            if not post_id:
                return None

            existing = ModelCrawlItem.get(site=site_name, board=board_name, post_id=int(post_id)) if str(post_id).isdigit() else ModelCrawlItem.get(site=site_name, board=board_name, post_char_id=str(post_id))
            if existing:
                return existing

            bbs = ModelCrawlItem(site_name, board_name)
            if str(post_id).isdigit():
                bbs.post_id = int(post_id)
            else:
                bbs.post_char_id = str(post_id)

            bbs.title = item.get('title', '')
            bbs.url = item.get('url', '')
            bbs.magnet_count = len(item['magnet']) if item.get('magnet') else 0
            bbs.file_count = len(item['download']) if item.get('download') else 0
            bbs.magnet = '\n'.join(item['magnet']) if item.get('magnet') else ''
            bbs.torrent_info = item.get('torrent_info')
            bbs.broadcast_status = item.get('broadcast_status', '')

            if bbs.file_count > 0:
                bbs.files = '||'.join(f"{x['link']}|{x['filename']}|NONE" for x in item['download'])
            else:
                bbs.files = None

            db.session.add(bbs)
            db.session.commit()
            if bbs.broadcast_status == 'LOGIN_REQUIRED':
                logger.info(f"[TaskCrawl] DB 저장 완료 (로그인 필요 건너뜀 기록): [{site_name}] {bbs.title[:30]}... (ID: {post_id})")
            else:
                logger.info(f"[TaskCrawl] DB 저장 완료: [{site_name}] {bbs.title[:30]}... (ID: {post_id})")
            return bbs
        except Exception as e:
            logger.error(f"[TaskCrawl] 단일 항목 DB 저장 오류: {e}")
            db.session.rollback()
            return None
