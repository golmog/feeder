var current_sites = [];
var current_crawlers = [];
var current_feeds = [];
var modal_crawler_boards = [];
var modal_feed_sources = [];

var json_editor = null;
var python_editor = null;

var SCRIPT_SKELETON = '# -*- coding: utf-8 -*-\n' +
  '"""\n' +
  '커스텀 사이트 훅 스크립트\n' +
  '"""\n' +
  'from ..setup import logger\n\n' +
  'class CustomSiteHook:\n' +
  '    SITE_NAME = "custom_site"  # 사이트 식별명\n\n' +
  '    DEFAULT_SITE_INFO = {\n' +
  '        "NAME": "custom_site",\n' +
  '        "TORRENT_SITE_URL": "https://example.com",\n' +
  '        "DELAY": 1.0,\n' +
  '        "BOARD_URL_RULE": "{URL}/bbs/board.php?bo_table={BOARD_NAME}&page={PAGE}",\n' +
  '        "XPATH_LIST_TAG": {\n' +
  '            "XPATH": "//tbody/tr[%s]//td[2]//a",\n' +
  '            "INDEX_START": 1,\n' +
  '            "INDEX_STEP": 1\n' +
  '        },\n' +
  '        "ID_REGEX": "wr_id=(?P<id>\\\\d+)",\n' +
  '        "DESCRIPTION": [\n' +
  '            "커스텀 훅 연동 사이트",\n' +
  '            "- 본문 분석 및 첨부파일 처리는 해당 스크립트에서 수행됩니다."\n' +
  '        ]\n' +
  '    }\n\n' +
  '    @classmethod\n' +
  '    def on_init_session(cls, site_info: dict, scheduler_cfg) -> None:\n' +
  '        """크롤링 시작 전 세션/쿠키 준비"""\n' +
  '        pass\n\n' +
  '    @classmethod\n' +
  '    def on_extract_detail(cls, detail_html: str, item: dict, site_info: dict, scheduler_cfg) -> list[str]:\n' +
  '        """본문 파싱, 마그넷 추출 또는 첨부파일 다운로드 & 압축 해제"""\n' +
  '        return []\n';

function init_ace_editors() {
  if ($('#modal_site_json_editor').length && !json_editor) {
    json_editor = ace.edit("modal_site_json_editor");
    json_editor.setTheme("ace/theme/monokai");
    json_editor.session.setMode("ace/mode/json");
    json_editor.setFontSize(13);
    json_editor.setShowPrintMargin(false);
    json_editor.session.setTabSize(2);
    json_editor.session.setUseSoftTabs(true);
    json_editor.session.setUseWrapMode(true);
  }

  if ($('#custom_script_code_editor').length && !python_editor) {
    python_editor = ace.edit("custom_script_code_editor");
    python_editor.setTheme("ace/theme/monokai");
    python_editor.session.setMode("ace/mode/python");
    python_editor.setFontSize(13);
    python_editor.setShowPrintMargin(false);
    python_editor.session.setTabSize(4);
    python_editor.session.setUseSoftTabs(true);
    python_editor.session.setUseWrapMode(false);
  }
}

$(document).on('click', '.modal-fullscreen-btn', function(e){
  e.preventDefault();
  var modalDialog = $(this).closest('.modal-dialog');
  modalDialog.toggleClass('modal-fullscreen');
  var icon = $(this).find('i');
  if (modalDialog.hasClass('modal-fullscreen')) {
    icon.removeClass('fa-expand').addClass('fa-compress');
  } else {
    icon.removeClass('fa-compress').addClass('fa-expand');
  }
  setTimeout(function(){
    if (json_editor) json_editor.resize();
    if (python_editor) python_editor.resize();
  }, 150);
});

if (window.ResizeObserver) {
  var editorResizeObserver = new ResizeObserver(function() {
    if (json_editor) json_editor.resize();
    if (python_editor) python_editor.resize();
  });
  var jsonEl = document.getElementById('modal_site_json_editor');
  if (jsonEl) editorResizeObserver.observe(jsonEl);
  var pyEl = document.getElementById('custom_script_code_editor');
  if (pyEl) editorResizeObserver.observe(pyEl);
  var scriptModalContent = document.querySelector('#custom_script_modal .modal-content');
  if (scriptModalContent) editorResizeObserver.observe(scriptModalContent);
}

$('#custom_script_modal').on('shown.bs.modal', function () {
  if (python_editor) {
    python_editor.resize();
    python_editor.renderer.updateFull();
  }
});

$('#site_modal').on('shown.bs.modal', function () {
  if (json_editor) {
    json_editor.resize();
    json_editor.renderer.updateFull();
  }
});

$(document).on('change', '#modal_site_json_wrap_chk', function(){
  if (json_editor) {
    json_editor.session.setUseWrapMode($(this).is(':checked'));
  }
});

$(document).on('change', '#custom_script_wrap_chk', function(){
  if (python_editor) {
    python_editor.session.setUseWrapMode($(this).is(':checked'));
  }
});

$(document).ready(function(){
  use_collapse("feed_use_proxy");
  use_collapse("feed_use_flaresolverr");
  use_collapse("feed_use_selenium");
  use_collapse("feed_use_torrent_info");
  use_collapse("feed_make_rss_file");
  toggle_qb_setting();
  load_all_data();
  init_ace_editors();

  try {
    localStorage.setItem('feeder_last_feed_page', 'setting');
    sync_feeder_header_navbar();
  } catch(err) {}

  restore_feed_subtab();
  setTimeout(restore_feed_subtab, 80);
});

function restore_feed_subtab() {
  try {
    var saved_tab = localStorage.getItem(package_name + '_' + sub + '_active_tab');
    if (saved_tab) {
      var tabElem = $('#nav-tab a[href="' + saved_tab + '"]');
      if (tabElem.length > 0 && !tabElem.hasClass('active')) {
        tabElem.tab('show');
      }
    }
  } catch(e) {}
}

$(document).on('shown.bs.tab', '#nav-tab a[data-toggle="tab"]', function(e){
  try {
    var targetTab = $(e.target).attr('href');
    if (targetTab && targetTab.startsWith('#')) {
      localStorage.setItem(package_name + '_' + sub + '_active_tab', targetTab);
    }
  } catch(err) {}
});

function sync_feeder_header_navbar() {
  try {
    var lastFeed = localStorage.getItem('feeder_last_feed_page') || 'setting';
    var lastDl = localStorage.getItem('feeder_last_download_page') || 'setting';
    $('.navbar a[href*="/' + package_name + '/feed"]').attr('href', '/' + package_name + '/feed/' + lastFeed);
    $('.navbar a[href*="/' + package_name + '/download"]').attr('href', '/' + package_name + '/download/' + lastDl);
  } catch(e) {}
}

$(document).on('click', '.navbar a', function(e){
  var href = $(this).attr('href') || '';
  if (href.indexOf('/' + package_name + '/feed') !== -1) {
    var lastFeed = localStorage.getItem('feeder_last_feed_page');
    if (lastFeed && lastFeed !== 'setting') {
      e.preventDefault();
      window.location.href = '/' + package_name + '/feed/' + lastFeed;
    }
  } else if (href.indexOf('/' + package_name + '/download') !== -1) {
    var lastDl = localStorage.getItem('feeder_last_download_page');
    if (lastDl && lastDl !== 'setting') {
      e.preventDefault();
      window.location.href = '/' + package_name + '/download/' + lastDl;
    }
  }
});

function toggle_qb_setting() {
  var method = $('input[name="feed_torrent_info_method"]:checked').val();
  if (method === 'qbittorrent') $('#qb_setting_div').show();
  else $('#qb_setting_div').hide();
}

$('input[name="feed_torrent_info_method"]').change(toggle_qb_setting);
$('#feed_use_proxy').change(function() { use_collapse('feed_use_proxy'); });
$('#feed_use_flaresolverr').change(function() { use_collapse('feed_use_flaresolverr'); });
$('#feed_use_selenium').change(function() { use_collapse('feed_use_selenium'); });
$('#feed_use_torrent_info').change(function() { use_collapse('feed_use_torrent_info'); });
$('#feed_make_rss_file').change(function() { use_collapse('feed_make_rss_file'); });

function load_all_data() {
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/load_site',
    type: "POST",
    dataType: "json",
    success: function(data) {
      current_sites = data.site || [];
      render_sites(current_sites);
    }
  });

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/load_crawlers',
    type: "POST",
    dataType: "json",
    success: function(data) {
      current_crawlers = data.crawlers || [];
      render_crawlers(current_crawlers);
    }
  });

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/load_feeds',
    type: "POST",
    dataType: "json",
    success: function(data) {
      current_feeds = data.feeds || [];
      render_feeds(current_feeds);
    }
  });
}

function render_sites(data) {
  var tbody = $('#site_list_tbody');
  if (!data || data.length === 0) {
    tbody.html('<tr><td colspan="4" class="py-4 text-muted">등록된 사이트가 없습니다.</td></tr>');
    return;
  }
  var str = '';
  for (var i = 0; i < data.length; i++) {
    var item = data[i];
    var info = item.info || {};
    var siteUrl = info.TORRENT_SITE_URL || '#';
    var descHtml = '';
    if (info.DESCRIPTION) {
      var descList = Array.isArray(info.DESCRIPTION) ? info.DESCRIPTION : [info.DESCRIPTION];
      descHtml = '<div class="text-left small mb-2 p-2 rounded" style="background: rgba(128, 128, 128, 0.12); border-left: 3px solid #17a2b8; line-height: 1.5; color: inherit;">' + descList.join('<br>') + '</div>';
    }

    var opts = item.options || {};
    var pClass = opts.use_proxy ? 'badge-proxy-on' : 'badge-off';
    var fClass = opts.use_flaresolverr ? 'badge-flare-on' : 'badge-off';
    var sClass = opts.use_selenium ? 'badge-selenium-on' : 'badge-off';
    var tClass = opts.use_torrent_info ? 'badge-torinfo-on' : 'badge-off';

    var pBadge = '<span class="badge ' + pClass + ' site_opt_toggle mr-1" data-id="' + item.id + '" data-opt="use_proxy" title="클릭하여 프록시 기본값 토글">Proxy ' + (opts.use_proxy ? 'ON' : 'OFF') + '</span>';
    var fBadge = '<span class="badge ' + fClass + ' site_opt_toggle mr-1" data-id="' + item.id + '" data-opt="use_flaresolverr" title="클릭하여 FlareSolverr 기본값 토글">Flare ' + (opts.use_flaresolverr ? 'ON' : 'OFF') + '</span>';
    var sBadge = '<span class="badge ' + sClass + ' site_opt_toggle mr-1" data-id="' + item.id + '" data-opt="use_selenium" title="클릭하여 Selenium 기본값 토글">Selenium ' + (opts.use_selenium ? 'ON' : 'OFF') + '</span>';
    var tBadge = '<span class="badge ' + tClass + ' site_opt_toggle" data-id="' + item.id + '" data-opt="use_torrent_info" title="클릭하여 토렌트 메타정보 기본값 토글">TorInfo ' + (opts.use_torrent_info ? 'ON' : 'OFF') + '</span>';

    str += '<tr>';
    str += '  <td class="font-weight-bold">' + item.id + '</td>';
    str += '  <td><span class="badge badge-secondary">' + item.info_type + '</span></td>';
    str += '  <td class="text-left">';
    str += '    <strong><a href="' + siteUrl + '" target="_blank">' + (info.NAME || '') + '</a></strong><br>';
    str += '    <div class="mt-1">' + pBadge + fBadge + sBadge + tBadge + '</div>';
    str += '  </td>';
    str += '  <td>';
    str += '    <div class="col-md-10 mx-auto px-0 text-left">' + descHtml;
    str += '      <div class="input-group input-group-sm">';
    str += '        <input id="board_id_' + item.id + '" type="text" class="form-control col-md-5" placeholder="게시판 ID (예: 2_2, 103, 166:875)">';
    str += '        <div class="input-group-append">';
    str += '          <button type="button" class="btn btn-info text-white test_btn" data-site_id="' + item.id + '">수집 테스트</button>';
    str += '          <button type="button" class="btn btn-primary text-white site_edit_btn" data-site_id="' + item.id + '" data-index="' + i + '">규칙 수정</button>';
    str += '          <button type="button" class="btn btn-danger text-white remove_site_btn" data-site_id="' + item.id + '">삭제</button>';
    str += '        </div>';
    str += '      </div>';
    str += '    </div>';
    str += '  </td>';
    str += '</tr>';
  }
  tbody.html(str);
}

$(document).on('click', '.site_opt_toggle', function(e){
  e.preventDefault();
  var site_id = $(this).data('id');
  var opt = $(this).data('opt');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/site_toggle_option',
    type: "POST",
    data: {site_id: site_id, option: opt},
    dataType: "json",
    success: function(data) {
      if (data.ret === 'success') {
        current_sites = data.site;
        render_sites(current_sites);
        notify('사이트 기본 옵션이 변경되었습니다.', 'info');
      } else {
        notify('옵션 변경 실패: ' + (data.log || data.ret), 'warning');
      }
    }
  });
});

function render_crawlers(data) {
  var tbody = $('#crawler_list_tbody');
  if (!data || data.length === 0) {
    tbody.html('<tr><td colspan="4" class="py-4 text-muted">등록된 수집기(CRAWLERS)가 없습니다. 상단의 "수집기 추가"를 누르세요.</td></tr>');
    return;
  }
  var str = '';
  for (var i = 0; i < data.length; i++) {
    var item = data[i];
    var isEnabled = (item.enabled === true || item.enabled === 'True' || item.enabled === 'true' || item.enabled === 'on');
    var isTorrentInfo = (item.use_torrent_info === true || item.use_torrent_info === 'True' || item.use_torrent_info === 'true' || item.use_torrent_info === 'on');
    var isProxy = (item.use_proxy === true || item.use_proxy === 'True' || item.use_proxy === 'true' || item.use_proxy === 'on');
    var isFlare = (item.use_flaresolverr === true || item.use_flaresolverr === 'True' || item.use_flaresolverr === 'true' || item.use_flaresolverr === 'on');
    var isSelenium = (item.use_selenium === true || item.use_selenium === 'True' || item.use_selenium === 'true' || item.use_selenium === 'on');

    var proxyDisplay = '<span class="text-muted">미사용</span>';
    if (isProxy) {
      proxyDisplay = item.proxy_url ? '<span class="text-warning font-weight-bold" title="' + item.proxy_url + '">개별</span>' : '<span class="text-warning font-weight-bold">기본</span>';
    }

    var boardsHtml = '';
    var boards = item.boards || [];
    if (boards.length > 0) {
      boardsHtml += '<div class="mt-1">';
      for (var b = 0; b < boards.length; b++) {
        var subTag = boards[b].subcat ? ':' + boards[b].subcat : '';
        boardsHtml += '<span class="badge badge-info mr-1 mb-1 font-weight-normal">' + boards[b].board + subTag + '</span>';
      }
      boardsHtml += '</div>';
    } else {
      boardsHtml += '<div class="text-muted small">등록된 게시판 없음</div>';
    }

    str += '<tr>';
    str += '  <td class="font-weight-bold">' + item.id + '</td>';
    str += '  <td class="text-left"><strong>' + item.site + '</strong><br>' + boardsHtml + '</td>';
    str += '  <td class="text-left small" style="line-height: 1.6;">';
    str += '    상태: ' + (isEnabled ? '<span class="text-success font-weight-bold">활성</span>' : '<span class="text-muted">중지</span>') + ' / ' + (item.interval || 1) + '회당 1회<br>';
    str += '    Proxy: ' + proxyDisplay + ' | Flare: ' + (isFlare ? '<span class="text-danger">ON</span>' : '<span class="text-muted">OFF</span>') + '<br>';
    str += '    Selenium: ' + (isSelenium ? '<span class="text-info">ON</span>' : '<span class="text-muted">OFF</span>') + ' | TorInfo: ' + (isTorrentInfo ? '<span class="text-primary">ON</span>' : '<span class="text-muted">OFF</span>');
    str += '  </td>';
    str += '  <td class="text-left">';
    if (item.last) {
      str += '    <div class="text-truncate mb-1" style="max-width: 440px;"><strong>최근:</strong> ' + item.last.title + '</div>';
      str += '    <small class="text-muted">수집: ' + item.last.created_time + '</small>';
    } else {
      str += '    <div class="text-muted small mb-1">수집된 데이터 없음</div>';
    }
    str += '    <div class="mt-2 btn-group btn-group-sm">';
    str += '      <button type="button" class="btn btn-outline-success crawler_manual_btn" data-id="' + item.id + '" data-site="' + item.site + '">즉시 실행</button>';
    str += '      <button type="button" class="btn btn-primary text-white crawler_edit_btn" data-id="' + item.id + '" data-index="' + i + '">수정</button>';
    str += '      <button type="button" class="btn btn-danger text-white remove_crawler_btn" data-id="' + item.id + '">삭제</button>';
    str += '      <button type="button" class="btn btn-secondary text-white remove_crawler_db_btn" data-id="' + item.id + '">DB 비우기</button>';
    str += '    </div>';
    str += '  </td>';
    str += '</tr>';
  }
  tbody.html(str);
}

function render_feeds(data) {
  var tbody = $('#feed_list_tbody');
  if (!data || data.length === 0) {
    tbody.html('<tr><td colspan="4" class="py-4 text-muted">등록된 피드(FEEDS)가 없습니다. 상단의 "피드 추가"를 누르세요.</td></tr>');
    return;
  }
  var str = '';
  for (var i = 0; i < data.length; i++) {
    var item = data[i];
    var isRssFile = (item.use_rss_file === true || item.use_rss_file === 'True' || item.use_rss_file === 'true' || item.use_rss_file === 'on');

    var sourcesHtml = '';
    var sources = item.sources || [];
    if (sources.length > 0) {
      sourcesHtml += '<div class="mt-1">';
      for (var s = 0; s < sources.length; s++) {
        var subTag = sources[s].subcat ? ':' + sources[s].subcat : '';
        sourcesHtml += '<span class="badge badge-info mr-1 mb-1 font-weight-normal">' + sources[s].site + ' [' + sources[s].board + subTag + ']</span>';
      }
      sourcesHtml += '</div>';
    } else {
      sourcesHtml += '<div class="text-muted small">소스 없음</div>';
    }

    var filterSummary = [];
    if (item.quality) filterSummary.push('화질: ' + item.quality);
    if (item.regexp && item.regexp.reject && item.regexp.reject.length > 0) filterSummary.push('거부(' + item.regexp.reject.length + '개)');
    if (item.regexp && item.regexp.accept && item.regexp.accept.length > 0) filterSummary.push('허용(' + item.regexp.accept.length + '개)');
    if (item.regexp && item.regexp.reject_excluding && item.regexp.reject_excluding.length > 0) filterSummary.push('필수(' + item.regexp.reject_excluding.length + '개)');
    if (item.accept_all) filterSummary.push('accept_all');
    var filterText = filterSummary.length > 0 ? filterSummary.join(' / ') : '<span class="text-muted">전역 설정 적용</span>';

    str += '<tr>';
    str += '  <td class="font-weight-bold">' + item.id + '</td>';
    str += '  <td class="text-left">';
    str += '    <strong>' + item.name + '</strong><br>' + sourcesHtml;
    str += '  </td>';
    str += '  <td class="text-left small" style="line-height: 1.6;">';
    str += '    ' + filterText + '<br>';
    str += '    공유 XML: ' + (isRssFile ? '<span class="text-success font-weight-bold">' + (item.rss_file || (item.name + '.xml')) + '</span>' : '<span class="text-muted">미생성</span>');
    str += '  </td>';
    str += '  <td class="text-left">';
    str += '    <div class="d-flex justify-content-between align-items-center">';
    str += '      <div class="btn-group btn-group-sm">';
    str += '        <button type="button" class="btn btn-primary text-white feed_edit_btn" data-id="' + item.id + '" data-index="' + i + '">수정</button>';
    str += '        <button type="button" class="btn btn-danger text-white remove_feed_btn" data-id="' + item.id + '">삭제</button>';
    if (isRssFile) {
      str += '      <button type="button" class="btn btn-outline-success generate_feed_file_btn" data-id="' + item.id + '" title="XML 파일 즉시 생성">XML 갱신</button>';
    }
    str += '      </div>';
    str += '      <a href="' + item.api + '" target="_blank" class="btn btn-sm text-white" style="background-color: #f26522; font-weight: 500;"><i class="fa fa-rss"></i> RSS 피드</a>';
    str += '    </div>';
    str += '    <div class="mt-1"><small class="text-muted" style="word-break: break-all;">' + item.api + '</small></div>';
    str += '  </td>';
    str += '</tr>';
  }
  tbody.html(str);
}

function set_modal_checkbox(id, is_checked) {
  var bool_val = (is_checked === true || is_checked === 'True' || is_checked === 'true' || is_checked === 'on');
  var elem = $('#' + id);
  elem.prop('checked', bool_val);
  try {
    if (elem.data('bs.toggle')) {
      elem.bootstrapToggle(bool_val ? 'on' : 'off');
    } else if (elem.parent().hasClass('toggle')) {
      elem.bootstrapToggle(bool_val ? 'on' : 'off');
    } else {
      elem.trigger('change');
    }
  } catch (err) {
    elem.trigger('change');
  }
}

function rules_to_string(rules) {
  if (!rules || !Array.isArray(rules)) return '';
  var lines = [];
  for (var i = 0; i < rules.length; i++) {
    var r = rules[i];
    if (typeof r === 'string') {
      lines.push(r);
    } else if (typeof r === 'object' && r !== null) {
      var keys = Object.keys(r);
      if (keys.length === 1) {
        var k = keys[0];
        var v = r[k];
        if (typeof v === 'object' && v !== null && v.from) {
          var fromStr = Array.isArray(v.from) ? '[' + v.from.join(', ') + ']' : v.from;
          lines.push(k + ': {from: ' + fromStr + '}');
        } else {
          lines.push(k);
        }
      } else {
        lines.push(JSON.stringify(r));
      }
    }
  }
  return lines.join('\n');
}

$('#crawler_use_proxy').change(function(){
  if ($(this).is(':checked')) {
    $('#modal_crawler_proxy_div').collapse('show');
  } else {
    $('#modal_crawler_proxy_div').collapse('hide');
  }
});

function apply_site_defaults_to_crawler_modal(site_name) {
  var site = current_sites.find(function(s){ return s.name === site_name; });
  if (!site) return;
  var opts = site.options || {};
  set_modal_checkbox('crawler_use_proxy', opts.use_proxy);
  set_modal_checkbox('crawler_use_flaresolverr', opts.use_flaresolverr);
  set_modal_checkbox('crawler_use_selenium', opts.use_selenium);
  set_modal_checkbox('crawler_use_torrent_info', opts.use_torrent_info);
  $('#crawler_proxy_url').val('');
  if (opts.use_proxy) {
    $('#modal_crawler_proxy_div').collapse('show');
  } else {
    $('#modal_crawler_proxy_div').collapse('hide');
  }
}

$(document).on('change', '#crawler_site', function(){
  if ($('#modal_crawler_id').val() === '-1') {
    apply_site_defaults_to_crawler_modal($(this).val());
  }
});

function render_modal_crawler_boards() {
  var tbody = $('#modal_crawler_boards_tbody');
  if (!modal_crawler_boards || modal_crawler_boards.length === 0) {
    tbody.html('<tr><td colspan="3" class="text-muted py-2">등록된 수집 대상 게시판이 없습니다.</td></tr>');
    $('#crawler_boards_json').val('[]');
    return;
  }
  var str = '';
  for (var i = 0; i < modal_crawler_boards.length; i++) {
    var b = modal_crawler_boards[i];
    str += '<tr>';
    str += '  <td><strong>' + b.board + '</strong></td>';
    str += '  <td>' + (b.subcat ? '<span class="badge badge-light">' + b.subcat + '</span>' : '<span class="text-muted">-</span>') + '</td>';
    str += '  <td><button type="button" class="btn btn-xs btn-danger text-white remove_crawler_board_btn" data-index="' + i + '">삭제</button></td>';
    str += '</tr>';
  }
  tbody.html(str);
  $('#crawler_boards_json').val(JSON.stringify(modal_crawler_boards));
}

$(document).on('click', '#add_crawler_board_btn', function(e){
  e.preventDefault();
  var boardVal = $('#modal_crawler_board_input').val().trim();
  var subcatVal = $('#modal_crawler_subcat_input').val().trim();
  if (!boardVal) {
    notify('게시판 ID를 입력하세요.', 'warning');
    return;
  }
  var exists = modal_crawler_boards.some(function(item){
    return item.board === boardVal && (item.subcat || '') === subcatVal;
  });
  if (exists) {
    notify('이미 목록에 추가된 게시판입니다.', 'info');
    return;
  }
  modal_crawler_boards.push({board: boardVal, subcat: subcatVal});
  render_modal_crawler_boards();
  $('#modal_crawler_board_input').val('');
  $('#modal_crawler_subcat_input').val('');
});

$(document).on('click', '.remove_crawler_board_btn', function(e){
  e.preventDefault();
  var idx = $(this).data('index');
  modal_crawler_boards.splice(idx, 1);
  render_modal_crawler_boards();
});

$(document).on('click', '#crawler_add_btn', function(e){
  e.preventDefault();
  if (!current_sites || current_sites.length === 0) {
    notify('등록된 사이트가 없습니다. 먼저 "사이트 관리"에서 사이트를 추가하세요.', 'warning');
    return;
  }
  var siteSelectHtml = '<select id="crawler_site" name="site" class="form-control form-control-sm">';
  for (var i in current_sites) siteSelectHtml += '<option value="' + current_sites[i].name + '">' + current_sites[i].name + '</option>';
  siteSelectHtml += '</select>';
  $('#crawler_site_select_div').html(siteSelectHtml);

  $('#crawler_modal_title').text('수집기(Crawler) 추가');
  $('#modal_crawler_id').val('-1');
  $('#modal_crawler_board_input').val('');
  $('#modal_crawler_subcat_input').val('');
  $('#crawler_interval').val('1');
  $('#crawler_delay').val('');
  $('#crawler_max_retries').val('');
  $('#crawler_proxy_url').val('');
  $('#modal_crawler_proxy_div').collapse('hide');

  set_modal_checkbox('crawler_enabled', true);
  set_modal_checkbox('crawler_use_torrent_info', false);
  set_modal_checkbox('crawler_use_proxy', false);
  set_modal_checkbox('crawler_use_flaresolverr', false);
  set_modal_checkbox('crawler_use_selenium', false);

  modal_crawler_boards = [];
  render_modal_crawler_boards();
  apply_site_defaults_to_crawler_modal(current_sites[0].name);

  $('#crawler_modal').modal('show');
});

$(document).on('click', '.crawler_edit_btn', function(e){
  e.preventDefault();
  var index = $(this).data('index');
  var item = current_crawlers[index];

  var siteSelectHtml = '<select id="crawler_site" name="site" class="form-control form-control-sm">';
  for (var i in current_sites) {
    var isSel = (current_sites[i].name === item.site) ? 'selected' : '';
    siteSelectHtml += '<option value="' + current_sites[i].name + '" ' + isSel + '>' + current_sites[i].name + '</option>';
  }
  siteSelectHtml += '</select>';
  $('#crawler_site_select_div').html(siteSelectHtml);

  $('#crawler_modal_title').text('수집기(Crawler) 수정: ' + item.site);
  $('#modal_crawler_id').val(item.id);
  $('#modal_crawler_board_input').val('');
  $('#modal_crawler_subcat_input').val('');
  $('#crawler_interval').val(item.interval || 1);
  $('#crawler_delay').val(item.delay !== undefined && item.delay !== null ? item.delay : '');
  $('#crawler_max_retries').val(item.max_retries !== undefined && item.max_retries !== null ? item.max_retries : '');
  $('#crawler_proxy_url').val(item.proxy_url || '');

  set_modal_checkbox('crawler_enabled', item.enabled);
  set_modal_checkbox('crawler_use_torrent_info', item.use_torrent_info);
  set_modal_checkbox('crawler_use_proxy', item.use_proxy);
  set_modal_checkbox('crawler_use_flaresolverr', item.use_flaresolverr);
  set_modal_checkbox('crawler_use_selenium', item.use_selenium);

  if (item.use_proxy) $('#modal_crawler_proxy_div').collapse('show');
  else $('#modal_crawler_proxy_div').collapse('hide');

  modal_crawler_boards = item.boards ? JSON.parse(JSON.stringify(item.boards)) : [];
  render_modal_crawler_boards();

  $('#crawler_modal').modal('show');
});

$(document).on('click', '#crawler_save_btn', function(e){
  e.preventDefault();
  var formData = $('#crawler_form').serialize();
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/add_crawler',
    type: "POST",
    data: formData,
    dataType: "json",
    success: function(data) {
      if (data.ret === 'success' || data.ret === 'success_update') {
        notify('수집기(Crawler) 설정이 저장되었습니다.', 'success');
        $('#crawler_modal').modal('hide');
        current_crawlers = data.crawlers || [];
        render_crawlers(current_crawlers);
      } else {
        notify('저장 실패: ' + (data.log || data.ret), 'danger');
      }
    }
  });
});

$(document).on('click', '.remove_crawler_btn', function(e){
  e.preventDefault();
  var target_id = $(this).data('id');
  if (!confirm('해당 수집기 설정과 수집된 DB 데이터를 모두 삭제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/remove_crawler',
    type: "POST",
    data: {target_id: target_id},
    dataType: "json",
    success: function(data) {
      notify('삭제되었습니다.', 'success');
      current_crawlers = data.crawlers || [];
      render_crawlers(current_crawlers);
    }
  });
});

$(document).on('click', '.remove_crawler_db_btn', function(e){
  e.preventDefault();
  var target_id = $(this).data('id');
  if (!confirm('해당 수집기의 수집 데이터(DB)만 초기화하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/remove_crawler_db',
    type: "POST",
    data: {target_id: target_id},
    dataType: "json",
    success: function(data) {
      notify('초기화되었습니다.', 'success');
      current_crawlers = data.crawlers || [];
      render_crawlers(current_crawlers);
    }
  });
});

$(document).on('click', '#crawler_reload_btn', function(e){
  e.preventDefault();
  load_all_data();
  notify('새로고침 완료', 'info');
});

function render_modal_feed_sources() {
  var tbody = $('#modal_feed_sources_tbody');
  if (!modal_feed_sources || modal_feed_sources.length === 0) {
    tbody.html('<tr><td colspan="4" class="text-muted py-2">지정된 소스 게시판이 없습니다.</td></tr>');
    $('#sources_json').val('[]');
    return;
  }

  var str = '';
  for (var i = 0; i < modal_feed_sources.length; i++) {
    var s = modal_feed_sources[i];
    str += '<tr>';
    str += '  <td><span class="badge badge-secondary">' + s.site + '</span></td>';
    str += '  <td><strong>' + s.board + '</strong></td>';
    str += '  <td>' + (s.subcat ? '<span class="badge badge-light">' + s.subcat + '</span>' : '<span class="text-muted">-</span>') + '</td>';
    str += '  <td><button type="button" class="btn btn-xs btn-danger text-white remove_feed_source_btn" data-index="' + i + '">제외</button></td>';
    str += '</tr>';
  }
  tbody.html(str);
  $('#sources_json').val(JSON.stringify(modal_feed_sources));
}

function update_crawler_source_select_options() {
  var select = $('#feed_source_crawler_select');
  select.empty();
  var count = 0;
  for (var i = 0; i < current_crawlers.length; i++) {
    var c = current_crawlers[i];
    for (var j = 0; j < (c.boards || []).length; j++) {
      var b = c.boards[j];
      var subTag = b.subcat ? ':' + b.subcat : '';
      select.append('<option value="' + c.site + '|' + b.board + '|' + (b.subcat || '') + '">[' + c.site + '] ' + b.board + subTag + '</option>');
      count++;
    }
  }
  if (count === 0) {
    select.append('<option value="">-- 등록된 수집기 게시판 없음 --</option>');
  }
}

$(document).on('click', '#add_feed_source_btn', function(e){
  e.preventDefault();
  var rawVal = $('#feed_source_crawler_select').val();
  if (!rawVal) {
    notify('추가할 수집기 게시판을 선택하세요.', 'warning');
    return;
  }
  var parts = rawVal.split('|');
  var site = parts[0];
  var board = parts[1];
  var subcat = parts[2] || '';

  var exists = modal_feed_sources.some(function(s){
    return s.site === site && s.board === board && (s.subcat || '') === subcat;
  });
  if (exists) {
    notify('이미 소스 목록에 포함되어 있습니다.', 'info');
    return;
  }

  modal_feed_sources.push({
    site: site,
    board: board,
    subcat: subcat,
    full_board_key: board + (subcat ? ':' + subcat : '')
  });
  render_modal_feed_sources();
});

$(document).on('click', '.remove_feed_source_btn', function(e){
  e.preventDefault();
  var idx = $(this).data('index');
  modal_feed_sources.splice(idx, 1);
  render_modal_feed_sources();
});

$('#use_feed_filter').change(function(){
  if ($(this).is(':checked')) {
    $('#modal_use_feed_filter_div').collapse('show');
  } else {
    $('#modal_use_feed_filter_div').collapse('hide');
  }
});

$('#use_rss_file').change(function(){
  if ($(this).is(':checked')) {
    $('#modal_use_feed_rss_file_div').collapse('show');
    if (!$('#rss_file').val()) {
      var fname = $('#feed_name').val().trim() || 'feed';
      $('#rss_file').val(fname + '.xml');
    }
  } else {
    $('#modal_use_feed_rss_file_div').collapse('hide');
  }
});

$(document).on('input change', '#feed_name', function(){
  if ($('#use_rss_file').is(':checked')) {
    var val = $(this).val().trim();
    if (val) $('#rss_file').attr('placeholder', val + '.xml');
  }
});

$(document).on('click', '#feed_add_btn', function(e){
  e.preventDefault();
  $('#feed_modal_title').text('피드(Feed) 추가');
  $('#modal_feed_id').val('-1');
  $('#feed_name').val('');
  $('#quality').val('');
  $('#filter_reject').val('');
  $('#filter_accept').val('');
  $('#filter_reject_excluding').val('');
  $('#rss_file').val('');
  $('#rss_file_path').val('');
  $('#rss_file_days').val('');
  $('#rss_file_items').val('');

  $('#modal_use_feed_filter_div').collapse('hide');
  $('#modal_use_feed_rss_file_div').collapse('hide');

  set_modal_checkbox('use_feed_filter', false);
  set_modal_checkbox('accept_all', false);
  set_modal_checkbox('use_rss_file', false);

  modal_feed_sources = [];
  render_modal_feed_sources();
  update_crawler_source_select_options();

  $('#feed_modal').modal('show');
});

$(document).on('click', '.feed_edit_btn', function(e){
  e.preventDefault();
  var index = $(this).data('index');
  var item = current_feeds[index];

  $('#feed_modal_title').text('피드(Feed) 수정: ' + item.name);
  $('#modal_feed_id').val(item.id);
  $('#feed_name').val(item.name || '');
  $('#quality').val(item.quality || '');

  var regexp = item.regexp || {};
  var reject_str = rules_to_string(regexp.reject);
  var accept_str = rules_to_string(regexp.accept);
  var reject_ex_str = rules_to_string(regexp.reject_excluding);
  var is_accept_all = (item.accept_all === true || item.accept_all === 'True' || item.accept_all === 'true' || item.accept_all === 'yes' || item.accept_all === 'on');
  var has_filter = Boolean(reject_str || accept_str || reject_ex_str || is_accept_all);

  $('#filter_reject').val(reject_str);
  $('#filter_accept').val(accept_str);
  $('#filter_reject_excluding').val(reject_ex_str);

  $('#rss_file').val(item.rss_file || '');
  $('#rss_file_path').val(item.rss_file_path || '');
  $('#rss_file_days').val(item.rss_file_days || '');
  $('#rss_file_items').val(item.rss_file_items || '');

  set_modal_checkbox('use_feed_filter', has_filter);
  set_modal_checkbox('accept_all', is_accept_all);
  set_modal_checkbox('use_rss_file', item.use_rss_file);

  if (has_filter) $('#modal_use_feed_filter_div').collapse('show');
  else $('#modal_use_feed_filter_div').collapse('hide');

  if (item.use_rss_file) $('#modal_use_feed_rss_file_div').collapse('show');
  else $('#modal_use_feed_rss_file_div').collapse('hide');

  modal_feed_sources = item.sources ? JSON.parse(JSON.stringify(item.sources)) : [];
  render_modal_feed_sources();
  update_crawler_source_select_options();

  $('#feed_modal').modal('show');
});

$(document).on('click', '#feed_save_btn', function(e){
  e.preventDefault();
  var formData = $('#feed_form').serialize();
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/add_feed',
    type: "POST",
    data: formData,
    dataType: "json",
    success: function(data) {
      if (data.ret === 'success' || data.ret === 'success_update') {
        notify('피드(Feed) 설정이 저장되었습니다.', 'success');
        $('#feed_modal').modal('hide');
        current_feeds = data.feeds || [];
        render_feeds(current_feeds);
      } else {
        notify('저장 실패: ' + (data.log || data.ret), 'danger');
      }
    }
  });
});

$(document).on('click', '.remove_feed_btn', function(e){
  e.preventDefault();
  var target_id = $(this).data('id');
  if (!confirm('해당 피드 설정을 삭제하시겠습니까? (수집된 DB 원본 데이터는 보존됩니다)')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/remove_feed',
    type: "POST",
    data: {target_id: target_id},
    dataType: "json",
    success: function(data) {
      notify('피드가 삭제되었습니다.', 'success');
      current_feeds = data.feeds || [];
      render_feeds(current_feeds);
    }
  });
});

$(document).on('click', '.generate_feed_file_btn', function(e){
  e.preventDefault();
  var target_id = $(this).data('id');
  notify('공유용 XML 파일 생성을 요청했습니다...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/generate_feed_file',
    type: "POST",
    data: {target_id: target_id},
    dataType: "json",
    success: function(data) {
      if (data.ret === 'success') {
        notify('XML 파일이 성공적으로 생성 및 갱신되었습니다.', 'success');
      } else {
        notify('파일 생성 실패: ' + (data.log || data.ret), 'warning');
      }
    }
  });
});

$(document).on('click', '#feed_reload_btn', function(e){
  e.preventDefault();
  load_all_data();
  notify('새로고침 완료', 'info');
});

$(document).on('click', '#my_site_add_btn', function(e){
  e.preventDefault();
  $('#site_modal_title').text('사이트 직접 추가');
  $('#modal_site_id').val('-1');
  var default_json = '{\n  "NAME": "",\n  "TORRENT_SITE_URL": "https://",\n  "XPATH_LIST_TAG": {\n    "XPATH": "",\n    "TITLE_XPATH": ""\n  }\n}';
  $('#modal_site_json').val(default_json);
  if (json_editor) json_editor.setValue(default_json, -1);
  $('#modal_log').val('');
  $('#site_modal').modal('show');
  setTimeout(function(){ if (json_editor) json_editor.resize(); }, 200);
});

$(document).on('click', '.site_edit_btn', function(e){
  e.preventDefault();
  var index = $(this).data('index');
  var target = current_sites[index];
  $('#site_modal_title').text('사이트 규칙 수정: ' + target.name);
  $('#modal_site_id').val(target.id);
  var json_str = JSON.stringify(target.info, null, 2);
  $('#modal_site_json').val(json_str);
  if (json_editor) json_editor.setValue(json_str, -1);
  $('#modal_log').val('');
  $('#site_modal').modal('show');
  setTimeout(function(){ if (json_editor) json_editor.resize(); }, 200);
});

$(document).on('click', '#modal_json_format_btn', function(e){
  e.preventDefault();
  if (!json_editor) return;
  try {
    var raw = json_editor.getValue();
    var parsed = JSON.parse(raw);
    var formatted = JSON.stringify(parsed, null, 2);
    json_editor.setValue(formatted, -1);
    $('#modal_log').val('JSON 포맷 정렬 완료');
    notify('JSON 코드가 깔끔하게 정렬되었습니다.', 'info');
  } catch(err) {
    $('#modal_log').val('정렬 불가 (문법 오류): ' + err.message);
    notify('JSON 문법 오류로 정렬할 수 없습니다.', 'warning');
  }
});

$(document).on('click', '#modal_json_test_btn', function(e){
  e.preventDefault();
  if (!json_editor) return;
  try {
    var parsed = JSON.parse(json_editor.getValue());
    var formatted = JSON.stringify(parsed, null, 2);
    json_editor.setValue(formatted, -1);
    $('#modal_site_json').val(formatted);
    $('#modal_log').val('정상 포맷입니다.');
    notify('올바른 JSON 포맷입니다.', 'success');
  } catch(err) {
    $('#modal_log').val('오류: ' + err.message);
    notify('JSON 오류', 'warning');
  }
});

$(document).on('click', '#modal_save_btn', function(e){
  e.preventDefault();
  if (json_editor) {
    $('#modal_site_json').val(json_editor.getValue());
  }
  var formData = $('#site_form').serialize();
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/site_edit',
    type: "POST",
    data: formData,
    dataType: "json",
    success: function(data) {
      if (data.ret === 'edit_success' || data.ret === 'add_success') {
        notify('사이트 규칙이 저장되었습니다.', 'success');
        $('#site_modal').modal('hide');
        current_sites = data.site;
        render_sites(current_sites);
      } else {
        notify(data.log || '저장 실패', 'danger');
      }
    }
  });
});

$(document).on('keydown', 'input[id^="board_id_"]', function(e){
  if (e.key === 'Enter' || e.keyCode === 13) {
    e.preventDefault();
    var site_id = $(this).attr('id').replace('board_id_', '');
    $('.test_btn[data-site_id="' + site_id + '"]').trigger('click');
  }
});

// 즉시 실행 요청 함수 (비동기 쏘기 전용, 스피너 즉시 강제 해제)
function request_manual_crawl(crawler_id) {
  var postData = {
    command: 'manual_crawl'
  };
  if (crawler_id) {
    postData.crawler_id = crawler_id;
  }
  var targetName = crawler_id ? '개별 수집기(ID: ' + crawler_id + ')' : '전체 수집기';
  notify(targetName + ' 즉시 실행 요청 중...', 'info');

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/manual_crawl',
    type: "POST",
    data: postData,
    dataType: "json",
    success: function(data) {
      if (data && data.ret === 'success') {
        notify(data.msg || targetName + ' 즉시 실행을 시작했습니다.', 'success');
      } else if (data && data.ret === 'running') {
        notify(data.msg || '현재 다른 수집 작업이 이미 실행 중입니다.', 'warning');
      } else {
        notify((data && data.msg) || '즉시 실행 요청 실패', 'warning');
      }
    },
    error: function() {
      notify('서버 통신 실패', 'danger');
    },
    complete: function() {
      // FF 프레임워크 전역 스피너 즉시 강제 종료
      try { if (typeof m_loading_hide === 'function') m_loading_hide(); } catch(e){}
      try { if (typeof m_modal_loading_hide === 'function') m_modal_loading_hide(); } catch(e){}
      try { $('#loading').hide(); } catch(e){}
    }
  });
}

// 상단 [즉시 실행] 버튼 핸들러
$(document).on('click', '#btn_manual_crawl', function(e){
  e.preventDefault();
  request_manual_crawl(null);
  setTimeout(function(){
    try { if (typeof m_loading_hide === 'function') m_loading_hide(); } catch(e){}
    try { if (typeof m_modal_loading_hide === 'function') m_modal_loading_hide(); } catch(e){}
    try { $('#loading').hide(); } catch(e){}
  }, 100);
});

// 개별 크롤러 [즉시 실행] 버튼 핸들러
$(document).on('click', '.crawler_manual_btn', function(e){
  e.preventDefault();
  var cId = $(this).data('id');
  request_manual_crawl(cId);
});

// 수집 테스트 요청 함수
function request_board_test(site_id, board_id) {
  notify('게시판 [' + board_id + '] 수집 테스트 시작...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/test',
    type: "POST",
    data: {site_id: site_id, board_id: board_id},
    dataType: "json",
    success: function(data) {
      if (data && data.ret === 'fail') {
        notify(data.log || '테스트 실패', 'warning');
        return;
      }
      $('#test_modal_title').text('수집 테스트 결과 (전체 ' + data.length + '개)');
      $('#test_modal_body').val(JSON.stringify(data, null, 2));
      $('#test_result_modal').modal('show');
    }
  });
}

$(document).on('click', '.test_btn', function(e){
  e.preventDefault();
  var site_id = $(this).data('site_id');
  var board_id = $('#board_id_' + site_id).val().trim();
  if (!board_id) { notify('게시판 ID를 입력하세요.', 'warning'); return; }
  request_board_test(site_id, board_id);
});

$(document).on('click', '.remove_site_btn', function(e){
  e.preventDefault();
  var site_id = $(this).data('site_id');
  if (!confirm('사이트를 삭제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/site_delete',
    type: "POST",
    data: {site_id: site_id},
    dataType: "json",
    success: function(data) {
      notify('삭제되었습니다.', 'success');
      current_sites = data.site;
      render_sites(current_sites);
    }
  });
});

$(document).on('click', '#custom_script_manage_btn', function(e){
  e.preventDefault();
  load_custom_script_list();
  $('#custom_script_modal').modal('show');
  setTimeout(function(){ if (python_editor) python_editor.resize(); }, 200);
});

function load_custom_script_list(selected_name) {
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/custom_script_list',
    type: "POST",
    dataType: "json",
    success: function(data) {
      var files = data.files || [];
      var select = $('#custom_script_select');
      select.empty();
      select.append('<option value="">-- 파일 선택 --</option>');
      for (var i = 0; i < files.length; i++) {
        var isSel = (files[i] === selected_name) ? 'selected' : '';
        select.append('<option value="' + files[i] + '" ' + isSel + '>' + files[i] + '</option>');
      }
      if (selected_name) {
        select.val(selected_name).trigger('change');
      } else if (files.length > 0) {
        select.val(files[0]).trigger('change');
      } else {
        $('#custom_script_new_btn').trigger('click');
      }
    }
  });
}

$(document).on('change', '#custom_script_select', function(){
  var filename = $(this).val();
  if (!filename) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/custom_script_read',
    type: "POST",
    data: {filename: filename},
    dataType: "json",
    success: function(data) {
      if (data.ret === 'success') {
        $('#custom_script_name').val(data.filename);
        $('#custom_script_code').val(data.content);
        if (python_editor) python_editor.setValue(data.content, -1);
        $('#custom_script_status').text('불러오기 완료');
      } else {
        notify('파일 로드 실패: ' + (data.log || data.ret), 'warning');
      }
    }
  });
});

$(document).on('click', '#custom_script_new_btn', function(e){
  e.preventDefault();
  $('#custom_script_select').val('');
  $('#custom_script_name').val('site_new.py');
  $('#custom_script_code').val(SCRIPT_SKELETON);
  if (python_editor) python_editor.setValue(SCRIPT_SKELETON, -1);
  $('#custom_script_status').text('새 스크립트 템플릿 로드');
});

$(document).on('click', '#custom_script_save_btn', function(e){
  e.preventDefault();
  var filename = $('#custom_script_name').val().trim();
  var content = python_editor ? python_editor.getValue() : $('#custom_script_code').val();
  if (!filename) {
    notify('파일명을 입력하세요.', 'warning');
    return;
  }
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/custom_script_save',
    type: "POST",
    data: {filename: filename, content: content},
    dataType: "json",
    success: function(data) {
      if (data.ret === 'success') {
        notify('스크립트 저장 및 사이트 템플릿 등록이 완료되었습니다.', 'success');
        $('#custom_script_status').text('저장 및 사이트 자동 등록 완료 (' + filename + ')');
        load_custom_script_list(data.filename);
        if (data.site) {
          current_sites = data.site;
          render_sites(current_sites);
        }
      } else {
        notify('저장 실패: ' + (data.log || data.ret), 'danger');
      }
    }
  });
});

$(document).on('click', '#custom_script_delete_btn', function(e){
  e.preventDefault();
  var filename = $('#custom_script_name').val().trim();
  if (!filename) return;
  if (!confirm('[' + filename + '] 스크립트를 완전히 삭제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/custom_script_delete',
    type: "POST",
    data: {filename: filename},
    dataType: "json",
    success: function(data) {
      if (data.ret === 'success') {
        notify('삭제되었습니다.', 'success');
        load_custom_script_list();
      } else {
        notify('삭제 실패: ' + (data.log || data.ret), 'danger');
      }
    }
  });
});

$(document).on('click', '#custom_script_upload_trigger_btn', function(e){
  e.preventDefault();
  $('#custom_script_file_input').val('').click();
});

$(document).on('change', '#custom_script_file_input', function(){
  var file = this.files[0];
  if (!file) return;
  var formData = new FormData();
  formData.append('command', 'custom_script_upload');
  formData.append('file', file);
  notify('파일 업로드 중...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/custom_script_upload',
    type: "POST",
    data: formData,
    processData: false,
    contentType: false,
    dataType: "json",
    success: function(data) {
      if (data.ret === 'success') {
        notify('스크립트 업로드 및 사이트 템플릿 등록이 완료되었습니다.', 'success');
        load_custom_script_list(data.filename);
        if (data.site) {
          current_sites = data.site;
          render_sites(current_sites);
        }
      } else {
        notify('업로드 실패: ' + (data.log || data.ret), 'danger');
      }
    }
  });
});

$(document).on('click', 'nav a, .navbar a', function(){
  var href = $(this).attr('href') || '';
  if (href.indexOf('/' + package_name + '/feed') !== -1 && href.indexOf('manual') === -1 && href.indexOf('log') === -1) {
    var lastFeedPage = localStorage.getItem('feeder_last_sub_feed');
    if (lastFeedPage && lastFeedPage !== 'setting') {
      $(this).attr('href', '/' + package_name + '/feed/' + lastFeedPage);
    }
  } else if (href.indexOf('/' + package_name + '/download') !== -1) {
    var lastDlPage = localStorage.getItem('feeder_last_sub_download');
    if (lastDlPage && lastDlPage !== 'setting') {
      $(this).attr('href', '/' + package_name + '/download/' + lastDlPage);
    }
  }
});
