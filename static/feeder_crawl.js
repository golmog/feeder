// =============================================================================
// CRAWL 모듈 전용 스크립트 (List & Setting 통합)
// =============================================================================
var site_info = null;
var current_sites = [];
var current_crawlers = [];
var modal_crawler_boards = [];

var json_editor = null;
var python_editor = null;

var SCRIPT_SKELETON = '# -*- coding: utf-8 -*-\n' +
  '"""\n' +
  '커스텀 사이트 훅 스크립트\n' +
  '"""\n' +
  'from feeder.setup import logger\n\n' +
  'class CustomSiteHook:\n' +
  '    SITE_NAME = "custom_site"\n\n' +
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
  '            "커스텀 훅 연동 사이트"\n' +
  '        ]\n' +
  '    }\n\n' +
  '    @classmethod\n' +
  '    def on_init_session(cls, site_info: dict, scheduler_cfg) -> None:\n' +
  '        pass\n\n' +
  '    @classmethod\n' +
  '    def on_extract_detail(cls, detail_html: str, item: dict, site_info: dict, scheduler_cfg) -> list[str]:\n' +
  '        return []\n';

// -----------------------------------------------------------------------------
// 초기화 분기 (List 화면 vs Setting 화면 자동 감지)
// -----------------------------------------------------------------------------
$(document).ready(function () {
  try {
    sync_feeder_header_navbar();
  } catch (err) {}

  // List 화면 진입 시
  if ($('#list_div').length > 0) {
    try {
      localStorage.setItem('feeder_last_crawl_page', 'list');
      sync_feeder_header_navbar();
    } catch (err) {}

    var saved_site = localStorage.getItem(sub + '_site_select') || 'all';
    var saved_status = localStorage.getItem(sub + '_status_filter') || 'all';
    var saved_order = localStorage.getItem(sub + '_order') || 'desc';
    var saved_size = localStorage.getItem(sub + '_page_size') || '25';
    var saved_search_select = localStorage.getItem(sub + '_search_select') || 'title';
    var saved_word = localStorage.getItem(sub + '_search_word') || '';
    var saved_page = localStorage.getItem(sub + '_current_page') || '1';

    if ($('#site_select').length > 0) $('#site_select').val(saved_site);
    $('#status_filter').val(saved_status);
    $('#order').val(saved_order);
    $('#page_size').val(saved_size);
    $('#search_select').val(saved_search_select);
    $('#search_word').val(saved_word);

    if (typeof server_site_info !== 'undefined' && server_site_info && server_site_info.site) {
      build_search_form(server_site_info);
    }
    load_download_profiles();
    window.globalRequestSearch(saved_page);
  }

  // Setting 화면 진입 시
  if ($('#site_list_tbody').length > 0 || $('#crawler_list_tbody').length > 0) {
    try {
      localStorage.setItem('feeder_last_crawl_page', 'setting');
      sync_feeder_header_navbar();
    } catch (err) {}

    use_collapse('crawl_use_proxy');
    use_collapse('crawl_use_flaresolverr');
    use_collapse('crawl_use_selenium');
    use_collapse('crawl_use_torrent_info');
    toggle_qb_setting();
    load_all_crawl_data();
    init_crawl_ace_editors();
    restore_active_subtab('crawl');
    setTimeout(function () { restore_active_subtab('crawl'); }, 80);
  }
});

// -----------------------------------------------------------------------------
// [CRAWL: LIST 화면 로직]
// -----------------------------------------------------------------------------
$('#search').click(function (e) {
  e.preventDefault();
  window.globalRequestSearch('1', false);
});

$('#search_word').keydown(function (e) {
  if (e.which === 13) {
    e.preventDefault();
    window.globalRequestSearch('1', false);
  }
});

$('#status_filter, #order, #page_size, #search_select').change(function () {
  if ($('#list_div').length > 0) {
    window.globalRequestSearch('1', false);
  }
});

$('#reset_btn').click(function (e) {
  e.preventDefault();
  if ($('#list_div').length === 0) return;

  $('#site_select').val('all').trigger('change');
  $('#order').val('desc');
  $('#page_size').val('25');
  $('#search_select').val('title');
  $('#status_filter').val('all');
  $('#search_word').val('');

  localStorage.removeItem(sub + '_search_word');
  localStorage.setItem(sub + '_site_select', 'all');
  localStorage.setItem(sub + '_board_select', 'all');
  localStorage.setItem(sub + '_order', 'desc');
  localStorage.setItem(sub + '_page_size', '25');
  localStorage.setItem(sub + '_search_select', 'title');
  localStorage.setItem(sub + '_status_filter', 'all');
  localStorage.setItem(sub + '_current_page', '1');

  window.globalRequestSearch('1', false);
});

$('body').on('change', '#site_select', function (e) {
  e.preventDefault();
  var selected_site = $(this).val();
  localStorage.setItem(sub + '_site_select', selected_site);
  localStorage.setItem(sub + '_board_select', 'all');
  update_board_select(selected_site);
  window.globalRequestSearch('1', false);
});

$('body').on('change', '#board_select', function (e) {
  e.preventDefault();
  localStorage.setItem(sub + '_board_select', $(this).val());
  window.globalRequestSearch('1', false);
});

function build_search_form(data) {
  if (!data || !data.site) return;
  site_info = data;

  var saved_site = localStorage.getItem(sub + '_site_select') || 'all';
  var current_val = $('#site_select').val() || saved_site;

  var site_str = '<select id="site_select" name="site_select" class="form-control form-control-sm"><option value="all">전체 사이트</option>';
  for (var i = 0; i < data.site.length; i++) {
    var sName = data.site[i];
    var isSel = (sName === current_val) ? 'selected' : '';
    site_str += '<option value="' + sName + '" ' + isSel + '>' + sName + '</option>';
  }
  site_str += '</select>';
  $('#site_select_div').html(site_str);

  update_board_select($('#site_select').val());
}

function update_board_select(selected_site) {
  var saved_board = localStorage.getItem(sub + '_board_select') || 'all';
  var str = '<select id="board_select" name="board_select" class="form-control form-control-sm"';

  if (!selected_site || selected_site === 'all') {
    str += ' disabled><option value="all">전체 게시판</option></select>';
    $('#board_select_div').html(str);
    return;
  }

  str += '><option value="all">전체 게시판</option>';
  if (site_info && site_info.board && site_info.board[selected_site]) {
    var b_list = site_info.board[selected_site];
    for (var i = 0; i < b_list.length; i++) {
      var item = b_list[i];
      var bKey = (typeof item === 'object' && item !== null && item.key) ? item.key : item;
      var bName = (typeof item === 'object' && item !== null && item.name) ? item.name : bKey;
      if (bKey && bKey !== 'None' && bKey !== 'null') {
        var isSel = (bKey === saved_board) ? 'selected' : '';
        str += '<option value="' + bKey + '" ' + isSel + '>' + bName + '</option>';
      }
    }
  }
  str += '</select>';
  $('#board_select_div').html(str);
}

function make_list(data) {
  try {
    if (data && data.info) {
      build_search_form(data.info);
    } else if (current_data && current_data.info) {
      build_search_form(current_data.info);
    }

    var list_items = Array.isArray(data) ? data : (data && data.list ? data.list : []);
    if (!list_items || list_items.length === 0) {
      document.getElementById('list_div').innerHTML = '<div class="text-center py-4 text-muted">수집된 콘텐츠가 없습니다.</div>';
      return;
    }

    var str = '';
    for (var i = 0; i < list_items.length; i++) {
      var item = list_items[i];
      str += j_row_start();
      str += j_col(1, item.id);

      var site_col = '<small class="text-muted">' + (item.created_time || '') + '</small><br>';
      site_col += '<span class="badge badge-info">' + item.site + '</span> ';

      var bStr = String(item.board || '');
      if (bStr && bStr !== 'None' && bStr !== 'null') {
        site_col += '<span class="badge badge-secondary">' + bStr + '</span>';
      } else {
        site_col += '<span class="badge badge-secondary">기본</span>';
      }

      if (item.broadcast_status === 'LOGIN_REQUIRED') {
        site_col += ' <span class="badge badge-warning">로그인 필요</span>';
      }
      str += j_col(2, site_col);

      var detail_col = '<div class="mb-2"><strong><a href="' + item.url + '" target="_blank">' + item.title + '</a></strong></div>';

      if (item.magnet && item.magnet.length > 0) {
        for (var j = 0; j < item.magnet.length; j++) {
          var mag = item.magnet[j];
          var mag_info = '';
          var t_info = item.torrent_info;
          var is_ed2k = String(mag).toLowerCase().startsWith('ed2k://');
          var link_badge = is_ed2k ? '<span class="badge badge-warning mr-1">ed2k</span>' : '<span class="badge badge-primary mr-1">magnet</span>';
          var copy_btn_text = is_ed2k ? 'ed2k 복사' : '마그넷 복사';

          var dl_info = item.download_info;
          var dl_badge = '';
          if (dl_info) {
            if (dl_info.status === 'completed') dl_badge = '<span class="badge badge-success mr-1">다운로드 완료</span>';
            else if (dl_info.status === 'failed') dl_badge = '<span class="badge badge-danger mr-1">다운로드 실패</span>';
            else dl_badge = '<span class="badge badge-primary mr-1">다운로드 진행중</span>';
          }

          if (typeof t_info === 'string') {
            try { t_info = JSON.parse(t_info); } catch (e) { t_info = null; }
          }

          if (t_info && Array.isArray(t_info)) {
            for (var k = 0; k < t_info.length; k++) {
              if (t_info[k].info_hash && mag.indexOf(t_info[k].info_hash) !== -1) {
                mag_info += '<div class="text-success font-weight-bold small mb-1">' + t_info[k].name + '</div>';
              }
            }
          }

          detail_col += '<div class="p-2 mb-2 rounded" style="background: rgba(128,128,128,0.1); font-size: 0.85rem;">';
          detail_col +=   mag_info;
          detail_col += '  <div class="text-truncate mb-2">' + link_badge + dl_badge + '<small><a href="' + mag + '">' + mag + '</a></small></div>';
          detail_col += '  <div class="btn-group btn-group-sm">';
          detail_col += '    <button type="button" class="btn btn-sm btn-secondary copy_magnet_btn text-white" data-hash="' + mag + '"><i class="fa fa-copy mr-1"></i>' + copy_btn_text + '</button>';
          detail_col += '    <button type="button" class="btn btn-sm btn-outline-info direct_download_btn" data-hash="' + mag + '" data-title="' + clean_title_attr(item.title) + '" data-feed="' + (item.board || '') + '"><i class="fa fa-download mr-1"></i>다운로드 추가</button>';
          if (is_torrent_info_installed && !is_ed2k) {
            detail_col += '  <button type="button" class="btn btn-sm btn-info global_torrent_info_btn text-white" data-hash="' + mag + '">Torrent Info</button>';
          }
          detail_col += '  </div>';
          detail_col += '</div>';
        }
      }

      if (item.files && item.files.length > 0) {
        var clean_ddns = (typeof ddns !== 'undefined' && ddns) ? ddns.replace(/\/+$/, '') : '';
        for (var f_idx = 0; f_idx < item.files.length; f_idx++) {
          var file_url = clean_ddns + '/' + package_name + '/api/crawl/download?id=' + item.id + '_' + f_idx + '&apikey=' + apikey;
          var filename = item.files[f_idx][1] || '첨부파일';
          detail_col += '<div class="p-2 mb-1 rounded d-flex justify-content-between align-items-center" style="background: rgba(128,128,128,0.06); font-size: 0.85rem;">';
          detail_col += '  <span><i class="fa fa-file mr-1"></i><a href="' + file_url + '">' + filename + '</a></span>';
          detail_col += '  <a href="' + file_url + '" class="btn btn-sm btn-primary text-white" download><i class="fa fa-download mr-1"></i>직접 다운로드</a>';
          detail_col += '</div>';
        }
      }

      if ((!item.magnet || item.magnet.length === 0) && (!item.files || item.files.length === 0)) {
        if (item.broadcast_status === 'LOGIN_REQUIRED') {
          detail_col += '<div class="p-2 mb-1 rounded small text-muted" style="background: rgba(255, 193, 7, 0.08); border-left: 3px solid #ffc107;">';
          detail_col += '  <i class="fa fa-lock mr-1 text-warning"></i>사이트 첨부파일 다운로드 권한(로그인)이 필요하여 마그넷 수집이 제외된 항목입니다.';
          detail_col += '</div>';
        } else {
          detail_col += '<div class="p-2 mb-1 rounded small text-muted" style="background: rgba(128, 128, 128, 0.06);">';
          detail_col += '  <i class="fa fa-info-circle mr-1"></i>수집된 마그넷 또는 첨부파일이 없습니다.';
          detail_col += '</div>';
        }
      }

      str += j_col(9, detail_col);
      str += j_row_end();
      if (i !== list_items.length - 1) str += j_hr();
    }
    document.getElementById('list_div').innerHTML = str;
  } catch (err) {
    console.error('make_list 렌더링 오류:', err);
    document.getElementById('list_div').innerHTML = '<div class="alert alert-danger m-3">목록 렌더링 중 오류가 발생했습니다: ' + err.message + '</div>';
  }
}

// -----------------------------------------------------------------------------
// [CRAWL: SETTING 화면 로직]
// -----------------------------------------------------------------------------
function init_crawl_ace_editors() {
  if ($('#modal_site_json_editor').length && !json_editor && window.ace) {
    json_editor = ace.edit('modal_site_json_editor');
    json_editor.setTheme('ace/theme/monokai');
    json_editor.session.setMode('ace/mode/json');
    json_editor.setFontSize(13);
    json_editor.setShowPrintMargin(false);
    json_editor.session.setTabSize(2);
    json_editor.session.setUseSoftTabs(true);
    json_editor.session.setUseWrapMode(true);
    feeder_ace_instances.push(json_editor);
  }

  if ($('#custom_script_code_editor').length && !python_editor && window.ace) {
    python_editor = ace.edit('custom_script_code_editor');
    python_editor.setTheme('ace/theme/monokai');
    python_editor.session.setMode('ace/mode/python');
    python_editor.setFontSize(13);
    python_editor.setShowPrintMargin(false);
    python_editor.session.setTabSize(4);
    python_editor.session.setUseSoftTabs(true);
    python_editor.session.setUseWrapMode(false);
    feeder_ace_instances.push(python_editor);
  }
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

$(document).on('change', '#modal_site_json_wrap_chk', function () {
  if (json_editor) json_editor.session.setUseWrapMode($(this).is(':checked'));
});

$(document).on('change', '#custom_script_wrap_chk', function () {
  if (python_editor) python_editor.session.setUseWrapMode($(this).is(':checked'));
});

function toggle_qb_setting() {
  var method = $('input[name="crawl_torrent_info_method"]:checked').val();
  if (method === 'qbittorrent') $('#qb_setting_div').show();
  else $('#qb_setting_div').hide();
}

$('input[name="crawl_torrent_info_method"]').change(toggle_qb_setting);
$('#crawl_use_proxy').change(function () { use_collapse('crawl_use_proxy'); });
$('#crawl_use_flaresolverr').change(function () { use_collapse('crawl_use_flaresolverr'); });
$('#crawl_use_selenium').change(function () { use_collapse('crawl_use_selenium'); });
$('#crawl_use_torrent_info').change(function () { use_collapse('crawl_use_torrent_info'); });

function load_all_crawl_data() {
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/load_site',
    type: 'POST',
    dataType: 'json',
    success: function (data) {
      current_sites = data.site || [];
      render_sites(current_sites);
    }
  });

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/load_crawlers',
    type: 'POST',
    dataType: 'json',
    success: function (data) {
      current_crawlers = data.crawlers || [];
      render_crawlers(current_crawlers);
    }
  });
}

function render_sites(data) {
  var tbody = $('#site_list_tbody');
  if (!tbody.length) return;
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

    var pBadge = '<span class="badge ' + pClass + ' site_opt_toggle mr-1" data-id="' + item.id + '" data-opt="use_proxy">Proxy ' + (opts.use_proxy ? 'ON' : 'OFF') + '</span>';
    var fBadge = '<span class="badge ' + fClass + ' site_opt_toggle mr-1" data-id="' + item.id + '" data-opt="use_flaresolverr">Flare ' + (opts.use_flaresolverr ? 'ON' : 'OFF') + '</span>';
    var sBadge = '<span class="badge ' + sClass + ' site_opt_toggle mr-1" data-id="' + item.id + '" data-opt="use_selenium">Selenium ' + (opts.use_selenium ? 'ON' : 'OFF') + '</span>';
    var tBadge = '<span class="badge ' + tClass + ' site_opt_toggle" data-id="' + item.id + '" data-opt="use_torrent_info">TorInfo ' + (opts.use_torrent_info ? 'ON' : 'OFF') + '</span>';

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

$(document).on('click', '.site_opt_toggle', function (e) {
  e.preventDefault();
  var site_id = $(this).data('id');
  var opt = $(this).data('opt');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/site_toggle_option',
    type: 'POST',
    data: { site_id: site_id, option: opt },
    dataType: 'json',
    success: function (data) {
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
  if (!tbody.length) return;
  if (!data || data.length === 0) {
    tbody.html('<tr><td colspan="4" class="py-4 text-muted">등록된 수집기가 없습니다.</td></tr>');
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

function set_modal_checkbox(id, is_checked) {
  var bool_val = (is_checked === true || is_checked === 'True' || is_checked === 'true' || is_checked === 'on');
  var elem = $('#' + id);
  elem.prop('checked', bool_val);
  try {
    if (elem.data('bs.toggle') || elem.parent().hasClass('toggle')) {
      elem.bootstrapToggle(bool_val ? 'on' : 'off');
    } else {
      elem.trigger('change');
    }
  } catch (err) {
    elem.trigger('change');
  }
}

$('#crawler_use_proxy').change(function () {
  if ($(this).is(':checked')) $('#modal_crawler_proxy_div').collapse('show');
  else $('#modal_crawler_proxy_div').collapse('hide');
});

function apply_site_defaults_to_crawler_modal(site_name) {
  var site = current_sites.find(function (s) { return s.name === site_name; });
  if (!site) return;
  var opts = site.options || {};
  set_modal_checkbox('crawler_use_proxy', opts.use_proxy);
  set_modal_checkbox('crawler_use_flaresolverr', opts.use_flaresolverr);
  set_modal_checkbox('crawler_use_selenium', opts.use_selenium);
  set_modal_checkbox('crawler_use_torrent_info', opts.use_torrent_info);
  $('#crawler_proxy_url').val('');
  if (opts.use_proxy) $('#modal_crawler_proxy_div').collapse('show');
  else $('#modal_crawler_proxy_div').collapse('hide');
}

$(document).on('change', '#crawler_site', function () {
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
  var isEditMode = ($('#modal_crawler_id').val() !== '-1');
  var str = '';
  for (var i = 0; i < modal_crawler_boards.length; i++) {
    var b = modal_crawler_boards[i];
    str += '<tr>';
    str += '  <td><strong>' + b.board + '</strong></td>';
    str += '  <td>' + (b.subcat ? '<span class="badge badge-secondary">' + b.subcat + '</span>' : '<span class="text-muted">-</span>') + '</td>';
    str += '  <td>';
    str += '    <div class="btn-group btn-group-sm">';
    if (isEditMode) {
      str += '      <button type="button" class="btn btn-xs btn-outline-warning clear_crawler_board_db_btn mr-1" data-index="' + i + '" title="해당 게시판의 수집 데이터만 비우기">DB 비우기</button>';
    }
    str += '      <button type="button" class="btn btn-xs btn-danger text-white remove_crawler_board_btn" data-index="' + i + '">삭제</button>';
    str += '    </div>';
    str += '  </td>';
    str += '</tr>';
  }
  tbody.html(str);
  $('#crawler_boards_json').val(JSON.stringify(modal_crawler_boards));
}

$(document).on('click', '#add_crawler_board_btn', function (e) {
  e.preventDefault();
  var boardVal = $('#modal_crawler_board_input').val().trim();
  var subcatVal = $('#modal_crawler_subcat_input').val().trim();
  if (!boardVal) {
    notify('게시판 ID를 입력하세요.', 'warning');
    return;
  }
  var exists = modal_crawler_boards.some(function (item) {
    return item.board === boardVal && (item.subcat || '') === subcatVal;
  });
  if (exists) {
    notify('이미 목록에 추가된 게시판입니다.', 'info');
    return;
  }
  modal_crawler_boards.push({ board: boardVal, subcat: subcatVal });
  render_modal_crawler_boards();
  $('#modal_crawler_board_input').val('');
  $('#modal_crawler_subcat_input').val('');
});

$(document).on('click', '.clear_crawler_board_db_btn', function (e) {
  e.preventDefault();
  var idx = $(this).data('index');
  var b = modal_crawler_boards[idx];
  var site = $('#crawler_site').val();
  var boardDisplay = b.board + (b.subcat ? ':' + b.subcat : '');

  if (!confirm('[' + site + ' - ' + boardDisplay + '] 게시판의 수집 데이터(DB)를 비우시겠습니까?\n(게시판 설정은 유지됩니다)')) return;

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/clear_board_db',
    type: 'POST',
    data: { site: site, board: b.board, subcat: b.subcat || '' },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify('[' + boardDisplay + '] 게시판의 수집 데이터가 삭제되었습니다.', 'success');
      } else {
        notify('DB 비우기 실패: ' + (data.msg || data.ret), 'warning');
      }
    }
  });
});

$(document).on('click', '.remove_crawler_board_btn', function (e) {
  e.preventDefault();
  var idx = $(this).data('index');
  var b = modal_crawler_boards[idx];
  var site = $('#crawler_site').val();
  var boardDisplay = b.board + (b.subcat ? ':' + b.subcat : '');
  var isEditMode = ($('#modal_crawler_id').val() !== '-1');

  if (isEditMode) {
    if (!confirm('[' + site + ' - ' + boardDisplay + '] 게시판을 수집 대상에서 삭제하시겠습니까?\n\n※ 경고: 해당 게시판의 모든 수집 데이터(DB)도 함께 영구 삭제됩니다.')) {
      return;
    }
    $.ajax({
      url: '/' + package_name + '/ajax/' + sub + '/clear_board_db',
      type: 'POST',
      data: { site: site, board: b.board, subcat: b.subcat || '' },
      dataType: 'json',
      success: function () {
        modal_crawler_boards.splice(idx, 1);
        render_modal_crawler_boards();
        notify('[' + boardDisplay + '] 게시판 및 수집 데이터가 삭제되었습니다.', 'info');
      }
    });
  } else {
    modal_crawler_boards.splice(idx, 1);
    render_modal_crawler_boards();
  }
});

$(document).on('click', '#crawler_add_btn', function (e) {
  e.preventDefault();
  if (!current_sites || current_sites.length === 0) {
    notify('등록된 사이트가 없습니다.', 'warning');
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

$(document).on('click', '.crawler_edit_btn', function (e) {
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

$(document).on('click', '#crawler_save_btn', function (e) {
  e.preventDefault();
  var formData = $('#crawler_form').serialize();
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/add_crawler',
    type: 'POST',
    data: formData,
    dataType: 'json',
    success: function (data) {
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

$(document).on('click', '.remove_crawler_btn', function (e) {
  e.preventDefault();
  var target_id = $(this).data('id');
  if (!confirm('해당 수집기 설정과 수집된 DB 데이터를 모두 삭제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/remove_crawler',
    type: 'POST',
    data: { target_id: target_id },
    dataType: 'json',
    success: function (data) {
      notify('삭제되었습니다.', 'success');
      current_crawlers = data.crawlers || [];
      render_crawlers(current_crawlers);
    }
  });
});

$(document).on('click', '.remove_crawler_db_btn', function (e) {
  e.preventDefault();
  var target_id = $(this).data('id');
  if (!confirm('해당 수집기의 수집 데이터(DB)만 초기화하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/remove_crawler_db',
    type: 'POST',
    data: { target_id: target_id },
    dataType: 'json',
    success: function (data) {
      notify('초기화되었습니다.', 'success');
      current_crawlers = data.crawlers || [];
      render_crawlers(current_crawlers);
    }
  });
});

$(document).on('click', '#crawler_reload_btn', function (e) {
  e.preventDefault();
  load_all_crawl_data();
  notify('새로고침 완료', 'info');
});

$(document).on('click', '#my_site_add_btn', function (e) {
  e.preventDefault();
  $('#site_modal_title').text('사이트 직접 추가');
  $('#modal_site_id').val('-1');
  var default_json = '{\n  "NAME": "",\n  "TORRENT_SITE_URL": "https://",\n  "XPATH_LIST_TAG": {\n    "XPATH": "",\n    "TITLE_XPATH": ""\n  }\n}';
  $('#modal_site_json').val(default_json);
  if (json_editor) json_editor.setValue(default_json, -1);
  $('#modal_log').val('');
  $('#site_modal').modal('show');
  setTimeout(function () { if (json_editor) json_editor.resize(); }, 200);
});

$(document).on('click', '.site_edit_btn', function (e) {
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
  setTimeout(function () { if (json_editor) json_editor.resize(); }, 200);
});

$(document).on('click', '#modal_json_format_btn', function (e) {
  e.preventDefault();
  if (!json_editor) return;
  try {
    var raw = json_editor.getValue();
    var parsed = JSON.parse(raw);
    var formatted = JSON.stringify(parsed, null, 2);
    json_editor.setValue(formatted, -1);
    $('#modal_log').val('JSON 포맷 정렬 완료');
    notify('JSON 코드가 깔끔하게 정렬되었습니다.', 'info');
  } catch (err) {
    $('#modal_log').val('정렬 불가 (문법 오류): ' + err.message);
    notify('JSON 문법 오류로 정렬할 수 없습니다.', 'warning');
  }
});

$(document).on('click', '#modal_json_test_btn', function (e) {
  e.preventDefault();
  if (!json_editor) return;
  try {
    var parsed = JSON.parse(json_editor.getValue());
    var formatted = JSON.stringify(parsed, null, 2);
    json_editor.setValue(formatted, -1);
    $('#modal_site_json').val(formatted);
    $('#modal_log').val('정상 포맷입니다.');
    notify('올바른 JSON 포맷입니다.', 'success');
  } catch (err) {
    $('#modal_log').val('오류: ' + err.message);
    notify('JSON 오류', 'warning');
  }
});

$(document).on('click', '#modal_save_btn', function (e) {
  e.preventDefault();
  if (json_editor) $('#modal_site_json').val(json_editor.getValue());
  var formData = $('#site_form').serialize();
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/site_edit',
    type: 'POST',
    data: formData,
    dataType: 'json',
    success: function (data) {
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

function request_manual_crawl(crawler_id) {
  var postData = { command: 'manual_crawl' };
  if (crawler_id) postData.crawler_id = crawler_id;
  var targetName = crawler_id ? '개별 수집기(ID: ' + crawler_id + ')' : '전체 수집기';
  notify(targetName + ' 즉시 실행 요청 중...', 'info');

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/manual_crawl',
    type: 'POST',
    data: postData,
    dataType: 'json',
    success: function (data) {
      if (data && data.ret === 'success') {
        notify(data.msg || targetName + ' 즉시 실행을 시작했습니다.', 'success');
      } else if (data && data.ret === 'running') {
        notify(data.msg || '현재 다른 수집 작업이 이미 실행 중입니다.', 'warning');
      } else {
        notify((data && data.msg) || '즉시 실행 요청 실패', 'warning');
      }
    },
    error: function () {
      notify('서버 통신 실패', 'danger');
    }
  });
}

$(document).on('click', '#btn_manual_crawl', function (e) {
  e.preventDefault();
  request_manual_crawl(null);
});

$(document).on('click', '.crawler_manual_btn', function (e) {
  e.preventDefault();
  var cId = $(this).data('id');
  request_manual_crawl(cId);
});

function request_board_test(site_id, board_id) {
  notify('게시판 [' + board_id + '] 수집 테스트 시작...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/test',
    type: 'POST',
    data: { site_id: site_id, board_id: board_id },
    dataType: 'json',
    success: function (data) {
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

$(document).on('click', '.test_btn', function (e) {
  e.preventDefault();
  var site_id = $(this).data('site_id');
  var board_id = $('#board_id_' + site_id).val().trim();
  if (!board_id) { notify('게시판 ID를 입력하세요.', 'warning'); return; }
  request_board_test(site_id, board_id);
});

$(document).on('click', '.remove_site_btn', function (e) {
  e.preventDefault();
  var site_id = $(this).data('site_id');
  if (!confirm('사이트를 삭제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/site_delete',
    type: 'POST',
    data: { site_id: site_id },
    dataType: 'json',
    success: function (data) {
      notify('삭제되었습니다.', 'success');
      current_sites = data.site;
      render_sites(current_sites);
    }
  });
});

$(document).on('click', '#custom_script_manage_btn', function (e) {
  e.preventDefault();
  load_custom_script_list();
  $('#custom_script_modal').modal('show');
  setTimeout(function () { if (python_editor) python_editor.resize(); }, 200);
});

function load_custom_script_list(selected_name) {
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/custom_script_list',
    type: 'POST',
    dataType: 'json',
    success: function (data) {
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

$(document).on('change', '#custom_script_select', function () {
  var filename = $(this).val();
  if (!filename) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/custom_script_read',
    type: 'POST',
    data: { filename: filename },
    dataType: 'json',
    success: function (data) {
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

$(document).on('click', '#custom_script_new_btn', function (e) {
  e.preventDefault();
  $('#custom_script_select').val('');
  $('#custom_script_name').val('site_new.py');
  $('#custom_script_code').val(SCRIPT_SKELETON);
  if (python_editor) python_editor.setValue(SCRIPT_SKELETON, -1);
  $('#custom_script_status').text('새 스크립트 템플릿 로드');
});

$(document).on('click', '#custom_script_save_btn', function (e) {
  e.preventDefault();
  var filename = $('#custom_script_name').val().trim();
  var content = python_editor ? python_editor.getValue() : $('#custom_script_code').val();
  if (!filename) { notify('파일명을 입력하세요.', 'warning'); return; }

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/custom_script_save',
    type: 'POST',
    data: { filename: filename, content: content },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify('스크립트 저장 및 사이트 템플릿 등록이 완료되었습니다.', 'success');
        $('#custom_script_status').text('저장 완료 (' + filename + ')');
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

$(document).on('click', '#custom_script_delete_btn', function (e) {
  e.preventDefault();
  var filename = $('#custom_script_name').val().trim();
  if (!filename) return;
  if (!confirm('[' + filename + '] 스크립트를 삭제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/custom_script_delete',
    type: 'POST',
    data: { filename: filename },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify('삭제되었습니다.', 'success');
        load_custom_script_list();
      } else {
        notify('삭제 실패: ' + (data.log || data.ret), 'danger');
      }
    }
  });
});

$(document).on('click', '#custom_script_upload_trigger_btn', function (e) {
  e.preventDefault();
  $('#custom_script_file_input').val('').click();
});

$(document).on('change', '#custom_script_file_input', function () {
  var file = this.files[0];
  if (!file) return;
  var formData = new FormData();
  formData.append('command', 'custom_script_upload');
  formData.append('file', file);
  notify('파일 업로드 중...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/custom_script_upload',
    type: 'POST',
    data: formData,
    processData: false,
    contentType: false,
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify('스크립트 업로드 완료', 'success');
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

$(document).on('click', '#btn_fix_ed2k_db', function (e) {
  e.preventDefault();
  if (!confirm('DB 내의 모든 마그넷 및 ed2k 링크 구조를 전수 검사하여 보정하시겠습니까?')) return;
  notify('DB 링크 보정 작업 진행 중...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/fix_ed2k_db',
    type: 'POST',
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify('DB 보정 완료: 수집 게시글 ' + data.bbs_fixed + '건 보정, 다운로드 큐 ' + data.dl_fixed + '건 복원', 'success');
      } else {
        notify('보정 실패: ' + (data.msg || data.ret), 'danger');
      }
    }
  });
});
