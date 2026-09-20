var current_sites = [];
var current_tasks = [];
var current_info = {};
var modal_targets = [];

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
    url: '/' + package_name + '/ajax/' + sub + '/load_tasks',
    type: "POST",
    dataType: "json",
    success: function(data) {
      current_tasks = data.tasks || [];
      render_tasks(current_tasks);
    }
  });
}

function render_tasks(data) {
  var tbody = $('#task_list_tbody');
  if (!data || data.length === 0) {
    tbody.html('<tr><td colspan="4" class="py-4 text-muted">등록된 수집 작업(TASKS)이 없습니다. 상단의 "작업 추가"를 누르세요.</td></tr>');
    return;
  }
  var str = '';
  for (var i = 0; i < data.length; i++) {
    var item = data[i];
    var isEnabled = (item.enabled === true || item.enabled === 'True' || item.enabled === 'true' || item.enabled === 'on');
    var isProxy = (item.use_proxy === true || item.use_proxy === 'True' || item.use_proxy === 'true' || item.use_proxy === 'on');
    var isFlare = (item.use_flaresolverr === true || item.use_flaresolverr === 'True' || item.use_flaresolverr === 'true' || item.use_flaresolverr === 'on');
    var isSelenium = (item.use_selenium === true || item.use_selenium === 'True' || item.use_selenium === 'true' || item.use_selenium === 'on');
    var isRssFile = (item.use_rss_file === true || item.use_rss_file === 'True' || item.use_rss_file === 'true' || item.use_rss_file === 'on');

    var proxyDisplay = '<span class="text-muted">미사용</span>';
    if (isProxy) {
      proxyDisplay = item.proxy_url ? '<span class="text-warning font-weight-bold" title="' + item.proxy_url + '">개별</span>' : '<span class="text-warning font-weight-bold">기본</span>';
    }

    var targetsHtml = '';
    var targets = item.targets || [];
    if (targets.length > 0) {
      targetsHtml += '<div class="mt-1">';
      for (var t = 0; t < targets.length; t++) {
        var subTag = targets[t].subcat ? ':' + targets[t].subcat : '';
        targetsHtml += '<span class="badge badge-info mr-1 mb-1 font-weight-normal">' + targets[t].site + ' [' + targets[t].board + subTag + ']</span>';
      }
      targetsHtml += '</div>';
    } else {
      targetsHtml += '<div class="text-muted small">타겟 없음</div>';
    }

    str += '<tr>';
    str += '  <td class="font-weight-bold">' + item.id + '</td>';
    str += '  <td class="text-left"><strong>' + (item.name || ('Task ' + item.id)) + '</strong><br>' + targetsHtml + '</td>';
    str += '  <td class="text-left small" style="line-height: 1.6;">';
    str += '    상태: ' + (isEnabled ? '<span class="text-success font-weight-bold">활성</span>' : '<span class="text-muted">중지</span>') + ' / ' + (item.interval || 1) + '회당 1회<br>';
    str += '    화질: ' + (item.quality ? '<span class="badge badge-primary">' + item.quality + '</span>' : '<span class="text-muted">전역 설정</span>') + '<br>';
    str += '    Proxy: ' + proxyDisplay + ' | Flare: ' + (isFlare ? '<span class="text-danger">ON</span>' : '<span class="text-muted">OFF</span>') + ' | Selenium: ' + (isSelenium ? '<span class="text-info">ON</span>' : '<span class="text-muted">OFF</span>') + '<br>';
    str += '    공유 XML: ' + (isRssFile ? '<span class="text-success font-weight-bold">' + (item.rss_file || '자동') + '</span>' : '<span class="text-muted">미생성</span>');
    str += '  </td>';
    str += '  <td class="text-left">';
    if (item.last) {
      str += '    <div class="text-truncate mb-1" style="max-width: 440px;"><strong>최근:</strong> ' + item.last.title + '</div>';
      str += '    <small class="text-muted">수집: ' + item.last.created_time + '</small>';
    } else {
      str += '    <div class="text-muted small mb-1">수집된 데이터 없음</div>';
    }
    str += '    <div class="mt-2 d-flex justify-content-between align-items-center">';
    str += '      <div class="btn-group btn-group-sm">';
    str += '        <button type="button" class="btn btn-primary text-white task_edit_btn" data-id="' + item.id + '" data-index="' + i + '">수정</button>';
    str += '        <button type="button" class="btn btn-danger text-white remove_task_btn" data-id="' + item.id + '">삭제</button>';
    str += '        <button type="button" class="btn btn-secondary text-white remove_task_db_btn" data-id="' + item.id + '">DB 비우기</button>';
    str += '      </div>';
    str += '      <a href="' + item.api + '" target="_blank" class="btn btn-sm text-white" style="background-color: #f26522; font-weight: 500;"><i class="fa fa-rss"></i> 통합 RSS</a>';
    str += '    </div>';
    str += '  </td>';
    str += '</tr>';
  }
  tbody.html(str);
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
    str += '          <button type="button" class="btn btn-outline-info test_btn" data-site_id="' + item.id + '">수집 테스트</button>';
    str += '          <button type="button" class="btn btn-outline-primary site_edit_btn" data-site_id="' + item.id + '" data-index="' + i + '">규칙 수정</button>';
    str += '          <button type="button" class="btn btn-outline-danger remove_site_btn" data-site_id="' + item.id + '">삭제</button>';
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

function render_modal_targets() {
  var tbody = $('#modal_targets_tbody');
  if (!modal_targets || modal_targets.length === 0) {
    tbody.html('<tr><td colspan="4" class="text-muted py-2">등록된 타겟 게시판이 없습니다.</td></tr>');
    $('#targets_json').val('[]');
    return;
  }
  var str = '';
  for (var i = 0; i < modal_targets.length; i++) {
    var t = modal_targets[i];
    str += '<tr>';
    str += '  <td><span class="badge badge-secondary">' + t.site + '</span></td>';
    str += '  <td><strong>' + t.board + '</strong></td>';
    str += '  <td>' + (t.subcat ? '<span class="badge badge-light">' + t.subcat + '</span>' : '<span class="text-muted">-</span>') + '</td>';
    str += '  <td><button type="button" class="btn btn-xs btn-outline-danger remove_target_item_btn" data-index="' + i + '">삭제</button></td>';
    str += '</tr>';
  }
  tbody.html(str);
  $('#targets_json').val(JSON.stringify(modal_targets));
}

$(document).on('click', '#add_target_item_btn', function(e){
  e.preventDefault();
  var site = $('#target_site_select').val();
  var board = $('#target_board_input').val().trim();
  var subcat = $('#target_subcat_input').val().trim();
  if (!site || !board) {
    notify('사이트와 게시판 ID를 입력하세요.', 'warning');
    return;
  }
  modal_targets.push({site: site, board: board, subcat: subcat});
  render_modal_targets();
  $('#target_board_input').val('');
  $('#target_subcat_input').val('');
});

$(document).on('click', '.remove_target_item_btn', function(e){
  e.preventDefault();
  var idx = $(this).data('index');
  modal_targets.splice(idx, 1);
  render_modal_targets();
});

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

$('#use_proxy').change(function(){
  if ($(this).is(':checked')) {
    $('#modal_use_proxy_div').collapse('show');
  } else {
    $('#modal_use_proxy_div').collapse('hide');
  }
});

$('#use_rss_file').change(function(){
  if ($(this).is(':checked')) {
    $('#modal_use_rss_file_div').collapse('show');
    if (!$('#rss_file').val()) {
      var task_name = $('#task_name').val().trim() || 'feed';
      $('#rss_file').val(task_name + '.xml');
    }
  } else {
    $('#modal_use_rss_file_div').collapse('hide');
  }
});

$('#use_task_filter').change(function(){
  if ($(this).is(':checked')) {
    $('#modal_use_task_filter_div').collapse('show');
  } else {
    $('#modal_use_task_filter_div').collapse('hide');
  }
});

$(document).on('input change', '#task_name', function(){
  if ($('#use_rss_file').is(':checked')) {
    var val = $(this).val().trim();
    if (val) $('#rss_file').attr('placeholder', val + '.xml');
  }
});

$(document).on('click', '#task_add_btn', function(e){
  e.preventDefault();
  if (!current_sites || current_sites.length === 0) {
    notify('등록된 사이트가 없습니다.', 'warning');
    return;
  }
  var siteSelectHtml = '<select id="target_site_select" class="form-control form-control-sm">';
  for (var i in current_sites) siteSelectHtml += '<option value="' + current_sites[i].name + '">' + current_sites[i].name + '</option>';
  siteSelectHtml += '</select>';
  $('#target_site_select_div').html(siteSelectHtml);

  $('#modal_title').text('수집 작업(Task) 추가');
  $('#modal_scheduler_id').val('-1');
  $('#task_name').val('');
  $('#target_board_input').val('');
  $('#target_subcat_input').val('');
  $('#interval').val('1');
  $('#quality').val('');
  $('#proxy_url').val('');
  $('#rss_file').val('');
  $('#rss_file_path').val('');
  $('#rss_file_days').val('');
  $('#rss_file_items').val('');
  $('#filter_reject').val('');
  $('#filter_accept').val('');
  $('#filter_reject_excluding').val('');
  $('#modal_use_proxy_div').collapse('hide');
  $('#modal_use_rss_file_div').collapse('hide');
  $('#modal_use_task_filter_div').collapse('hide');
  set_modal_checkbox('enabled', true);
  set_modal_checkbox('use_proxy', false);
  set_modal_checkbox('use_flaresolverr', false);
  set_modal_checkbox('use_selenium', false);
  set_modal_checkbox('use_torrent_info', false);
  set_modal_checkbox('use_rss_file', false);
  set_modal_checkbox('use_task_filter', false);
  set_modal_checkbox('accept_all', false);

  modal_targets = [];
  render_modal_targets();
  $('#add_job_modal').modal('show');
});

$(document).on('click', '.task_edit_btn', function(e){
  e.preventDefault();
  var index = $(this).data('index');
  var item = current_tasks[index];

  var siteSelectHtml = '<select id="target_site_select" class="form-control form-control-sm">';
  for (var i in current_sites) siteSelectHtml += '<option value="' + current_sites[i].name + '">' + current_sites[i].name + '</option>';
  siteSelectHtml += '</select>';
  $('#target_site_select_div').html(siteSelectHtml);

  $('#modal_title').text('수집 작업(Task) 수정: ' + (item.name || item.id));
  $('#modal_scheduler_id').val(item.id);
  $('#task_name').val(item.name || '');
  $('#interval').val(item.interval || 1);
  $('#quality').val(item.quality || '');
  $('#proxy_url').val(item.proxy_url || '');
  $('#rss_file').val(item.rss_file || '');
  $('#rss_file_path').val(item.rss_file_path || '');
  $('#rss_file_days').val(item.rss_file_days || '');
  $('#rss_file_items').val(item.rss_file_items || '');

  // 정규식 필터 데이터 복원
  var regexp = item.regexp || {};
  var reject_str = rules_to_string(regexp.reject);
  var accept_str = rules_to_string(regexp.accept);
  var reject_ex_str = rules_to_string(regexp.reject_excluding);
  var is_accept_all = (item.accept_all === true || item.accept_all === 'True' || item.accept_all === 'true' || item.accept_all === 'yes' || item.accept_all === 'on');
  var has_filter = Boolean(reject_str || accept_str || reject_ex_str || is_accept_all);

  $('#filter_reject').val(reject_str);
  $('#filter_accept').val(accept_str);
  $('#filter_reject_excluding').val(reject_ex_str);

  set_modal_checkbox('enabled', item.enabled);
  set_modal_checkbox('use_torrent_info', item.use_torrent_info);
  set_modal_checkbox('use_proxy', item.use_proxy);
  set_modal_checkbox('use_flaresolverr', item.use_flaresolverr);
  set_modal_checkbox('use_selenium', item.use_selenium);
  set_modal_checkbox('use_rss_file', item.use_rss_file);
  set_modal_checkbox('use_task_filter', has_filter);
  set_modal_checkbox('accept_all', is_accept_all);

  if (item.use_proxy) $('#modal_use_proxy_div').collapse('show');
  else $('#modal_use_proxy_div').collapse('hide');

  if (item.use_rss_file) $('#modal_use_rss_file_div').collapse('show');
  else $('#modal_use_rss_file_div').collapse('hide');

  if (has_filter) $('#modal_use_task_filter_div').collapse('show');
  else $('#modal_use_task_filter_div').collapse('hide');

  modal_targets = item.targets ? JSON.parse(JSON.stringify(item.targets)) : [];
  render_modal_targets();
  $('#add_job_modal').modal('show');
});

$(document).on('click', '#add_job_save_btn', function(e){
  e.preventDefault();
  var formData = $('#job_form').serialize();
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/add_task',
    type: "POST",
    data: formData,
    dataType: "json",
    success: function(data) {
      if (data.ret === 'success' || data.ret === 'success_update') {
        notify('수집 작업(Task)이 저장되었습니다.', 'success');
        $('#add_job_modal').modal('hide');
        current_tasks = data.tasks || [];
        render_tasks(current_tasks);
      } else {
        notify('저장 실패: ' + (data.log || data.ret), 'danger');
      }
    }
  });
});

$(document).on('click', '.remove_task_btn', function(e){
  e.preventDefault();
  var target_id = $(this).data('id');
  if (!confirm('해당 태스크와 수집된 DB 데이터를 모두 삭제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/remove_task',
    type: "POST",
    data: {target_id: target_id},
    dataType: "json",
    success: function(data) {
      notify('삭제되었습니다.', 'success');
      current_tasks = data.tasks || [];
      render_tasks(current_tasks);
    }
  });
});

$(document).on('click', '.remove_task_db_btn', function(e){
  e.preventDefault();
  var target_id = $(this).data('id');
  if (!confirm('해당 태스크에 속한 게시판들의 수집 데이터(DB)를 초기화하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/remove_task_db',
    type: "POST",
    data: {target_id: target_id},
    dataType: "json",
    success: function(data) {
      notify('초기화되었습니다.', 'success');
      current_tasks = data.tasks || [];
      render_tasks(current_tasks);
    }
  });
});

$(document).on('click', '#scheduler_reload_btn', function(e){
  e.preventDefault();
  load_all_data();
  notify('작업 목록 새로고침 완료', 'info');
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

$(document).on('click', '.test_btn', function(e){
  e.preventDefault();
  var site_id = $(this).data('site_id');
  var board_id = $('#board_id_' + site_id).val().trim();
  if (!board_id) { notify('게시판 ID를 입력하세요.', 'warning'); return; }
  notify('게시판 [' + board_id + '] 수집 테스트 시작...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/test',
    type: "POST",
    data: {site_id: site_id, board_id: board_id},
    dataType: "json",
    success: function(data) {
      $('#test_modal_title').text('수집 테스트 결과 (전체 ' + data.length + '개)');
      $('#test_modal_body').val(JSON.stringify(data, null, 2));
      $('#test_result_modal').modal('show');
    }
  });
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
