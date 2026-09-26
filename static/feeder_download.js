// =============================================================================
// DOWNLOAD 모듈 전용 스크립트 (Queue & List & Setting 통합)
// =============================================================================
var sse_source = null;
var cached_download_list = [];

var current_downloaders = [];
var current_profiles = [];
var current_accounts = [];
var available_feeds = [];
var modal_profile_feeds = [];
var modal_profile_chain = [];
var available_engine_schemas = [];
var available_transporter_schemas = [];
var dl_engine_editor = null;
var dl_trans_editor = null;

var TRANSPORTER_SKELETON = '# -*- coding: utf-8 -*-\n' +
  'from feeder.setup import logger\n' +
  'from feeder.util_download import BaseTransporter\n\n' +
  'class CustomTransporter(BaseTransporter):\n' +
  '    TRANSPORTER_ID = "custom_trans"\n' +
  '    TRANSPORTER_NAME = "커스텀 이송 핸들러"\n\n' +
  '    CONFIG_SCHEMA = [\n' +
  '        {"name": "target_path", "label": "대상 경로", "type": "text", "default": "/data/target"}\n' +
  '    ]\n\n' +
  '    def transport(self, item, source_path: str, dest_config: dict) -> tuple[bool, str, str]:\n' +
  '        logger.info(f"이송 실행: {item.title} -> {dest_config.get(\'target_path\')}")\n' +
  '        return True, "completed", "이송 완료"\n';

// -----------------------------------------------------------------------------
// 초기화 분기 (Queue 화면 vs List 화면 vs Setting 화면 자동 감지)
// -----------------------------------------------------------------------------
$(document).ready(function () {
  try {
    sync_feeder_header_navbar();
  } catch (err) {}

  // Queue 대시보드 화면 진입 시
  if ($('#active_queue_tbody').length > 0) {
    try {
      localStorage.setItem('feeder_last_download_page', 'queue');
      sync_feeder_header_navbar();
    } catch (err) {}

    request_active_queue();
    if (typeof enable_sse !== 'undefined' && enable_sse) {
      init_sse_listener();
    }
  }

  // List 다운로드 이력 화면 진입 시
  if ($('#download_list_tbody').length > 0) {
    try {
      localStorage.setItem('feeder_last_download_page', 'list');
      sync_feeder_header_navbar();
    } catch (err) {}

    var saved_status = localStorage.getItem(sub + '_status_filter') || 'all';
    $('#status_filter').val(saved_status);

    var saved_size = localStorage.getItem(sub + '_page_size') || '25';
    $('#page_size').val(saved_size);

    var saved_word = localStorage.getItem(sub + '_search_word') || '';
    $('#search_word').val(saved_word);

    var saved_page = localStorage.getItem(sub + '_current_page') || '1';

    load_retry_profiles();
    window.globalRequestSearch(saved_page, false);
  }

  // Setting 화면 진입 시
  if ($('#downloader_list_tbody').length > 0) {
    try {
      localStorage.setItem('feeder_last_download_page', 'setting');
      sync_feeder_header_navbar();
    } catch (err) {}

    load_all_download_config();
    init_dl_engine_editor();
    init_dl_trans_editor();
    restore_active_subtab('download');
    setTimeout(function () { restore_active_subtab('download'); }, 80);
  }
});

// -----------------------------------------------------------------------------
// [DOWNLOAD: QUEUE 대시보드 로직]
// -----------------------------------------------------------------------------
function init_sse_listener() {
  if (window.EventSource) {
    var sseUrl = '/' + package_name + '/api/' + sub + '/sse' + (apikey ? '?apikey=' + apikey : '');
    sse_source = new EventSource(sseUrl);
    sse_source.onmessage = function (event) {
      try {
        var data = JSON.parse(event.data);
        if (data && data.counts) {
          $('#stat_pending').text(data.counts.pending || 0);
          $('#stat_downloading').text(data.counts.downloading || 0);
          $('#stat_staging').text(data.counts.staging || 0);
          $('#stat_uploading').text(data.counts.uploading || 0);
          $('#stat_completed').text(data.counts.completed || 0);
          $('#stat_failed').text(data.counts.failed || 0);
        }
      } catch (err) {}
    };
  }
}

function request_active_queue() {
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/web_list',
    type: 'POST',
    data: { page: 1, page_size: 50, status_filter: 'active' },
    dataType: 'json',
    success: function (data) {
      render_queue_rows(data.list || []);
    }
  });
}

function render_queue_rows(list) {
  var tbody = $('#active_queue_tbody');
  if (!tbody.length) return;
  if (!list || list.length === 0) {
    tbody.html('<tr><td colspan="6" class="py-4 text-muted">현재 진행 중인 활성 작업이 없습니다.</td></tr>');
    return;
  }

  var str = '';
  for (var i = 0; i < list.length; i++) {
    var it = list[i];
    var statusBadge = '';
    if (it.status === 'pending') statusBadge = '<span class="badge badge-warning">대기 (Pending)</span>';
    else if (it.status === 'downloading') statusBadge = '<span class="badge badge-primary">다운로드 중</span>';
    else if (it.status === 'pending_local_staging' || it.status === 'local_staging') statusBadge = '<span class="badge badge-info">로컬 스테이징</span>';
    else if (it.status === 'pending_relay') statusBadge = '<span class="badge badge-warning">원격 릴레이 대기</span>';
    else if (it.status === 'relay_transferring') statusBadge = '<span class="badge badge-primary">원격 릴레이 전송 중</span>';
    else if (it.status === 'downloaded') statusBadge = '<span class="badge badge-success">다운로드 완료</span>';
    else if (it.status === 'pending_upload' || it.status === 'uploading') statusBadge = '<span class="badge badge-primary">업로드 중</span>';
    else if (it.status === 'completed') statusBadge = '<span class="badge badge-success">최종 완료</span>';
    else if (it.status === 'failed') statusBadge = '<span class="badge badge-danger">실패</span>';
    else statusBadge = '<span class="badge badge-secondary">' + it.status + '</span>';

    var engineInfo = it.current_engine_name ? '<br><small class="text-muted">엔진: ' + it.current_engine_name + '</small>' : '';
    var errorMsg = it.error_message ? '<br><small class="text-danger">오류: ' + it.error_message + '</small>' : '';
    var pathDisplay = it.local_path ? '<div class="text-truncate small text-muted" style="max-width: 220px;" title="' + it.local_path + '">' + it.local_path + '</div>' : '<span class="text-muted">-</span>';

    str += '<tr>';
    str += '  <td class="font-weight-bold">' + it.id + '</td>';
    str += '  <td>' + statusBadge + engineInfo + '</td>';
    str += '  <td class="text-left">';
    str += '    <span class="badge badge-dark mr-1">' + (it.feed_name || 'Feed') + '</span>';
    str += '    <strong>' + it.title + '</strong>' + errorMsg;
    if (it.file_name) str += '<br><small class="text-info font-weight-bold">폴더/파일: ' + it.file_name + '</small>';
    str += '  </td>';
    str += '  <td>' + format_bytes(it.file_size) + '<br>' + pathDisplay + '</td>';
    str += '  <td class="small text-muted">' + (it.updated_time || it.created_time || '') + '</td>';
    str += '  <td>';
    str += '    <div class="btn-group btn-group-sm">';
    str += '      <button type="button" class="btn btn-warning text-dark font-weight-bold queue_action_btn" data-action="retry" data-id="' + it.id + '">재시도</button>';
    str += '      <button type="button" class="btn btn-success text-white queue_action_btn" data-action="force_complete" data-id="' + it.id + '">완료</button>';
    str += '      <button type="button" class="btn btn-danger text-white queue_action_btn" data-action="delete" data-id="' + it.id + '">삭제</button>';
    str += '    </div>';
    str += '  </td>';
    str += '</tr>';
  }
  tbody.html(str);
}

$('#queue_refresh_btn').click(function (e) {
  e.preventDefault();
  request_active_queue();
  notify('작업 큐를 새로고침했습니다.', 'info');
});

$(document).on('click', '.queue_action_btn', function (e) {
  e.preventDefault();
  var act = $(this).data('action');
  var id = $(this).data('id');

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/item_action',
    type: 'POST',
    data: { action: act, id: id },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify('작업이 처리되었습니다.', 'info');
        request_active_queue();
      } else {
        notify(data.msg || '실패', 'warning');
      }
    }
  });
});

// -----------------------------------------------------------------------------
// [DOWNLOAD: LIST 이력 화면 로직]
// -----------------------------------------------------------------------------
function load_retry_profiles() {
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/load_profiles',
    type: 'POST',
    dataType: 'json',
    success: function (data) {
      cached_download_profiles = data.profiles || [];
    }
  });
}

function make_list(list) {
  cached_download_list = list || [];
  var tbody = $('#download_list_tbody');
  if (!tbody.length) return;
  if (!list || list.length === 0) {
    tbody.html('<tr><td colspan="6" class="py-4 text-muted">검색 조건에 일치하는 다운로드 이력이 없습니다.</td></tr>');
    return;
  }

  var str = '';
  for (var i = 0; i < list.length; i++) {
    var it = list[i];
    var statusBadge = '';
    if (it.status === 'completed') statusBadge = '<span class="badge badge-success">최종 완료</span>';
    else if (it.status === 'downloading') statusBadge = '<span class="badge badge-primary">다운로드 중</span>';
    else if (it.status === 'pending') statusBadge = '<span class="badge badge-warning">대기 (Pending)</span>';
    else if (it.status === 'pending_local_staging' || it.status === 'local_staging') statusBadge = '<span class="badge badge-info">로컬 스테이징</span>';
    else if (it.status === 'pending_colab') statusBadge = '<span class="badge badge-warning">Colab 대기</span>';
    else if (it.status === 'colab_transferring') statusBadge = '<span class="badge badge-primary">Colab 전송 중</span>';
    else if (it.status === 'downloaded') statusBadge = '<span class="badge badge-success">다운로드 완료</span>';
    else if (it.status === 'pending_upload' || it.status === 'uploading') statusBadge = '<span class="badge badge-primary">업로드 중</span>';
    else if (it.status === 'move_failed') statusBadge = '<span class="badge badge-warning">이동 실패</span>';
    else if (it.status === 'failed') statusBadge = '<span class="badge badge-danger">실패</span>';
    else statusBadge = '<span class="badge badge-secondary">' + it.status + '</span>';

    var engineInfo = it.current_engine_name ? '<br><small class="text-muted">엔진: ' + it.current_engine_name + '</small>' : '';
    var errorMsg = it.error_message ? '<br><small class="text-danger">오류: ' + it.error_message + '</small>' : '';
    var pathDisplay = it.local_path ? '<div class="text-truncate small text-muted" style="max-width: 220px;" title="' + it.local_path + '">' + it.local_path + '</div>' : '<span class="text-muted">-</span>';
    var timeStr = it.completed_time || it.updated_time || it.created_time || '';

    str += '<tr>';
    str += '  <td class="font-weight-bold">' + it.id + '</td>';
    str += '  <td>' + statusBadge + engineInfo + '</td>';
    str += '  <td class="text-left">';
    str += '    <span class="badge badge-dark mr-1">' + (it.feed_name || 'Feed') + '</span>';
    str += '    <strong>' + it.title + '</strong>' + errorMsg;
    if (it.file_name) str += '<br><small class="text-info font-weight-bold">폴더/파일: ' + it.file_name + '</small>';
    str += '  </td>';
    str += '  <td>' + format_bytes(it.file_size) + '<br>' + pathDisplay + '</td>';
    str += '  <td class="small text-muted">' + timeStr + '</td>';
    str += '  <td>';
    str += '    <div class="btn-group btn-group-sm">';
    str += '      <button type="button" class="btn btn-warning text-dark font-weight-bold btn_list_action" data-action="retry" data-id="' + it.id + '">재시도</button>';
    str += '      <button type="button" class="btn btn-success text-white btn_list_action" data-action="force_complete" data-id="' + it.id + '">완료</button>';
    str += '      <button type="button" class="btn btn-danger text-white btn_list_action" data-action="delete" data-id="' + it.id + '">삭제</button>';
    str += '    </div>';
    str += '  </td>';
    str += '</tr>';
  }
  tbody.html(str);
}

$('#status_filter, #page_size').change(function () {
  if ($('#download_list_tbody').length > 0) {
    window.globalRequestSearch('1', false);
  }
});

$('#reset_btn').click(function (e) {
  e.preventDefault();
  if ($('#download_list_tbody').length === 0) return;

  $('#status_filter').val('all');
  $('#search_word').val('');
  $('#page_size').val('25');

  localStorage.removeItem(sub + '_search_word');
  localStorage.setItem(sub + '_status_filter', 'all');
  localStorage.setItem(sub + '_page_size', '25');
  localStorage.setItem(sub + '_current_page', '1');

  window.globalRequestSearch('1', false);
});

$('#modal_retry_profile_select').change(function () {
  var pName = $(this).val();
  var pObj = cached_download_profiles.find(function (p) { return p.name === pName; });
  if (pObj) {
    var chainStr = (pObj.priority_chain || []).join(' -> ') || '(비어있음)';
    var dest = pObj.destination || {};
    var destStr = dest.type || 'local';
    if (dest.type === 'colab_gdrive') destStr += ' (Colab 무트래픽)';
    else if (dest.type === 'gdrive_rotation') destStr += ' (SA 15GB 우회)';

    $('#modal_retry_chain_preview').text(chainStr);
    $('#modal_retry_dest_preview').text(destStr);
  } else {
    $('#modal_retry_chain_preview').text('(기본 활성 다운로더 전체)');
    $('#modal_retry_dest_preview').text('local (로컬 디스크 보존)');
  }
});

$(document).on('click', '.btn_list_action', function (e) {
  e.preventDefault();
  var act = $(this).data('action');
  var id = $(this).data('id');
  var item = cached_download_list.find(function (x) { return String(x.id) === String(id); });
  var titleText = item ? item.title : ('ID: ' + id);

  if (act === 'retry') {
    $('#modal_retry_item_id').val(id);
    $('#modal_retry_item_title').text(titleText);

    var pSelect = $('#modal_retry_profile_select');
    pSelect.empty();
    if (cached_download_profiles && cached_download_profiles.length > 0) {
      for (var i = 0; i < cached_download_profiles.length; i++) {
        var p = cached_download_profiles[i];
        pSelect.append('<option value="' + p.name + '">' + p.name + '</option>');
      }
      if (item && item.feed_name) {
        var matchP = cached_download_profiles.find(function (p) {
          return (p.feeds || []).indexOf(item.feed_name) !== -1 || (p.feeds || []).indexOf('*') !== -1;
        });
        if (matchP) pSelect.val(matchP.name);
      }
    } else {
      pSelect.append('<option value="">-- 기본 다운로더 전체 사용 --</option>');
    }
    pSelect.trigger('change');
    $('#download_retry_modal').modal('show');
    return;
  }

  if (act === 'force_complete') {
    if (!confirm('[' + id + '번 항목]\n"' + titleText + '"\n\n해당 작업을 최종 완료(completed) 상태로 변경하시겠습니까?')) return;
  } else if (act === 'delete') {
    if (!confirm('[' + id + '번 항목]\n"' + titleText + '"\n\n해당 작업을 완전히 삭제하시겠습니까?')) return;
  }

  notify('작업 요청 중...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/item_action',
    type: 'POST',
    data: { action: act, id: id },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify(data.msg || '작업이 처리되었습니다.', 'success');
        window.globalRequestSearch(null, true);
      } else {
        notify(data.msg || '실패', 'warning');
      }
    }
  });
});

$(document).on('click', '#btn_confirm_retry_execute', function (e) {
  e.preventDefault();
  var id = $('#modal_retry_item_id').val();
  var pName = $('#modal_retry_profile_select').val();

  notify('다운로드 재시도 등록 중...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/item_action',
    type: 'POST',
    data: {
      action: 'retry',
      id: id,
      profile_name: pName
    },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify(data.msg || '재시도 대기열에 등록되었습니다.', 'success');
        $('#download_retry_modal').modal('hide');
        window.globalRequestSearch(null, true);
      } else {
        notify(data.msg || '재시도 실패', 'warning');
      }
    }
  });
});

// -----------------------------------------------------------------------------
// [DOWNLOAD: SETTING 화면 로직]
// -----------------------------------------------------------------------------
function init_dl_engine_editor() {
  if ($('#download_script_code_editor').length && !dl_engine_editor && window.ace) {
    dl_engine_editor = ace.edit('download_script_code_editor');
    dl_engine_editor.setTheme('ace/theme/monokai');
    dl_engine_editor.session.setMode('ace/mode/python');
    dl_engine_editor.setFontSize(13);
    dl_engine_editor.setShowPrintMargin(false);
    dl_engine_editor.session.setTabSize(4);
    dl_engine_editor.session.setUseSoftTabs(true);
    dl_engine_editor.session.setUseWrapMode(true);
    feeder_ace_instances.push(dl_engine_editor);
  }
}

function init_dl_trans_editor() {
  if ($('#transporter_script_code_editor').length && !dl_trans_editor && window.ace) {
    dl_trans_editor = ace.edit('transporter_script_code_editor');
    dl_trans_editor.setTheme('ace/theme/monokai');
    dl_trans_editor.session.setMode('ace/mode/python');
    dl_trans_editor.setFontSize(13);
    dl_trans_editor.setShowPrintMargin(false);
    dl_trans_editor.session.setTabSize(4);
    dl_trans_editor.session.setUseSoftTabs(true);
    dl_trans_editor.session.setUseWrapMode(true);
    feeder_ace_instances.push(dl_trans_editor);
  }
}

$('#download_script_modal').on('shown.bs.modal', function () {
  init_dl_engine_editor();
  if (dl_engine_editor) {
    dl_engine_editor.resize();
    dl_engine_editor.renderer.updateFull();
  }
});

$('#transporter_script_modal').on('shown.bs.modal', function () {
  init_dl_trans_editor();
  if (dl_trans_editor) {
    dl_trans_editor.resize();
    dl_trans_editor.renderer.updateFull();
  }
});

function load_all_download_config() {
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/load_downloaders',
    type: 'POST',
    dataType: 'json',
    success: function (data) {
      current_downloaders = data.downloaders || [];
      available_engine_schemas = data.schemas || [];
      render_downloaders(current_downloaders);
      update_engine_type_dropdown();
    }
  });

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/load_profiles',
    type: 'POST',
    dataType: 'json',
    success: function (data) {
      current_profiles = data.profiles || [];
      available_feeds = data.feeds || [];
      available_transporter_schemas = data.transporter_schemas || [];
      render_profiles(current_profiles);
      update_profile_dest_type_dropdown();
    }
  });

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/load_accounts',
    type: 'POST',
    dataType: 'json',
    success: function (data) {
      current_accounts = data.accounts || [];
      render_accounts(current_accounts, data.stats || {});
    }
  });
}

function render_downloaders(data) {
  var tbody = $('#downloader_list_tbody');
  if (!tbody.length) return;
  if (!data || data.length === 0) {
    tbody.html('<tr><td colspan="5" class="py-4 text-muted">등록된 다운로더 엔진이 없습니다.</td></tr>');
    return;
  }
  var str = '';
  for (var i = 0; i < data.length; i++) {
    var item = data[i];
    var isEnabled = (item.enabled === true || item.enabled === 'True' || item.enabled === 'on');
    var statusBadge = isEnabled ? '<span class="badge badge-success">활성</span>' : '<span class="badge badge-secondary">중지</span>';
    var rawTimeout = (item.stalled_timeout_hours !== undefined && item.stalled_timeout_hours !== null && item.stalled_timeout_hours !== '') ? parseInt(item.stalled_timeout_hours) : 24;
    var timeoutStr = (rawTimeout <= 0) ? '<span class="text-info font-weight-bold">타임아웃 무제한</span>' : rawTimeout + '시간 타임아웃';

    var connDetail = '';
    if (item.engine_type === 'alldebrid') {
      connDetail = '리모트: <code>' + (item.remote_name || 'ad') + ':' + (item.rclone_base_path || 'magnets') + '</code>';
    } else if (item.engine_type === '115') {
      connDetail = 'CD2: ' + (item.cd2_addr || '127.0.0.1') + ':' + (item.cd2_port || 19798) + ' | 마운트: ' + (item.cd2_mount_path || '-');
    } else if (item.engine_type === 'qbittorrent') {
      connDetail = 'URL: ' + (item.url || '-') + ' | 저장: ' + (item.save_path || '-');
    } else {
      var keys = Object.keys(item).filter(function (k) { return ['name', 'engine_type', 'enabled', 'stalled_timeout_hours'].indexOf(k) === -1; });
      connDetail = keys.slice(0, 3).map(function (k) { return k + ': ' + item[k]; }).join(' | ') || '-';
    }

    str += '<tr>';
    str += '  <td class="font-weight-bold">' + item.name + '</td>';
    str += '  <td><span class="badge badge-info">' + item.engine_type + '</span></td>';
    str += '  <td>' + statusBadge + '<br><small class="text-muted">' + timeoutStr + '</small></td>';
    str += '  <td class="text-left small">' + connDetail + '</td>';
    str += '  <td>';
    str += '    <div class="btn-group btn-group-sm">';
    str += '      <button type="button" class="btn btn-primary text-white edit_dl_btn" data-index="' + i + '">수정</button>';
    str += '      <button type="button" class="btn btn-danger text-white delete_dl_btn" data-name="' + item.name + '">삭제</button>';
    str += '    </div>';
    str += '  </td>';
    str += '</tr>';
  }
  tbody.html(str);
}

function update_engine_type_dropdown(selected_type) {
  var select = $('#dl_engine_type');
  select.empty();
  if (!available_engine_schemas || available_engine_schemas.length === 0) {
    select.append('<option value="">-- 등록된 엔진 없음 --</option>');
    return;
  }
  for (var i = 0; i < available_engine_schemas.length; i++) {
    var schema = available_engine_schemas[i];
    var isSel = (schema.engine_id === selected_type) ? 'selected' : '';
    select.append('<option value="' + schema.engine_id + '" ' + isSel + '>' + schema.engine_name + ' (' + schema.engine_id + ')</option>');
  }
}

function render_dynamic_engine_fields(engine_id, current_values) {
  var container = $('#dl_dynamic_engine_fields');
  container.empty();

  var schemaObj = available_engine_schemas.find(function (s) { return s.engine_id === engine_id; });
  if (!schemaObj || !schemaObj.config_schema || schemaObj.config_schema.length === 0) {
    container.html('<div class="text-muted small p-2 text-center">해당 엔진에 별도 설정 항목이 없습니다.</div>');
    return;
  }

  var html = '<hr><h6 class="text-info font-weight-bold mb-3"><i class="fa fa-sliders mr-1"></i>' + schemaObj.engine_name + ' 상세 설정</h6>';
  var fields = schemaObj.config_schema;

  for (var i = 0; i < fields.length; i++) {
    var f = fields[i];
    var val = (current_values && current_values[f.name] !== undefined) ? current_values[f.name] : (f.default !== undefined ? f.default : '');
    var ph = f.placeholder || '';
    var descHtml = f.desc ? '<small class="form-text text-muted">' + f.desc + '</small>' : '';

    html += '<div class="form-group row mb-2">';
    html += '  <label class="col-sm-3 col-form-label text-right font-weight-bold">' + f.label + '</label>';
    html += '  <div class="col-sm-9">';

    if (f.type === 'checkbox') {
      var isChk = (val === true || val === 'true' || val === 'On' || val === 'on') ? 'checked' : '';
      html += '    <input type="checkbox" id="dl_field_' + f.name + '" class="mt-2" ' + isChk + '>';
    } else if (f.type === 'number') {
      html += '    <input type="number" id="dl_field_' + f.name + '" class="form-control form-control-sm" value="' + val + '" placeholder="' + ph + '">';
    } else if (f.type === 'password') {
      html += '    <input type="password" id="dl_field_' + f.name + '" class="form-control form-control-sm" value="' + val + '" placeholder="' + ph + '">';
    } else {
      html += '    <input type="text" id="dl_field_' + f.name + '" class="form-control form-control-sm" value="' + val + '" placeholder="' + ph + '">';
    }

    html += descHtml;
    html += '  </div>';
    html += '</div>';
  }
  container.html(html);
}

$('#dl_engine_type').change(function () {
  render_dynamic_engine_fields($(this).val(), null);
});

$(document).on('click', '#downloader_add_btn', function (e) {
  e.preventDefault();
  $('#downloader_modal_title').text('다운로더 엔진 추가');
  $('#downloader_mode').val('add');
  $('#dl_name').val('').prop('readonly', false);
  $('#dl_enabled').prop('checked', true);

  var default_engine = available_engine_schemas.length > 0 ? available_engine_schemas[0].engine_id : '';
  update_engine_type_dropdown(default_engine);
  render_dynamic_engine_fields(default_engine, null);

  $('#downloader_modal').modal('show');
});

$(document).on('click', '.edit_dl_btn', function (e) {
  e.preventDefault();
  var idx = $(this).data('index');
  var item = current_downloaders[idx];

  $('#downloader_modal_title').text('다운로더 엔진 수정: ' + item.name);
  $('#downloader_mode').val('edit');
  $('#dl_name').val(item.name).prop('readonly', true);
  $('#dl_enabled').prop('checked', item.enabled);

  update_engine_type_dropdown(item.engine_type);
  render_dynamic_engine_fields(item.engine_type, item);

  $('#downloader_modal').modal('show');
});

$(document).on('click', '#downloader_test_btn', function (e) {
  e.preventDefault();
  var name = $('#dl_name').val().trim() || 'test_engine';
  var e_type = $('#dl_engine_type').val();
  if (!e_type) { notify('엔진 타입을 선택하세요.', 'warning'); return; }

  var cfg = { name: name, engine_type: e_type };
  var schemaObj = available_engine_schemas.find(function (s) { return s.engine_id === e_type; });
  if (schemaObj && schemaObj.config_schema) {
    for (var i = 0; i < schemaObj.config_schema.length; i++) {
      var f = schemaObj.config_schema[i];
      var el = $('#dl_field_' + f.name);
      if (f.type === 'checkbox') cfg[f.name] = el.is(':checked');
      else if (f.type === 'number') cfg[f.name] = parseInt(el.val()) || 0;
      else cfg[f.name] = el.val().trim();
    }
  }

  notify('엔진 연결 테스트 중...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/test_downloader',
    type: 'POST',
    data: { downloader_json: JSON.stringify(cfg) },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify(data.msg || '연결 성공', 'success');
      } else {
        notify('연결 실패: ' + (data.msg || '오류'), 'warning');
      }
    },
    error: function () {
      notify('연결 테스트 통신 실패', 'danger');
    }
  });
});

$(document).on('click', '#downloader_save_btn', function (e) {
  e.preventDefault();
  var name = $('#dl_name').val().trim();
  var e_type = $('#dl_engine_type').val();
  if (!name) { notify('식별명을 입력하세요.', 'warning'); return; }
  if (!e_type) { notify('엔진 타입을 선택하세요.', 'warning'); return; }

  var cfg = {
    name: name,
    engine_type: e_type,
    enabled: $('#dl_enabled').is(':checked')
  };

  var schemaObj = available_engine_schemas.find(function (s) { return s.engine_id === e_type; });
  if (schemaObj && schemaObj.config_schema) {
    for (var i = 0; i < schemaObj.config_schema.length; i++) {
      var f = schemaObj.config_schema[i];
      var el = $('#dl_field_' + f.name);
      if (f.type === 'checkbox') cfg[f.name] = el.is(':checked');
      else if (f.type === 'number') cfg[f.name] = parseInt(el.val()) || 0;
      else cfg[f.name] = el.val().trim();
    }
  }

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/save_downloader',
    type: 'POST',
    data: { downloader_json: JSON.stringify(cfg) },
    dataType: 'json',
    success: function (data) {
      notify('다운로더 설정이 저장되었습니다.', 'success');
      $('#downloader_modal').modal('hide');
      current_downloaders = data.downloaders || [];
      render_downloaders(current_downloaders);
    }
  });
});

$(document).on('click', '.delete_dl_btn', function (e) {
  e.preventDefault();
  var target_name = $(this).data('name');
  if (!confirm('[' + target_name + '] 다운로더를 삭제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/delete_downloader',
    type: 'POST',
    data: { name: target_name },
    dataType: 'json',
    success: function (data) {
      notify('삭제되었습니다.', 'success');
      current_downloaders = data.downloaders || [];
      render_downloaders(current_downloaders);
    }
  });
});

$(document).on('click', '#download_script_manage_btn', function (e) {
  e.preventDefault();
  load_download_script_list();
  $('#download_script_modal').modal('show');
  setTimeout(function () { if (dl_engine_editor) dl_engine_editor.resize(); }, 200);
});

function load_download_script_list(selected_name) {
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/download_script_list',
    type: 'POST',
    dataType: 'json',
    success: function (data) {
      var files = data.files || [];
      var select = $('#download_script_select');
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
      }
    }
  });
}

$(document).on('change', '#download_script_select', function () {
  var filename = $(this).val();
  if (!filename) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/download_script_read',
    type: 'POST',
    data: { filename: filename },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        $('#download_script_name').val(data.filename);
        $('#download_script_code').val(data.content);
        if (dl_engine_editor) dl_engine_editor.setValue(data.content, -1);
        $('#download_script_status').text('불러오기 완료');
      } else {
        notify('파일 로드 실패: ' + (data.log || data.ret), 'warning');
      }
    }
  });
});

$(document).on('click', '#download_script_new_btn', function (e) {
  e.preventDefault();
  $('#download_script_select').val('');
  $('#download_script_name').val('engine_new.py');
  var SKELETON = '# -*- coding: utf-8 -*-\nfrom feeder.util_download import BaseDownloadEngine\n\nclass CustomEngine(BaseDownloadEngine):\n    ENGINE_ID = "custom"\n    ENGINE_NAME = "커스텀 다운로더"\n    CONFIG_SCHEMA = []\n    def add_magnet(self, link, title=None):\n        return True, "task_1", ""\n    def get_status(self, task_ids=None):\n        return [], ""\n    def delete_task(self, task_id):\n        return True\n';
  $('#download_script_code').val(SKELETON);
  if (dl_engine_editor) dl_engine_editor.setValue(SKELETON, -1);
  $('#download_script_status').text('새 템플릿 로드');
});

$(document).on('click', '#download_script_save_btn', function (e) {
  e.preventDefault();
  var filename = $('#download_script_name').val().trim();
  var content = dl_engine_editor ? dl_engine_editor.getValue() : $('#download_script_code').val();
  if (!filename) { notify('파일명을 입력하세요.', 'warning'); return; }

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/download_script_save',
    type: 'POST',
    data: { filename: filename, content: content },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify('엔진 스크립트가 저장되었습니다.', 'success');
        $('#download_script_status').text('저장 완료 (' + filename + ')');
        load_download_script_list(data.filename);
        available_engine_schemas = data.schemas || [];
        update_engine_type_dropdown();
      } else {
        notify('저장 실패: ' + (data.log || data.ret), 'danger');
      }
    }
  });
});

$(document).on('click', '#download_script_delete_btn', function (e) {
  e.preventDefault();
  var filename = $('#download_script_name').val().trim();
  if (!filename) return;
  if (!confirm('[' + filename + '] 엔진 스크립트를 삭제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/download_script_delete',
    type: 'POST',
    data: { filename: filename },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify('삭제되었습니다.', 'success');
        load_download_script_list();
      } else {
        notify('삭제 실패', 'danger');
      }
    }
  });
});

function update_profile_dest_type_dropdown(selected_type) {
  var select = $('#profile_dest_type');
  select.empty();
  if (!available_transporter_schemas || available_transporter_schemas.length === 0) {
    select.append('<option value="">-- 등록된 이송 핸들러 없음 --</option>');
    return;
  }
  for (var i = 0; i < available_transporter_schemas.length; i++) {
    var s = available_transporter_schemas[i];
    var isSel = (s.transporter_id === selected_type) ? 'selected' : '';
    select.append('<option value="' + s.transporter_id + '" ' + isSel + '>' + s.transporter_name + ' (' + s.transporter_id + ')</option>');
  }
}

function render_dynamic_dest_fields(transporter_id, current_values) {
  var container = $('#profile_dynamic_dest_fields');
  container.empty();

  var schemaObj = available_transporter_schemas.find(function (s) { return s.transporter_id === transporter_id; });
  if (!schemaObj || !schemaObj.config_schema || schemaObj.config_schema.length === 0) {
    container.html('<div class="text-muted small p-2 text-center">해당 목적지에 별도 세부 설정 항목이 없습니다.</div>');
    return;
  }

  var html = '<hr><h6 class="text-info font-weight-bold mb-3"><i class="fa fa-folder-open mr-1"></i>' + schemaObj.transporter_name + ' 세부 설정</h6>';
  var fields = schemaObj.config_schema;

  for (var i = 0; i < fields.length; i++) {
    var f = fields[i];
    var val = (current_values && current_values[f.name] !== undefined) ? current_values[f.name] : (f.default !== undefined ? f.default : '');
    var ph = f.placeholder || '';
    var descHtml = f.desc ? '<small class="form-text text-muted">' + f.desc + '</small>' : '';

    html += '<div class="form-group row mb-2">';
    html += '  <label class="col-sm-3 col-form-label text-right font-weight-bold">' + f.label + '</label>';
    html += '  <div class="col-sm-9">';

    if (f.type === 'checkbox') {
      var isChk = (val === true || val === 'true' || val === 'On' || val === 'on') ? 'checked' : '';
      html += '    <input type="checkbox" id="dest_field_' + f.name + '" class="mt-2" ' + isChk + '>';
    } else if (f.type === 'number') {
      html += '    <input type="number" id="dest_field_' + f.name + '" class="form-control form-control-sm" value="' + val + '" placeholder="' + ph + '">';
    } else if (f.type === 'password') {
      html += '    <input type="password" id="dest_field_' + f.name + '" class="form-control form-control-sm" value="' + val + '" placeholder="' + ph + '">';
    } else {
      html += '    <input type="text" id="dest_field_' + f.name + '" class="form-control form-control-sm" value="' + val + '" placeholder="' + ph + '">';
    }

    html += descHtml;
    html += '  </div>';
    html += '</div>';
  }
  container.html(html);
}

$('#profile_dest_type').change(function () {
  render_dynamic_dest_fields($(this).val(), null);
});

function render_profiles(data) {
  var tbody = $('#profile_list_tbody');
  if (!tbody.length) return;
  if (!data || data.length === 0) {
    tbody.html('<tr><td colspan="5" class="py-4 text-muted">등록된 다운로드 프로필이 없습니다.</td></tr>');
    return;
  }
  var str = '';
  for (var i = 0; i < data.length; i++) {
    var p = data[i];
    var feedsHtml = (p.feeds || []).map(function (f) { return '<span class="badge badge-info mr-1">' + f + '</span>'; }).join('');
    var chainHtml = (p.priority_chain || []).map(function (c, idx) {
      return '<span class="badge badge-secondary mr-1">' + (idx + 1) + '. ' + c + '</span>';
    }).join(' <i class="fa fa-arrow-right text-muted small mr-1"></i> ');

    var dest = p.destination || {};
    var destInfo = '<span class="badge badge-dark">' + (dest.type || 'local') + '</span>';
    if (dest.type === 'rclone_simple' && dest.remote_path) {
      destInfo += '<br><small class="text-muted">' + dest.remote_path + '</small>';
    } else if (dest.type === 'gdrive_rotation') {
      destInfo = '<span class="badge badge-success">GDrive 계정 풀</span>';
      if (dest.complete_path) destInfo += '<br><small class="text-muted">' + dest.complete_path + '</small>';
    } else if (dest.type === 'colab_gdrive') {
      destInfo = '<span class="badge badge-primary">Colab GDrive</span>';
      if (dest.remote_name) destInfo += '<br><small class="text-muted">' + dest.remote_name + ' (' + (dest.buffer_limit_gb || 50) + 'GB 버퍼)</small>';
    }

    str += '<tr>';
    str += '  <td class="font-weight-bold">' + p.name + '</td>';
    str += '  <td>' + feedsHtml + '</td>';
    str += '  <td>' + chainHtml + '</td>';
    str += '  <td>' + destInfo + '</td>';
    str += '  <td>';
    str += '    <div class="btn-group btn-group-sm">';
    str += '      <button type="button" class="btn btn-primary text-white edit_profile_btn" data-index="' + i + '">수정</button>';
    str += '      <button type="button" class="btn btn-danger text-white delete_profile_btn" data-name="' + p.name + '">삭제</button>';
    str += '    </div>';
    str += '  </td>';
    str += '</tr>';
  }
  tbody.html(str);
}

function update_profile_modal_dropdowns() {
  var feedSelect = $('#profile_feed_select');
  feedSelect.empty();
  if (available_feeds && available_feeds.length > 0) {
    for (var i = 0; i < available_feeds.length; i++) {
      var fname = available_feeds[i].name || available_feeds[i];
      feedSelect.append('<option value="' + fname + '">' + fname + '</option>');
    }
  } else {
    feedSelect.append('<option value="">-- 등록된 피드 없음 --</option>');
  }

  var engineSelect = $('#profile_engine_select');
  engineSelect.empty();
  if (current_downloaders && current_downloaders.length > 0) {
    for (var j = 0; j < current_downloaders.length; j++) {
      var d = current_downloaders[j];
      engineSelect.append('<option value="' + d.name + '">' + d.name + ' (' + d.engine_type + ')</option>');
    }
  } else {
    engineSelect.append('<option value="">-- 등록된 다운로더 엔진 없음 --</option>');
  }
}

function render_selected_feeds() {
  var container = $('#profile_selected_feeds_container');
  container.empty();
  if (!modal_profile_feeds || modal_profile_feeds.length === 0) {
    container.html('<span class="text-muted small">선택된 피드가 없습니다.</span>');
    return;
  }
  for (var i = 0; i < modal_profile_feeds.length; i++) {
    var f = modal_profile_feeds[i];
    var badgeClass = (f === '*') ? 'badge-warning' : 'badge-info';
    var badgeHtml = $(
      '<span class="badge ' + badgeClass + ' mr-2 mb-1 p-2 font-weight-normal" style="font-size: 0.85rem;">' +
      f + ' <a href="#" class="text-white ml-1 remove-profile-feed-btn" data-feed="' + f + '">&times;</a></span>'
    );
    container.append(badgeHtml);
  }
}

function render_chain_table() {
  var tbody = $('#profile_chain_table_tbody');
  tbody.empty();
  if (!modal_profile_chain || modal_profile_chain.length === 0) {
    tbody.html('<tr><td colspan="3" class="text-muted py-2">등록된 다운로더 엔진이 없습니다.</td></tr>');
    return;
  }
  for (var i = 0; i < modal_profile_chain.length; i++) {
    var engine = modal_profile_chain[i];
    var str = '<tr>';
    str += '  <td class="font-weight-bold">' + (i + 1) + '순위</td>';
    str += '  <td class="text-left font-weight-bold text-primary">' + engine + '</td>';
    str += '  <td>';
    str += '    <div class="btn-group btn-group-sm">';
    str += '      <button type="button" class="btn btn-xs btn-secondary text-white chain-move-btn" data-dir="up" data-index="' + i + '" ' + (i === 0 ? 'disabled' : '') + '>▲</button>';
    str += '      <button type="button" class="btn btn-xs btn-secondary text-white chain-move-btn" data-dir="down" data-index="' + i + '" ' + (i === modal_profile_chain.length - 1 ? 'disabled' : '') + '>▼</button>';
    str += '      <button type="button" class="btn btn-xs btn-danger text-white chain-remove-btn" data-index="' + i + '">제외</button>';
    str += '    </div>';
    str += '  </td>';
    str += '</tr>';
    tbody.append(str);
  }
}

$(document).on('click', '#btn_add_profile_feed', function (e) {
  e.preventDefault();
  var sel = $('#profile_feed_select').val();
  if (!sel) return;
  if (modal_profile_feeds.indexOf(sel) !== -1) {
    notify('이미 목록에 포함된 피드입니다.', 'warning');
    return;
  }
  modal_profile_feeds.push(sel);
  render_selected_feeds();
});

$(document).on('click', '#btn_add_all_feed', function (e) {
  e.preventDefault();
  if (modal_profile_feeds.indexOf('*') !== -1) return;
  modal_profile_feeds.push('*');
  render_selected_feeds();
});

$(document).on('click', '.remove-profile-feed-btn', function (e) {
  e.preventDefault();
  var target = $(this).data('feed');
  modal_profile_feeds = modal_profile_feeds.filter(function (f) { return f !== target; });
  render_selected_feeds();
});

$(document).on('click', '#btn_add_profile_engine', function (e) {
  e.preventDefault();
  var eng = $('#profile_engine_select').val();
  if (!eng) return;
  if (modal_profile_chain.indexOf(eng) !== -1) {
    notify('이미 체인에 포함된 다운로더입니다.', 'warning');
    return;
  }
  modal_profile_chain.push(eng);
  render_chain_table();
});

$(document).on('click', '.chain-move-btn', function (e) {
  e.preventDefault();
  var idx = parseInt($(this).data('index'));
  var dir = $(this).data('dir');
  if (dir === 'up' && idx > 0) {
    var temp = modal_profile_chain[idx - 1];
    modal_profile_chain[idx - 1] = modal_profile_chain[idx];
    modal_profile_chain[idx] = temp;
  } else if (dir === 'down' && idx < modal_profile_chain.length - 1) {
    var temp2 = modal_profile_chain[idx + 1];
    modal_profile_chain[idx + 1] = modal_profile_chain[idx];
    modal_profile_chain[idx] = temp2;
  }
  render_chain_table();
});

$(document).on('click', '.chain-remove-btn', function (e) {
  e.preventDefault();
  var idx = parseInt($(this).data('index'));
  modal_profile_chain.splice(idx, 1);
  render_chain_table();
});

$(document).on('click', '#profile_add_btn', function (e) {
  e.preventDefault();
  $('#profile_modal_title').text('다운로드 프로필 추가');
  $('#profile_mode').val('add');
  $('#profile_name').val('').prop('readonly', false);
  $('#profile_append_hash_on_conflict').prop('checked', true);

  var default_trans = available_transporter_schemas.length > 0 ? available_transporter_schemas[0].transporter_id : '';
  update_profile_dest_type_dropdown(default_trans);
  render_dynamic_dest_fields(default_trans, null);

  modal_profile_feeds = ['*'];
  modal_profile_chain = [];

  update_profile_modal_dropdowns();
  render_selected_feeds();
  render_chain_table();
  $('#profile_modal').modal('show');
});

$(document).on('click', '.edit_profile_btn', function (e) {
  e.preventDefault();
  var idx = $(this).data('index');
  var p = current_profiles[idx];

  $('#profile_modal_title').text('다운로드 프로필 수정: ' + p.name);
  $('#profile_mode').val('edit');
  $('#profile_name').val(p.name).prop('readonly', true);
  $('#profile_append_hash_on_conflict').prop('checked', p.append_hash_on_conflict !== false);

  var dest = p.destination || {};
  update_profile_dest_type_dropdown(dest.type || '');
  render_dynamic_dest_fields(dest.type || '', dest);

  modal_profile_feeds = (p.feeds && p.feeds.length > 0) ? JSON.parse(JSON.stringify(p.feeds)) : ['*'];
  modal_profile_chain = (p.priority_chain && p.priority_chain.length > 0) ? JSON.parse(JSON.stringify(p.priority_chain)) : [];

  update_profile_modal_dropdowns();
  render_selected_feeds();
  render_chain_table();
  $('#profile_modal').modal('show');
});

$(document).on('click', '#profile_save_btn', function (e) {
  e.preventDefault();
  var name = $('#profile_name').val().trim();
  if (!name) { notify('프로필명을 입력하세요.', 'warning'); return; }

  if (!modal_profile_feeds || modal_profile_feeds.length === 0) {
    notify('적어도 하나 이상의 피드를 선택하세요 (*는 전체 대상).', 'warning');
    return;
  }
  if (!modal_profile_chain || modal_profile_chain.length === 0) {
    notify('우선순위 체인에 다운로더 엔진을 1개 이상 추가하세요.', 'warning');
    return;
  }

  var destType = $('#profile_dest_type').val();
  var destObj = { type: destType };

  var schemaObj = available_transporter_schemas.find(function (s) { return s.transporter_id === destType; });
  if (schemaObj && schemaObj.config_schema) {
    for (var i = 0; i < schemaObj.config_schema.length; i++) {
      var f = schemaObj.config_schema[i];
      var el = $('#dest_field_' + f.name);
      if (f.type === 'checkbox') destObj[f.name] = el.is(':checked');
      else if (f.type === 'number') destObj[f.name] = parseInt(el.val()) || 0;
      else destObj[f.name] = el.val().trim();
    }
  }

  var profile_obj = {
    name: name,
    feeds: modal_profile_feeds,
    priority_chain: modal_profile_chain,
    append_hash_on_conflict: $('#profile_append_hash_on_conflict').is(':checked'),
    destination: destObj
  };

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/save_profile',
    type: 'POST',
    data: { profile_json: JSON.stringify(profile_obj) },
    dataType: 'json',
    success: function (data) {
      notify('프로필이 저장되었습니다.', 'success');
      $('#profile_modal').modal('hide');
      current_profiles = data.profiles || [];
      render_profiles(current_profiles);
    }
  });
});

$(document).on('click', '.delete_profile_btn', function (e) {
  e.preventDefault();
  var target_name = $(this).data('name');
  if (!confirm('[' + target_name + '] 프로필을 삭제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/delete_profile',
    type: 'POST',
    data: { name: target_name },
    dataType: 'json',
    success: function (data) {
      notify('삭제되었습니다.', 'success');
      current_profiles = data.profiles || [];
      render_profiles(current_profiles);
    }
  });
});

function render_accounts(accounts, stats) {
  var tbody = $('#gdrive_account_list_tbody');
  if (!tbody.length) return;
  if (!accounts || accounts.length === 0) {
    tbody.html('<tr><td colspan="5" class="py-4 text-muted">등록된 구글 드라이브 계정이 없습니다.</td></tr>');
    return;
  }
  var usageMap = stats.usage_24h || {};
  var blockedMap = stats.blocked_remains || {};

  var str = '';
  for (var i = 0; i < accounts.length; i++) {
    var acc = accounts[i];
    var uname = acc.username;
    var bytes = usageMap[uname] || 0;
    var mb = (bytes / (1024 * 1024)).toFixed(1);
    var gb = (bytes / (1024 * 1024 * 1024)).toFixed(2);
    var remoteDisplay = acc.remote_name ? '<code>' + acc.remote_name + '</code>' : '<span class="text-muted small">기본 리모트 사용</span>';

    var blockBadge = '';
    if (blockedMap[uname]) {
      var remainH = (blockedMap[uname] / 3600).toFixed(1);
      blockBadge = ' <span class="badge badge-danger">차단 (' + remainH + 'h 남음)</span>';
    } else {
      blockBadge = ' <span class="badge badge-success">정상</span>';
    }

    str += '<tr>';
    str += '  <td class="font-weight-bold text-left">' + uname + blockBadge + '</td>';
    str += '  <td><code>' + (acc.mydrive_rclone_id || '-') + '</code></td>';
    str += '  <td>' + remoteDisplay + '</td>';
    str += '  <td>' + gb + ' GB (' + mb + ' MB)</td>';
    str += '  <td>';
    str += '    <div class="btn-group btn-group-sm">';
    str += '      <button type="button" class="btn btn-xs btn-primary text-white edit_acc_btn mr-1" data-index="' + i + '">수정</button>';
    str += '      <button type="button" class="btn btn-xs btn-danger text-white remove_acc_btn" data-index="' + i + '">제외</button>';
    str += '    </div>';
    str += '  </td>';
    str += '</tr>';
  }
  tbody.html(str);
}

$(document).on('click', '.edit_acc_btn', function (e) {
  e.preventDefault();
  var idx = parseInt($(this).data('index'));
  var acc = current_accounts[idx];
  if (!acc) return;

  $('#modal_acc_index').val(idx);
  $('#gdrive_account_modal_title').text('구글 드라이브 계정 수정: ' + acc.username);
  $('#modal_acc_username').val(acc.username || '');
  $('#modal_acc_mydrive_id').val(acc.mydrive_rclone_id || '');
  $('#modal_acc_remote_name').val(acc.remote_name || '');
  $('#gdrive_account_modal').modal('show');
});

$(document).on('click', '#gdrive_account_save_btn', function (e) {
  e.preventDefault();
  var idx = parseInt($('#modal_acc_index').val());
  var uname = $('#modal_acc_username').val().trim();
  var mydrive_id = $('#modal_acc_mydrive_id').val().trim();
  var rname = $('#modal_acc_remote_name').val().trim();

  if (!uname) { notify('계정 식별자를 입력하세요.', 'warning'); return; }
  if (!mydrive_id) { notify('내 드라이브 폴더 ID를 입력하세요.', 'warning'); return; }

  if (idx >= 0 && idx < current_accounts.length) {
    current_accounts[idx].username = uname;
    current_accounts[idx].mydrive_rclone_id = mydrive_id;
    if (rname) current_accounts[idx].remote_name = rname;
    else delete current_accounts[idx].remote_name;
  }

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/save_accounts',
    type: 'POST',
    data: { accounts_json: JSON.stringify(current_accounts) },
    dataType: 'json',
    success: function (data) {
      notify('계정 정보가 수정되었습니다.', 'success');
      $('#gdrive_account_modal').modal('hide');
      current_accounts = data.accounts || [];
      render_accounts(current_accounts, {});
    }
  });
});

$(document).on('click', '#gdrive_batch_modal_btn', function (e) {
  e.preventDefault();
  $('#gdrive_batch_textarea').val('');
  $('#gdrive_batch_modal').modal('show');
});

$(document).on('click', '#gdrive_batch_save_btn', function (e) {
  e.preventDefault();
  var text = $('#gdrive_batch_textarea').val().trim();
  if (!text) { notify('입력된 계정 정보가 없습니다.', 'warning'); return; }

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/batch_add_accounts',
    type: 'POST',
    data: { batch_text: text },
    dataType: 'json',
    success: function (data) {
      notify(data.added_count + '개 계정이 등록되었습니다.', 'success');
      $('#gdrive_batch_modal').modal('hide');
      current_accounts = data.accounts || [];
      render_accounts(current_accounts, {});
    }
  });
});

$(document).on('click', '.remove_acc_btn', function (e) {
  e.preventDefault();
  var idx = $(this).data('index');
  var acc = current_accounts[idx];
  var accName = acc ? acc.username : '해당';
  if (!confirm('[' + accName + '] 계정을 제외하시겠습니까?')) return;

  current_accounts.splice(idx, 1);
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/save_accounts',
    type: 'POST',
    data: { accounts_json: JSON.stringify(current_accounts) },
    dataType: 'json',
    success: function (data) {
      notify('계정이 삭제되었습니다.', 'info');
      current_accounts = data.accounts || [];
      render_accounts(current_accounts, {});
    }
  });
});

$(document).on('click', '#gdrive_reset_blocks_btn', function (e) {
  e.preventDefault();
  if (!confirm('모든 구글 드라이브 계정의 차단 상태를 즉시 해제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/reset_blocked_accounts',
    type: 'POST',
    dataType: 'json',
    success: function (data) {
      notify('모든 계정 차단이 해제되었습니다.', 'success');
      render_accounts(current_accounts, data.stats || {});
    }
  });
});

$(document).on('click', '#history_import_modal_btn', function (e) {
  e.preventDefault();
  $('#import_text_content').val('');
  $('#history_import_modal').modal('show');
});

$(document).on('click', '#btn_run_import_text', function (e) {
  e.preventDefault();
  var text = $('#import_text_content').val().trim();
  if (!text) { notify('임포트할 마그넷 목록을 입력하세요.', 'warning'); return; }
  notify('마그넷 목록 임포트 중...', 'info');

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/import_history_text',
    type: 'POST',
    data: { import_text: text },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify('이력 등록 완료: ' + data.added + '건 추가 (중복 ' + data.skipped + '건 스킵)', 'success');
        $('#history_import_modal').modal('hide');
      } else {
        notify(data.msg || '임포트 실패', 'danger');
      }
    }
  });
});

$(document).on('click', '#btn_run_import_db', function (e) {
  e.preventDefault();
  var dbPath = $('#import_db_path').val().trim();
  if (!dbPath) { notify('SQLite DB 파일 경로를 입력하세요.', 'warning'); return; }

  var formData = {
    db_path: dbPath,
    table_name: $('#import_table_name').val().trim(),
    magnet_col: $('#import_magnet_col').val().trim(),
    title_col: $('#import_title_col').val().trim(),
    file_name_col: $('#import_file_name_col').val().trim(),
    where_clause: $('#import_where_clause').val().trim()
  };

  notify('DB 이력 흡수 중...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/import_history_db',
    type: 'POST',
    data: formData,
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify('DB 이력 흡수 완료: 총 ' + data.added + '건 완료 등록 (중복 ' + data.skipped + '건 스킵)', 'success');
        $('#history_import_modal').modal('hide');
      } else {
        notify('DB 임포트 실패: ' + (data.msg || '오류'), 'danger');
      }
    }
  });
});

$(document).on('click', '#transporter_script_manage_btn', function (e) {
  e.preventDefault();
  load_transporter_script_list();
  $('#transporter_script_modal').modal('show');
  setTimeout(function () { if (dl_trans_editor) dl_trans_editor.resize(); }, 200);
});

function load_transporter_script_list(selected_name) {
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/transporter_script_list',
    type: 'POST',
    dataType: 'json',
    success: function (data) {
      var files = data.files || [];
      var select = $('#transporter_script_select');
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
      }
    }
  });
}

$(document).on('change', '#transporter_script_select', function () {
  var filename = $(this).val();
  if (!filename) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/transporter_script_read',
    type: 'POST',
    data: { filename: filename },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        $('#transporter_script_name').val(data.filename);
        $('#transporter_script_code').val(data.content);
        if (dl_trans_editor) dl_trans_editor.setValue(data.content, -1);
        $('#transporter_script_status').text('불러오기 완료');
      } else {
        notify('파일 로드 실패: ' + (data.log || data.ret), 'warning');
      }
    }
  });
});

$(document).on('click', '#transporter_script_new_btn', function (e) {
  e.preventDefault();
  $('#transporter_script_select').val('');
  $('#transporter_script_name').val('trans_new.py');
  $('#transporter_script_code').val(TRANSPORTER_SKELETON);
  if (dl_trans_editor) dl_trans_editor.setValue(TRANSPORTER_SKELETON, -1);
  $('#transporter_script_status').text('새 이송 템플릿 로드');
});

$(document).on('click', '#transporter_script_save_btn', function (e) {
  e.preventDefault();
  var filename = $('#transporter_script_name').val().trim();
  var content = dl_trans_editor ? dl_trans_editor.getValue() : $('#transporter_script_code').val();
  if (!filename) { notify('파일명을 입력하세요.', 'warning'); return; }

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/transporter_script_save',
    type: 'POST',
    data: { filename: filename, content: content },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify('이송 스크립트가 저장되었습니다.', 'success');
        $('#transporter_script_status').text('저장 완료 (' + filename + ')');
        load_transporter_script_list(data.filename);
        available_transporter_schemas = data.transporter_schemas || [];
        update_profile_dest_type_dropdown();
      } else {
        notify('저장 실패: ' + (data.log || data.ret), 'danger');
      }
    }
  });
});

$(document).on('click', '#transporter_script_delete_btn', function (e) {
  e.preventDefault();
  var filename = $('#transporter_script_name').val().trim();
  if (!filename) return;
  if (!confirm('[' + filename + '] 이송 스크립트를 삭제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/transporter_script_delete',
    type: 'POST',
    data: { filename: filename },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify('삭제되었습니다.', 'success');
        load_transporter_script_list();
      } else {
        notify('삭제 실패', 'danger');
      }
    }
  });
});
