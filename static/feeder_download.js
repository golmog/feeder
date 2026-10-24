// =============================================================================
// DOWNLOAD 모듈 전용 스크립트 (Queue & List & Setting 통합)
// =============================================================================
var sse_source = null;
var feeder_dashboard = null;
var queue_page_closed = false;
var download_activity_request = null;
var pipeline_control_request = null;
var cached_download_list = [];
var pipeline_control_timer = null;
var pipeline_control_loading = false;
var pipeline_control_changing = false;
var pipeline_control_version = 0;
var last_pipeline_control = null;
var pipeline_control_sse_time = 0;
var download_activity_source = null;
var download_activity_timer = null;
var download_activity_poll_timer = null;
var download_activity_cursor = 0;
var download_activity_last_time = 0;
var download_activity_received = 0;
var download_activity_loading = false;

var current_downloaders = [];
var current_profiles = [];
var current_accounts = [];
var available_feeds = [];
var modal_profile_feeds = [];
var modal_profile_chain = [];
var modal_profile_settings = {};
var modal_engine_destinations = {};
var available_engine_schemas = [];
var available_transporter_schemas = [];

$(document).on('click', '#test_remote_worker_btn, #save_remote_worker_btn', function () {
  var save = this.id === 'save_remote_worker_btn';
  var buttons = $('#test_remote_worker_btn, #save_remote_worker_btn');
  buttons.prop('disabled', true);
  $('#remote_worker_result').text(save ? '연결 설정을 저장합니다…' : '워커 접속과 인증을 확인합니다…');
  $.ajax({url: '/' + package_name + '/ajax/' + sub + '/' + (save ? 'save_remote_worker' : 'test_remote_worker'), type: 'POST', timeout: 30000,
    data: {worker_url: $('#worker_connection_url').val().trim(), worker_token: $('#worker_connection_token').val().trim()},
    success: function (data) {
      $('#remote_worker_result').text(data.msg);
      if (save && data.ret === 'success') $('#worker_connection_token').val('').attr('placeholder', '저장된 토큰 사용 · 변경할 때만 입력');
      $.notify(data.msg, {type: data.ret === 'success' ? 'success' : 'warning'});
    },
    error: function () { $('#remote_worker_result').text('워커 연결 상태를 확인하지 못했습니다. 서버 상태와 Tailscale 연결을 확인해주세요.'); },
    complete: function () { buttons.prop('disabled', false); }
  });
});
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
    feeder_dashboard = new FeederDashboard();
    feeder_dashboard.start();
    init_download_activity();
    refresh_pipeline_control();
    pipeline_control_timer = setInterval(refresh_pipeline_control, 3000);
    try {
      localStorage.setItem('feeder_last_download_page', 'queue');
      sync_feeder_header_navbar();
    } catch (err) {}

    load_retry_profiles();

    if (typeof enable_sse !== 'undefined' && enable_sse && window.EventSource) init_sse_listener();
  }

  // List 다운로드 이력 화면 진입 시
  if ($('#download_list_tbody').length > 0) {
    try {
      localStorage.setItem('feeder_last_download_page', 'list');
      sync_feeder_header_navbar();
    } catch (err) {}

    var saved_status = localStorage.getItem(sub + '_status_filter') || 'all';
    var saved_size = localStorage.getItem(sub + '_page_size') || '25';
    var saved_word = localStorage.getItem(sub + '_search_word') || '';
    var saved_page = localStorage.getItem(sub + '_current_page') || '1';

    $('#status_filter').val(saved_status);
    $('#page_size').val(saved_size);
    $('#search_word').val(saved_word);

    load_retry_profiles();
    window.globalRequestSearch(saved_page, false);
  }

  // Setting 화면 진입 시
  if ($('#downloader_list_tbody').length > 0) {
    try {
      localStorage.setItem('feeder_last_download_page', 'setting');
      sync_feeder_header_navbar();
    } catch (err) {}

    use_collapse('download_use_local_staging');
    use_collapse('download_use_gdrive_pool');
    use_collapse('download_gdrive_use_impersonate');

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
function format_download_activity_time(timestamp) {
  var date = new Date(timestamp);
  return [date.getHours(), date.getMinutes(), date.getSeconds()].map(function (value) {
    return String(value).padStart(2, '0');
  }).join(':');
}

function render_download_activity(data) {
  if (!data || !Array.isArray(data.entries)) return;
  download_activity_received = Date.now();
  var container = document.getElementById('download_activity_logs');
  if (!container) return;
  data.entries.forEach(function (entry) {
    if (entry.id <= download_activity_cursor) return;
    download_activity_cursor = entry.id;
    download_activity_last_time = entry.timestamp * 1000;
    var row = document.createElement('div');
    row.className = entry.level === 'error' ? 'text-danger' : (entry.level === 'warning' ? 'text-warning' : '');
    row.textContent = format_download_activity_time(download_activity_last_time) +
      ' [' + entry.stage + '] ' + entry.message;
    container.appendChild(row);
    while (container.children.length > 200) container.removeChild(container.firstChild);
  });
  if ($('#download_activity_follow').is(':checked')) container.scrollTop = container.scrollHeight;
  update_download_activity_time();
}

function update_download_activity_time() {
  var now = Date.now();
  var activity = download_activity_last_time ? '마지막 작업 활동 ' + Math.max(0, Math.floor((now - download_activity_last_time) / 1000)) + '초 전' : '아직 기록된 작업 활동이 없습니다.';
  var received = download_activity_received ? ' · 마지막 수신 ' + Math.max(0, Math.floor((now - download_activity_received) / 1000)) + '초 전' : '';
  $('#download_activity_summary').text(activity + received);
}

function init_download_activity() {
  var url = '/' + package_name + '/api/' + sub + '/activity' + (apikey ? '?apikey=' + encodeURIComponent(apikey) : '');
  download_activity_timer = setInterval(update_download_activity_time, 1000);
  var poll = function () {
    if (queue_page_closed || download_activity_loading || (download_activity_source && Date.now() - download_activity_received < 10000)) return;
    download_activity_loading = true;
    download_activity_request = $.ajax({url: url, data: {snapshot: '1', after: download_activity_cursor}, dataType: 'json', global: false, timeout: 10000})
      .done(function (data) {
        if (queue_page_closed) return;
        render_download_activity(data);
        $('#download_activity_connection').text('HTTP 수신 · SSE 연결과 독립 갱신').attr('data-state', 'ready');
      })
      .fail(function () {
        if (!queue_page_closed) $('#download_activity_connection').text('활동 기록 갱신 지연 · 재시도 중').attr('data-state', 'stale');
      })
      .always(function () { download_activity_loading = false; });
  };
  // SSE 첫 메시지를 기다리지 않고 로그도 먼저 읽는다.
  poll();
  download_activity_poll_timer = setInterval(poll, 3000);
  if (typeof enable_sse !== 'undefined' && enable_sse && window.EventSource) {
    download_activity_source = new EventSource(url);
    download_activity_source.onopen = function () { if (!queue_page_closed) $('#download_activity_connection').text('SSE 연결됨'); };
    download_activity_source.onmessage = function (event) {
      if (queue_page_closed) return;
      try {
        render_download_activity(JSON.parse(event.data));
        $('#download_activity_connection').text('SSE 연결됨').attr('data-state', 'ready');
      } catch (err) { $('#download_activity_connection').text('수신 데이터 확인 실패').attr('data-state', 'error'); }
    };
    download_activity_source.onerror = function () {
      if (!queue_page_closed) $('#download_activity_connection').text('SSE 재연결 중 · HTTP로 보완').attr('data-state', 'stale');
    };
    download_activity_source.addEventListener('activity_error', function () {
      if (!queue_page_closed) $('#download_activity_connection').text('활동 기록 조회 재시도 중').attr('data-state', 'error');
    });
  }
}

$(document).on('click', '#download_activity_clear', function () {
  $('#download_activity_logs').empty();
});

function init_sse_listener() {
  if (!window.EventSource) return;
  if (sse_source) sse_source.close();
  var connected = false;
  var sseUrl = '/' + package_name + '/api/' + sub + '/sse?version=2' + (apikey ? '&apikey=' + encodeURIComponent(apikey) : '');
  sse_source = new EventSource(sseUrl);
  sse_source.onopen = function () {
    if (queue_page_closed) return;
    $('#sse_status_badge').hide();
    if (connected && feeder_dashboard) feeder_dashboard.refresh(feeder_dashboard.pageState === 'error');
    connected = true;
  };
  sse_source.onmessage = function (event) {
    if (queue_page_closed || !feeder_dashboard) return;
    try {
      var data = JSON.parse(event.data);
      if (data.version === 2 && data.kind === 'refresh') feeder_dashboard.poll();
    } catch (err) {}
  };
  sse_source.onerror = function () {
    if (!queue_page_closed) $('#sse_status_badge').text('SSE 재연결 중 · HTTP 갱신 유지').show();
  };
}

window.addEventListener('pagehide', function () {
  queue_page_closed = true;
  pipeline_control_version++;
  if (feeder_dashboard) feeder_dashboard.close();
  if (download_activity_request) download_activity_request.abort();
  if (pipeline_control_request) pipeline_control_request.abort();
  if (download_activity_source) { download_activity_source.close(); download_activity_source = null; }
  if (download_activity_timer) { clearInterval(download_activity_timer); download_activity_timer = null; }
  if (download_activity_poll_timer) { clearInterval(download_activity_poll_timer); download_activity_poll_timer = null; }
  if (pipeline_control_timer) {
    clearInterval(pipeline_control_timer);
    pipeline_control_timer = null;
  }
  if (sse_source) {
    try { sse_source.close(); } catch (e) {}
    sse_source = null;
  }
});

function render_upload_queue_count(counts) {
  var current = counts.upload_current === undefined ? '—' : counts.upload_current;
  var total = counts.upload_total === undefined ? (counts.uploading === undefined ? '—' : counts.uploading) : counts.upload_total;
  $('#stat_uploading').text(current + ' / ' + total);
}

function render_pipeline_control(control) {
  if (last_pipeline_control && control.observed_at < last_pipeline_control.observed_at) return;
  last_pipeline_control = control;
  var labels = {
    idle: '작업 실행 대기 중',
    running: '작업 실행 중',
    stopping: '중단 예약됨 · 진행 중인 작업이 끝나면 중단합니다',
    paused: '작업 중단됨 · Feeder 업로드가 종료되어 FF를 재시작할 수 있습니다'
  };
  $('#pipeline_control_status').text(control.single_item_id
    ? '작업 #' + control.single_item_id + ' 개별 실행 중 · 전체 중단 유지'
    : (labels[control.state] || '작업 상태 확인 중...'));
  $('.queue_action_btn[data-action="execute_single"]').prop('disabled', control.state !== 'paused');
  $('#pipeline_pause_btn').toggle(!control.pause_requested).prop('disabled', pipeline_control_changing);
  $('#pipeline_resume_btn').toggle(!!control.pause_requested).prop('disabled', pipeline_control_changing)
    .html('<i class="fa fa-play mr-1"></i>' + (control.state === 'stopping' && !control.single_item_id ? '중단 예약 취소' : '작업 재개'));
}

function refresh_pipeline_control() {
  if (queue_page_closed || pipeline_control_loading || pipeline_control_changing) return;
  if (last_pipeline_control && sse_source && sse_source.readyState === 1 &&
      pipeline_control_sse_time && Date.now() - pipeline_control_sse_time < 10000) return;
  pipeline_control_loading = true;
  var version = pipeline_control_version;
  pipeline_control_request = $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/pipeline_control_state',
    type: 'POST', data: {dashboard_version: '2'}, dataType: 'json', timeout: 10000, global: false
  }).done(function (data) {
    if (version !== pipeline_control_version) return;
    if (data.ret === 'success' && data.counts) render_upload_queue_count(data.counts);
    if (data.ret === 'success' && data.control) render_pipeline_control(data.control);
    else pipeline_control_unavailable();
  }).fail(function () {
    if (version === pipeline_control_version) pipeline_control_unavailable();
  }).always(function () { pipeline_control_loading = false; });
}

function pipeline_control_unavailable() {
  $('#pipeline_control_status').text('작업 상태를 확인할 수 없습니다. 연결을 확인하세요.');
  $('#pipeline_pause_btn, #pipeline_resume_btn').prop('disabled', true);
  $('.queue_action_btn[data-action="execute_single"]').prop('disabled', true);
}

$('#pipeline_pause_btn, #pipeline_resume_btn').click(function () {
  if (pipeline_control_changing) return;
  var command = this.id === 'pipeline_pause_btn' ? 'pause_pipeline' : 'resume_pipeline';
  pipeline_control_changing = true;
  pipeline_control_version += 1;
  $('#pipeline_pause_btn, #pipeline_resume_btn').prop('disabled', true);
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/' + command,
    type: 'POST', dataType: 'json', timeout: 10000
  }).done(function (data) {
    if (queue_page_closed) return;
    if (data.ret === 'success' && data.control) {
      render_pipeline_control(data.control);
      notify(command === 'pause_pipeline' ? '진행 중인 작업이 끝나면 중단합니다.' : '작업 중단 상태를 해제했습니다.', 'info');
    } else {
      last_pipeline_control = null;
      pipeline_control_unavailable();
      notify(data.msg || '작업 상태 변경에 실패했습니다.', 'warning');
    }
  }).fail(function () {
    if (queue_page_closed) return;
    last_pipeline_control = null;
    pipeline_control_unavailable();
    notify('작업 상태 변경 결과를 확인하지 못했습니다. 상태를 다시 조회합니다.', 'warning');
  })
    .always(function () {
      if (queue_page_closed) return;
      pipeline_control_changing = false;
      if (last_pipeline_control) render_pipeline_control(last_pipeline_control);
      refresh_pipeline_control();
    });
});

function request_active_queue() {
  if (!queue_page_closed && feeder_dashboard) feeder_dashboard.refresh();
}

function queue_escape_html(value) {
  return String(value == null ? '' : value).replace(/[&<>"']/g, function (character) {
    return {'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'}[character];
  });
}

function queue_progress_percent(value) {
  var percent = Number(value);
  if (!Number.isFinite(percent)) return 0;
  return Math.round(Math.max(0, Math.min(100, percent)) * 10) / 10;
}

function queue_eta_text(value) {
  var text = String(value == null ? '' : value).trim();
  // 워커/구조화된 Rclone 통계는 초 숫자이고, 일반 Rclone 로그는 시간 단위가 붙어 있다.
  if (!/^\d+(?:\.\d+)?s?$/.test(text)) return text;
  var seconds = Math.ceil(Number(text.replace(/s$/, '')));
  if (!Number.isFinite(seconds)) return '';
  var hours = Math.floor(seconds / 3600);
  var minutes = Math.floor(seconds % 3600 / 60);
  var remaining = seconds % 60;
  var parts = [];
  if (hours) parts.push(hours + '시간');
  if (minutes) parts.push(minutes + '분');
  if (remaining || !parts.length) parts.push(remaining + '초');
  return parts.join(' ');
}

function render_queue_rows(list) {
  var tbody = $('#active_queue_tbody');
  if (!tbody.length) return;
  if (!list || list.length === 0) {
    cached_download_list = [];
    render_queue_rows.signatures = {};
    tbody.html('<tr><td colspan="3" class="py-4 text-muted">현재 진행 중인 활성 작업이 없습니다.</td></tr>');
    return;
  }

  cached_download_list = list.slice();
  // DB와 같은 단계 우선순위. 실시간 진행률은 같은 단계의 표시 순서만 보강한다.
  var ranks = {uploading:0, local_staging:1, relay_transferring:2, downloading:3, pending_upload:4,
    pending_local_staging:5, pending_relay:6, downloaded:7, waiting_transporter:8, pending:9};
  list = list.slice().sort(function (a, b) {
    var rankA = ranks[a.status] === undefined ? 99 : ranks[a.status];
    var rankB = ranks[b.status] === undefined ? 99 : ranks[b.status];
    if (rankA !== rankB) return rankA - rankB;
    if (rankA < 4 && Number(a.progress || 0) !== Number(b.progress || 0)) return Number(b.progress || 0) - Number(a.progress || 0);
    if (rankA < 99) return Number(a.id) - Number(b.id);
    return String(a.row_key).localeCompare(String(b.row_key));
  });

  // 엔진별 실시간 다운로드 속도 합산 집계
  var engineSpeeds = {};
  for (var k = 0; k < list.length; k++) {
    var rowItem = list[k];
    if (rowItem.status === 'downloading') {
      var spd = parseFloat(rowItem.download_speed) || 0;
      if (spd > 0) {
        var rawEng = rowItem.current_engine_name || rowItem.engine_name || '';
        var engKey = rawEng ? (rawEng.length <= 4 ? rawEng.toUpperCase() : rawEng) : '엔진';
        engineSpeeds[engKey] = (engineSpeeds[engKey] || 0) + spd;
      }
    }
  }

  var speedDisplay = '';
  var engKeys = Object.keys(engineSpeeds);
  if (engKeys.length === 1) {
    var singleKey = engKeys[0];
    speedDisplay = '<i class="fa fa-arrow-circle-o-down mr-1"></i>표시 중 ' + queue_escape_html(singleKey) + ' 다운로드: ' + format_bytes(engineSpeeds[singleKey]) + '/s';
  } else if (engKeys.length > 1) {
    var parts = [];
    for (var eIdx = 0; eIdx < engKeys.length; eIdx++) {
      var eKey = engKeys[eIdx];
      parts.push(queue_escape_html(eKey) + ': ' + format_bytes(engineSpeeds[eKey]) + '/s');
    }
    speedDisplay = '<i class="fa fa-arrow-circle-o-down mr-1"></i>표시 중 다운로드: ' + parts.join(' | ');
  } else {
    speedDisplay = '<i class="fa fa-arrow-circle-o-down mr-1"></i>다운로드: — (수신한 작업 기준)';
  }

  $('#queue_engine_speed_badge').html(speedDisplay);

  var currentRows = {}, signatures = {}, renderedIds = {}, ordered = [];
  var oldOrder = [];
  tbody.children('tr[data-queue-id]').each(function () {
    var key = $(this).attr('data-queue-id'); currentRows[key] = this; oldOrder.push(key);
  });
  var previous = render_queue_rows.signatures || {};
  var focus = document.activeElement;
  var focusedKey = focus && $(focus).closest('tr').attr('data-queue-id');
  var focusedAction = focus && $(focus).attr('data-action');
  for (var i = 0; i < list.length; i++) {
    var raw = list[i], rowKey = raw.row_key || ('db:' + raw.id);
    var signature = JSON.stringify(raw) + ':' + (last_pipeline_control ? last_pipeline_control.state : '');
    signatures[rowKey] = signature; renderedIds[rowKey] = true;
    if (previous[rowKey] === signature && currentRows[rowKey]) {
      ordered.push(currentRows[rowKey]); continue;
    }
    var it = {};
    Object.keys(raw).forEach(function (key) { it[key] = typeof raw[key] === 'string' ? queue_escape_html(raw[key]) : raw[key]; });
    var str = '';
    var statusBadge = '';
    if (it.status === 'pending') statusBadge = '<span class="badge badge-warning">대기 (Pending)</span>';
    else if (it.status === 'downloading') statusBadge = '<span class="badge badge-primary">다운로드</span>';
    else if (it.status === 'pending_local_staging' || it.status === 'local_staging') statusBadge = '<span class="badge badge-info">로컬 스테이징</span>';
    else if (it.status === 'pending_relay') statusBadge = '<span class="badge badge-warning">원격 릴레이 대기</span>';
    else if (it.status === 'relay_transferring') statusBadge = '<span class="badge badge-primary">원격 릴레이 전송 중</span>';
    else if (it.status === 'cancelled') statusBadge = '<span class="badge badge-secondary">삭제/취소</span>';
    else if (it.status === 'blocked') statusBadge = '<span class="badge badge-danger">차단</span>';
    else if (it.status === 'waiting_transporter') statusBadge = '<span class="badge badge-warning">이송 설정 복구 대기</span>';
    else if (it.status === 'downloaded') {
      if (it.destination_type === 'local') {
        statusBadge = '<span class="badge badge-success">다운로드 완료 (이동 대기)</span>';
      } else if (it.destination_type === 'rclone_simple') {
        statusBadge = '<span class="badge badge-primary">다운로드 완료 (Rclone 대기)</span>';
      } else {
        statusBadge = '<span class="badge badge-success">다운로드 완료 (업로드 대기)</span>';
      }
    }
    else if (it.status === 'engine_unverified') statusBadge = '<span class="badge badge-warning">DB 연결 확인 중</span>';
    else if (it.status === 'engine_unmanaged' || it.status === 'ad_unmanaged') {
      var engBadgeName = it.current_engine_name || it.feed_name || '엔진';
      statusBadge = '<span class="badge badge-secondary">' + engBadgeName + ' 미등록 (잔여)</span>';
    }
    else if (it.status === 'pending_upload' || it.status === 'uploading') statusBadge = '<span class="badge badge-primary">업로드</span>';
    else if (it.status === 'completed') statusBadge = '<span class="badge badge-success">최종 완료</span>';
    else if (it.status === 'failed') statusBadge = '<span class="badge badge-danger">실패</span>';
    else statusBadge = '<span class="badge badge-secondary">' + it.status + '</span>';

    var engineInfo = it.current_engine_name ? '<br><small class="text-muted">엔진: ' + it.current_engine_name + '</small>' : '';
    var errorMsg = it.error_message ? '<div class="mt-1 small text-danger font-weight-bold"><i class="fa fa-exclamation-triangle mr-1"></i>' + it.error_message + '</div>' : '';
    var timeStr = it.updated_time || it.created_time || '';
    var pathDisplay = '';
    if (it.status === 'engine_unverified' || it.status === 'engine_unmanaged' || it.status === 'ad_unmanaged') {
      pathDisplay = '<span class="text-muted"><i class="fa fa-cloud mr-1"></i>원격 클라우드 보관</span>';
    } else if (it.local_path) {
      pathDisplay = '<code>' + it.local_path + '</code>';
    } else {
      pathDisplay = '<span class="text-muted">-</span>';
    }

    var detailHtml = '';

    // 1행: 피드명 + 제목
    detailHtml += '<div class="mb-1" style="word-break: break-all;">';
    detailHtml += '  <span class="badge badge-dark mr-1">' + (it.feed_name || 'Feed') + '</span>';
    detailHtml += '  <strong style="font-size: 0.95rem;">' + it.title + '</strong>';
    detailHtml += '</div>';

    // 2행: 파일명 (용량)과 저장 경로
    var fsizeText = it.file_size ? ' (' + format_bytes(it.file_size) + ')' : '';
    var nameAndSize = it.file_name
      ? '<span class="font-weight-bold text-info mr-2"><i class="fa fa-folder-open-o mr-1"></i>' + it.file_name + fsizeText + '</span>'
      : (fsizeText ? '<span class="badge badge-secondary mr-2"><i class="fa fa-database mr-1"></i>' + format_bytes(it.file_size) + '</span>' : '');

    detailHtml += '<div class="small text-muted d-flex justify-content-between align-items-center flex-wrap" style="line-height: 1.5;">';
    detailHtml += '  <div class="queue-file-location d-inline-flex align-items-center flex-wrap mr-2">';
    detailHtml +=      nameAndSize;
    detailHtml += '    <span class="mr-2"><i class="fa fa-hdd-o mr-1"></i>' + pathDisplay + '</span>';
    detailHtml += '  </div>';
    detailHtml += '</div>';

    // 3행: 진행 상태별 게이지 바 (다운로드 중 / 로컬 스테이징 / 업로드 실시간 노출)
    if (it.status === 'downloading') {
      var progVal = queue_progress_percent(it.progress);
      var speedStr = it.engine_stale ? '로딩 중' : it.download_speed ? format_bytes(it.download_speed) + '/s' : (it.speed_str || '0 B/s');
      var dlBytesStr = it.downloaded_bytes ? format_bytes(it.downloaded_bytes) : '0 B';
      var totalBytesStr = it.file_size ? format_bytes(it.file_size) : '확인 중';
      var rawEngineName = it.current_engine_name || it.engine_name || '';
      var engLabel = rawEngineName ? (rawEngineName.length <= 4 ? rawEngineName.toUpperCase() : rawEngineName) : '엔진';

      detailHtml += '<div class="mt-1 p-1 px-2 rounded" style="background: rgba(0, 123, 255, 0.06); border: 1px solid rgba(0, 123, 255, 0.15);">';
      detailHtml += '  <div class="d-flex justify-content-between align-items-center mb-1 small font-weight-bold">';
      detailHtml += '    <span class="text-primary" style="font-size: 0.82rem;"><i class="fa fa-arrow-circle-o-down mr-1"></i>' + engLabel + ' 다운로드 진행: ' + progVal + '% (' + dlBytesStr + ' / ' + totalBytesStr + ')</span>';
      detailHtml += '    <span class="badge badge-info"><i class="fa fa-tachometer mr-1"></i>' + speedStr + '</span>';
      detailHtml += '  </div>';
      detailHtml += '  <div class="progress" style="height: 12px; background-color: rgba(255, 255, 255, 0.15); border-radius: 3px;">';
      detailHtml += '    <div class="progress-bar progress-bar-striped progress-bar-animated bg-primary" role="progressbar" style="width: ' + progVal + '%; font-size: 0.72rem; line-height: 12px;" aria-valuenow="' + progVal + '" aria-valuemin="0" aria-valuemax="100">' + (progVal > 8 ? progVal + '%' : '') + '</div>';
      detailHtml += '  </div>';
      detailHtml += '</div>';
    } else if (it.status === 'local_staging') {
      var progVal = queue_progress_percent(it.progress);
      var speedStr = it.speed_str || (it.download_speed ? format_bytes(it.download_speed) + '/s' : '전송 준비중');
      var dlBytesStr = it.downloaded_bytes ? format_bytes(it.downloaded_bytes) : '0 B';
      var totalBytesStr = it.file_size ? format_bytes(it.file_size) : '확인 중';
      var etaText = queue_eta_text(it.eta);
      var etaStr = etaText ? ' (ETA: ' + etaText + ')' : '';

      detailHtml += '<div class="mt-1 p-1 px-2 rounded" style="background: rgba(23, 162, 184, 0.08); border: 1px solid rgba(23, 162, 184, 0.25);">';
      detailHtml += '  <div class="d-flex justify-content-between align-items-center mb-1 small font-weight-bold">';
      detailHtml += '    <span class="text-info" style="font-size: 0.82rem;"><i class="fa fa-hdd-o mr-1"></i>로컬 스테이징 수신: ' + progVal + '% (' + dlBytesStr + ' / ' + totalBytesStr + ')' + etaStr + '</span>';
      detailHtml += '    <span class="badge badge-info"><i class="fa fa-tachometer mr-1"></i>' + speedStr + '</span>';
      detailHtml += '  </div>';
      detailHtml += '  <div class="progress" style="height: 12px; background-color: rgba(255, 255, 255, 0.15); border-radius: 3px;">';
      detailHtml += '    <div class="progress-bar progress-bar-striped progress-bar-animated bg-info" role="progressbar" style="width: ' + progVal + '%; font-size: 0.72rem; line-height: 12px;" aria-valuenow="' + progVal + '" aria-valuemin="0" aria-valuemax="100">' + (progVal > 8 ? progVal + '%' : '') + '</div>';
      detailHtml += '  </div>';
      detailHtml += '</div>';
    } else if (it.status === 'uploading') {
      var progVal = queue_progress_percent(it.progress);
      var speedStr = it.speed_str || (it.download_speed ? format_bytes(it.download_speed) + '/s' : '전송 준비중');
      var dlBytesStr = it.downloaded_bytes ? format_bytes(it.downloaded_bytes) : '0 B';
      var totalBytesStr = it.file_size ? format_bytes(it.file_size) : '확인 중';
      var etaText = queue_eta_text(it.eta);
      var etaStr = etaText ? ' (ETA: ' + etaText + ')' : '';
      var uploadTargetTitle = (it.destination_type === 'rclone_simple') ? 'Rclone 전송' : 'Google Drive 업로드';

      detailHtml += '<div class="mt-1 p-1 px-2 rounded" style="background: rgba(40, 167, 69, 0.08); border: 1px solid rgba(40, 167, 69, 0.25);">';
      detailHtml += '  <div class="d-flex justify-content-between align-items-center mb-1 small font-weight-bold">';
      detailHtml += '    <span class="text-success" style="font-size: 0.82rem;"><i class="fa fa-cloud-upload mr-1"></i>' + uploadTargetTitle + ': ' + progVal + '% (' + dlBytesStr + ' / ' + totalBytesStr + ')' + etaStr + '</span>';
      detailHtml += '    <span class="badge badge-success"><i class="fa fa-tachometer mr-1"></i>' + speedStr + '</span>';
      detailHtml += '  </div>';
      detailHtml += '  <div class="progress" style="height: 12px; background-color: rgba(255, 255, 255, 0.15); border-radius: 3px;">';
      detailHtml += '    <div class="progress-bar progress-bar-striped progress-bar-animated bg-success" role="progressbar" style="width: ' + progVal + '%; font-size: 0.72rem; line-height: 12px;" aria-valuenow="' + progVal + '" aria-valuemin="0" aria-valuemax="100">' + (progVal > 8 ? progVal + '%' : '') + '</div>';
      detailHtml += '  </div>';
      detailHtml += '</div>';
    } else if (it.status === 'pending_local_staging') {
      detailHtml += '<div class="mt-1 small text-info font-weight-bold"><i class="fa fa-clock-o mr-1"></i>로컬 스테이징 대기 중...</div>';
    } else if (it.status === 'downloaded') {
      if (it.destination_type === 'local') {
        detailHtml += '<div class="mt-1 small text-success font-weight-bold"><i class="fa fa-check-circle-o mr-1"></i>다운로드 완료 (단순 경로 이동 대기 중...)</div>';
      } else if (it.destination_type === 'rclone_simple') {
        detailHtml += '<div class="mt-1 small text-primary font-weight-bold"><i class="fa fa-clock-o mr-1"></i>다운로드 완료 (Rclone 전송 대기 중...)</div>';
      } else {
        detailHtml += '<div class="mt-1 small text-success font-weight-bold"><i class="fa fa-check-circle-o mr-1"></i>로컬 다운로드 완료 (Google Drive 업로드 대기 중...)</div>';
      }
    } else if (it.status === 'pending_upload') {
      if (it.destination_type === 'rclone_simple') {
        detailHtml += '<div class="mt-1 small text-primary font-weight-bold"><i class="fa fa-clock-o mr-1"></i>Rclone 업로드 대기 중...</div>';
      } else {
        detailHtml += '<div class="mt-1 small text-primary font-weight-bold"><i class="fa fa-clock-o mr-1"></i>Google Drive 업로드 대기 중...</div>';
      }
    } else if (it.status === 'relay_transferring') {
      detailHtml += '<div class="mt-1 small text-primary font-weight-bold"><i class="fa fa-cloud-upload mr-1"></i>원격 릴레이 전송 진행 중...</div>';
    } else if (it.status === 'engine_unverified') {
      detailHtml += '<div class="mt-1 small text-warning">DB 등록 여부를 확인하지 못했습니다. 연결 복구 전까지 읽기 전용으로 표시합니다.</div>';
    } else if (it.status === 'engine_unmanaged' || it.status === 'ad_unmanaged') {
      var engLabelName = it.current_engine_name || it.feed_name || '다운로더';
      detailHtml += '<div class="mt-1 small text-muted"><i class="fa fa-cloud mr-1"></i>' + engLabelName + ' 계정 내 보관 중 (DB 미등록 잔여 항목)</div>';
    }

    if (it.engine_stale && it.status !== 'downloading') detailHtml += '<div class="small text-warning">로딩 중</div>';
    detailHtml += errorMsg;

    str += '<tr data-queue-id="' + queue_escape_html(rowKey) + '">';
    str += '  <td class="font-weight-bold">' + it.id + '</td>';
    str += '  <td>' + statusBadge + engineInfo + '</td>';
    str += '  <td class="text-left">' + detailHtml;
    str += '    <div class="queue-item-actions d-flex flex-wrap align-items-center mt-2 pt-1">';
    str += '      <span class="small text-muted text-nowrap mr-2"><i class="fa fa-clock-o mr-1"></i>' + timeStr + '</span>';
    str += '      <div class="queue-item-buttons d-flex flex-wrap justify-content-end ml-auto">';
    if (it.status === 'engine_unverified') {
      str += '<span class="small text-muted">DB 연결 확인 후 조작 가능</span>';
    } else if (it.status === 'engine_unmanaged' || it.status === 'ad_unmanaged' || String(it.id).startsWith('EXT-') || String(it.id).startsWith('AD-')) {
      str += '    <button type="button" class="btn btn-xs btn-outline-info adopt_orphan_btn" ' +
             ' data-id="' + it.id + '"' +
             ' data-task-id="' + (it.task_id || it.id) + '"' +
             ' data-hash="' + (it.hash || '') + '"' +
             ' data-engine="' + (it.current_engine_name || '') + '"' +
             ' data-title="' + it.title + '"' +
             ' data-file-name="' + (it.file_name || it.title) + '"' +
             ' data-file-size="' + (it.file_size || 0) + '"' +
             ' data-source-path="' + (it.local_path || '') + '"' +
             ' data-raw-status="' + (it.raw_status || '') + '">작업 흡수</button>';
      str += ' <button type="button" class="btn btn-xs btn-outline-danger block_orphan_btn" data-task-id="' + String(it.task_id || it.id) + '" data-engine="' + (it.current_engine_name || '') + '" data-hash="' + (it.hash || '') + '" data-title="' + (it.title || '') + '">중단·차단</button>';
    } else {
      if (['pending', 'downloading', 'pending_local_staging', 'downloaded', 'waiting_transporter', 'pending_upload', 'move_failed'].indexOf(it.status) !== -1) {
        var singleDisabled = !last_pipeline_control || last_pipeline_control.state !== 'paused';
        str += '      <button type="button" class="btn btn-sm btn-outline-primary queue_action_btn" data-action="execute_single" data-id="' + it.id + '"' + (singleDisabled ? ' disabled' : '') + ' title="전체 작업이 완전히 중단된 상태에서 이 항목만 실행합니다.">개별 실행</button>';
      }
      str += '      <button type="button" class="btn btn-sm btn-warning text-dark font-weight-bold queue_action_btn" data-action="retry" data-id="' + it.id + '">재시도</button>';
      str += '      <button type="button" class="btn btn-sm btn-success text-white queue_action_btn" data-action="force_complete" data-id="' + it.id + '">완료</button>';
      str += '      <button type="button" class="btn btn-sm btn-secondary text-white queue_action_btn" data-action="force_fail" data-id="' + it.id + '">실패</button>';
      str += '      <button type="button" class="btn btn-sm btn-danger text-white queue_action_btn" data-action="delete" data-id="' + it.id + '">삭제</button>';
      str += '      <button type="button" class="btn btn-sm btn-outline-danger queue_action_btn" data-action="block" data-id="' + it.id + '">중단·차단</button>';
    }
    str += '      </div>';
    str += '    </div>';
    str += '  </td>';
    str += '</tr>';
    var rendered = $(str)[0], existing = currentRows[rowKey];
    if (existing) {
      existing.innerHTML = rendered.innerHTML;
      ordered.push(existing);
    } else ordered.push(rendered);
  }
  tbody.children('tr:not([data-queue-id])').remove();
  Object.keys(currentRows).forEach(function (key) { if (!renderedIds[key]) currentRows[key].remove(); });
  var newOrder = list.map(function (item) { return item.row_key || ('db:' + item.id); });
  if (JSON.stringify(oldOrder) !== JSON.stringify(newOrder)) {
    var fragment = document.createDocumentFragment();
    ordered.forEach(function (row) { fragment.appendChild(row); });
    tbody[0].appendChild(fragment);
  }
  render_queue_rows.signatures = signatures;
  if (focusedKey && focusedAction && document.activeElement !== focus) {
    var target = ordered.find(function (row) { return row.getAttribute('data-queue-id') === focusedKey; });
    if (target) {
      var button = Array.from(target.querySelectorAll('[data-action]')).find(function (el) { return el.getAttribute('data-action') === focusedAction; });
      if (button) button.focus({preventScroll: true});
    }
  }
}

$('#queue_refresh_btn').click(function (e) {
  e.preventDefault();
  request_active_queue();
});

$(document).on('click', '.queue_action_btn', function (e) {
  e.preventDefault();
  var act = $(this).data('action');
  var id = $(this).data('id');
  var targetRow = $(this).closest('tr');

  if (act === 'block' && !confirm('전송을 즉시 중단하고 이 콘텐츠를 차단하시겠습니까?\n로컬 스테이징과 원격 임시 업로드 파일도 삭제합니다.')) return;
  if (act === 'execute_single') $('.queue_action_btn[data-action="execute_single"]').prop('disabled', true);

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/item_action',
    type: 'POST',
    data: { action: act, id: id },
    dataType: 'json',
    success: function (data) {
      if (queue_page_closed) return;
      if (data.ret === 'success') {
        if (data.control) render_pipeline_control(data.control);
        notify(data.msg || '작업이 처리되었습니다.', 'info');
        request_active_queue();
      } else {
        notify(data.msg || '실패', 'warning');
        if (act === 'execute_single') refresh_pipeline_control();
      }
    },
    error: function () {
      notify('작업 요청 결과를 확인하지 못했습니다. 상태를 다시 확인하세요.', 'warning');
      if (act === 'execute_single') refresh_pipeline_control();
    }
  });
});

// 미등록 고아 작업 흡수 모달 열기 (1순위 엔진 일치 프로필만 필터링)
$(document).on('click', '.adopt_orphan_btn', function (e) {
  e.preventDefault();
  var btn = $(this);
  var targetEngine = (btn.data('engine') || '').trim();
  var title = btn.data('title');
  var fsize = parseInt(btn.data('file-size')) || 0;

  $('#modal_adopt_task_id').val(btn.data('task-id'));
  $('#modal_adopt_hash').val(btn.data('hash'));
  $('#modal_adopt_engine').val(targetEngine);
  $('#modal_adopt_file_name').val(btn.data('file-name'));
  $('#modal_adopt_file_size').val(fsize);
  $('#modal_adopt_source_path').val(btn.data('source-path'));
  $('#modal_adopt_raw_status').val(btn.data('raw-status'));

  $('#modal_adopt_title').text(title);
  $('#modal_adopt_engine_display').text(targetEngine);
  $('#modal_adopt_size_display').text(format_bytes(fsize));

  var pSelect = $('#modal_adopt_profile_select');
  pSelect.empty();

  var matchingCount = 0;
  if (cached_download_profiles && cached_download_profiles.length > 0) {
    for (var i = 0; i < cached_download_profiles.length; i++) {
      var p = cached_download_profiles[i];
      // 1순위 엔진이 대상 엔진과 일치하는 프로필만 선택지로 추가
      if (p.priority_chain && p.priority_chain.length > 0 && p.priority_chain[0].toLowerCase() === targetEngine.toLowerCase()) {
        pSelect.append('<option value="' + p.name + '">' + p.name + ' (목적지: ' + (p.destination ? p.destination.type : 'local') + ')</option>');
        matchingCount++;
      }
    }
  }

  if (matchingCount === 0) {
    pSelect.append('<option value="">-- 1순위가 [' + targetEngine + ']인 프로필 없음 --</option>');
    $('#btn_confirm_adopt_orphan').prop('disabled', true);
  } else {
    $('#btn_confirm_adopt_orphan').prop('disabled', false);
  }

  $('#adopt_orphan_modal').modal('show');
});

// 고아 작업 큐 흡수 실행 확인
$(document).on('click', '#btn_confirm_adopt_orphan', function (e) {
  e.preventDefault();
  var profileName = $('#modal_adopt_profile_select').val();
  if (!profileName) {
    notify('적용할 프로필을 선택하세요.', 'warning');
    return;
  }

  notify('작업 큐 흡수 요청 중...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/adopt_orphan',
    type: 'POST',
    data: {
      task_id: $('#modal_adopt_task_id').val(),
      hash: $('#modal_adopt_hash').val(),
      engine_name: $('#modal_adopt_engine').val(),
      title: $('#modal_adopt_title').text(),
      file_name: $('#modal_adopt_file_name').val(),
      file_size: $('#modal_adopt_file_size').val(),
      source_path: $('#modal_adopt_source_path').val(),
      raw_status: $('#modal_adopt_raw_status').val(),
      profile_name: profileName
    },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify(data.msg || '작업이 큐에 성공적으로 등록되었습니다.', 'success');
        $('#adopt_orphan_modal').modal('hide');
        request_active_queue();
      } else {
        notify(data.msg || '작업 흡수 실패', 'warning');
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
    tbody.html('<tr><td colspan="4" class="py-4 text-muted">검색 조건에 일치하는 다운로드 이력이 없습니다.</td></tr>');
    return;
  }

  var str = '';
  for (var i = 0; i < list.length; i++) {
    var it = list[i];
    var statusBadge = '';
    if (it.status === 'completed') statusBadge = '<span class="badge badge-success">최종 완료</span>';
    else if (it.status === 'downloading') statusBadge = '<span class="badge badge-primary">다운로드</span>';
    else if (it.status === 'pending') statusBadge = '<span class="badge badge-warning">대기 (Pending)</span>';
    else if (it.status === 'cancelled') statusBadge = '<span class="badge badge-secondary">삭제/취소</span>';
    else if (it.status === 'pending_local_staging' || it.status === 'local_staging') statusBadge = '<span class="badge badge-info">로컬 스테이징</span>';
    else if (it.status === 'pending_relay') statusBadge = '<span class="badge badge-warning">원격 릴레이 대기</span>';
    else if (it.status === 'relay_transferring') statusBadge = '<span class="badge badge-primary">원격 릴레이 전송 중</span>';
    else if (it.status === 'downloaded') statusBadge = '<span class="badge badge-success">다운로드 완료</span>';
    else if (it.status === 'pending_upload' || it.status === 'uploading') statusBadge = '<span class="badge badge-primary">업로드</span>';
    else if (it.status === 'waiting_transporter') statusBadge = '<span class="badge badge-warning">이송 설정 복구 대기</span>';
    else if (it.status === 'move_failed') statusBadge = it.successful ? '<span class="badge badge-success">소스 없음 · 성공 종료 (move_failed)</span>' : '<span class="badge badge-warning">이동 실패</span>';
    else if (it.status === 'failed') statusBadge = '<span class="badge badge-danger">실패</span>';
    else if (it.status === 'blocked') statusBadge = '<span class="badge badge-danger">차단</span>';
    else statusBadge = '<span class="badge badge-secondary">' + it.status + '</span>';

    var engineInfo = it.current_engine_name ? '<br><small class="text-muted">엔진: ' + it.current_engine_name + '</small>' : '';
    var errorMsg = it.error_message ? '<div class="mt-1 small text-danger font-weight-bold"><i class="fa fa-exclamation-triangle mr-1"></i>' + it.error_message + '</div>' : '';
    var timeStr = it.completed_time || it.updated_time || it.created_time || '';
    var pathDisplay = it.local_path ? '<code>' + it.local_path + '</code>' : '<span class="text-muted">-</span>';

    var detailHtml = '';

    // 1행: 피드명 + 제목
    detailHtml += '<div class="mb-1" style="word-break: break-all;">';
    detailHtml += '  <span class="badge badge-dark mr-1">' + (it.feed_name || 'Feed') + '</span>';
    detailHtml += '  <strong style="font-size: 0.95rem;">' + it.title + '</strong>';
    detailHtml += '</div>';

    // 2행: 파일명 (용량) + 저장 경로 + [우측: 완료시각] (한 줄 통합 배치)
    var fsizeText = it.file_size ? ' (' + format_bytes(it.file_size) + ')' : '';
    var nameAndSize = it.file_name
      ? '<span class="font-weight-bold text-info mr-2"><i class="fa fa-folder-open-o mr-1"></i>' + it.file_name + fsizeText + '</span>'
      : (fsizeText ? '<span class="badge badge-secondary mr-2"><i class="fa fa-database mr-1"></i>' + format_bytes(it.file_size) + '</span>' : '');

    detailHtml += '<div class="small text-muted d-flex justify-content-between align-items-center flex-wrap" style="line-height: 1.5;">';
    detailHtml += '  <div class="d-inline-flex align-items-center flex-wrap mr-2">';
    detailHtml +=      nameAndSize;
    detailHtml += '    <span class="mr-2"><i class="fa fa-hdd-o mr-1"></i>' + pathDisplay + '</span>';
    detailHtml += '  </div>';
    detailHtml += '  <span class="text-muted ml-auto"><i class="fa fa-clock-o mr-1"></i>' + timeStr + '</span>';
    detailHtml += '</div>';

    detailHtml += errorMsg;

    str += '<tr>';
    str += '  <td class="font-weight-bold">' + it.id + '</td>';
    str += '  <td>' + statusBadge + engineInfo + '</td>';
    str += '  <td class="text-left">' + detailHtml + '</td>';
    str += '  <td>';
    str += '    <div class="btn-group btn-group-sm">';
    if (it.status !== 'blocked') {
    str += '      <button type="button" class="btn btn-warning text-dark font-weight-bold btn_list_action" data-action="retry" data-id="' + it.id + '">재시도</button>';
    str += '      <button type="button" class="btn btn-success text-white btn_list_action" data-action="force_complete" data-id="' + it.id + '">완료</button>';
    str += '      <button type="button" class="btn btn-secondary text-white btn_list_action" data-action="force_fail" data-id="' + it.id + '">실패</button>';
    }
    str += '      <button type="button" class="btn btn-danger text-white btn_list_action" data-action="delete" data-id="' + it.id + '">삭제</button>';
    if (it.status !== 'blocked') str += '      <button type="button" class="btn btn-outline-danger btn_list_action" data-action="block" data-id="' + it.id + '">중단·차단</button>';
    else str += '      <button type="button" class="btn btn-outline-danger blacklist_open_btn">차단 관리</button>';
    str += '    </div>';
    str += '  </td>';
    str += '</tr>';
  }
  tbody.html(str);
}

$(document).on('change', '#status_filter', function () {
  if ($('#download_list_tbody').length === 0) return;
  localStorage.setItem(sub + '_status_filter', $(this).val());
  localStorage.setItem(sub + '_current_page', '1');
  window.globalRequestSearch('1', false);
});

$(document).on('change', '#page_size', function () {
  if ($('#download_list_tbody').length === 0) return;
  localStorage.setItem(sub + '_page_size', $(this).val());
  localStorage.setItem(sub + '_current_page', '1');
  window.globalRequestSearch('1', false);
});

$('#reset_btn').click(function (e) {
  e.preventDefault();
  if ($('#download_list_tbody').length === 0) return;

  $('#status_filter').val('all');
  $('#search_word').val('');
  $('#page_size').val('25');

  localStorage.setItem(sub + '_search_word', '');
  localStorage.setItem(sub + '_status_filter', 'all');
  localStorage.setItem(sub + '_page_size', '25');
  localStorage.setItem(sub + '_current_page', '1');

  window.globalRequestSearch('1', false);
});

$('#search').click(function (e) {
  e.preventDefault();
  if ($('#download_list_tbody').length === 0) return;
  var word = ($('#search_word').val() || '').trim();
  localStorage.setItem(sub + '_search_word', word);
  localStorage.setItem(sub + '_current_page', '1');
  window.globalRequestSearch('1', false);
});

$('#search_word').keydown(function (e) {
  if (e.which === 13) {
    e.preventDefault();
    if ($('#download_list_tbody').length === 0) return;
    var word = ($('#search_word').val() || '').trim();
    localStorage.setItem(sub + '_search_word', word);
    localStorage.setItem(sub + '_current_page', '1');
    window.globalRequestSearch('1', false);
  }
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
  var targetRow = $(this).closest('tr');

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

  if (act === 'block' && !confirm('전송을 즉시 중단하고 이 콘텐츠를 차단하시겠습니까?\n로컬 스테이징과 원격 임시 업로드 파일도 삭제합니다.')) return;
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
        if (act === 'delete' && targetRow.length > 0) {
          targetRow.fadeOut(200, function () {
            $(this).remove();
            window.globalRequestSearch(null, true);
          });
        } else {
          window.globalRequestSearch(null, true);
        }
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

$('#download_use_local_staging').change(function () { use_collapse('download_use_local_staging'); });
$('#download_use_gdrive_pool').change(function () { use_collapse('download_use_gdrive_pool'); });
$('#download_gdrive_use_impersonate').change(function () { use_collapse('download_gdrive_use_impersonate'); });

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
    if (item.engine_type === 'alldebrid') {
      var webdavHours = item.webdav_failure_timeout_hours === undefined ? 24 : Number(item.webdav_failure_timeout_hours);
      timeoutStr += '<br>WebDAV 실패 유예: ' + (webdavHours === 0 ? '무제한' : webdavHours + '시간');
    }

    var connDetail = '';
    if (item.engine_type === 'qbittorrent') {
      connDetail = 'URL: ' + (item.url || '-') + ' | 저장: ' + (item.save_path || '-');
    } else if (item.engine_type === 'cd2') {
      connDetail = 'CD2: ' + (item.cd2_addr || '127.0.0.1') + ':' + (item.cd2_port || 19798) + ' | 마운트: ' + (item.cd2_mount_path || '-');
    } else {
      // 커스텀 엔진(AllDebrid, PikPak 등): 식별용 주요 설정값 동적 표시
      var keys = Object.keys(item).filter(function (k) {
        return ['name', 'engine_type', 'enabled', 'stalled_timeout_hours', 'missing_timeout_hours', 'webdav_failure_timeout_hours'].indexOf(k) === -1 && !/key|token|secret|password|credential/i.test(k);
      });
      connDetail = keys.slice(0, 3).map(function (k) {
        var val = item[k];
        if (typeof val === 'string' && val.length > 20) val = val.substring(0, 20) + '...';
        return k + ': ' + val;
      }).join(' | ') || '-';
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
    if ((!current_values || current_values[f.name] === undefined) && f.name === 'use_impersonate') {
      val = $('#download_gdrive_use_impersonate').is(':checked');
    }
    var ph = f.placeholder || '';
    var descHtml = f.desc ? '<small class="form-text text-muted">' + f.desc + '</small>' : '';

    html += '<div class="form-group row mb-2">';
    html += '  <label class="col-sm-3 col-form-label text-right font-weight-bold">' + f.label + '</label>';
    html += '  <div class="col-sm-9">';

    if (f.type === 'checkbox') {
      var isChk = (val === true || val === 'true' || val === 'On' || val === 'on') ? 'checked' : '';
      html += '    <input type="checkbox" id="dl_field_' + f.name + '" class="mt-2" ' + isChk + '>';
    } else if (f.type === 'number') {
      html += '    <input type="number" id="dl_field_' + f.name + '" class="form-control form-control-sm" value="' + val + '" placeholder="' + ph + '"' + (f.min !== undefined ? ' min="' + f.min + '"' : '') + (f.max !== undefined ? ' max="' + f.max + '"' : '') + (f.step !== undefined ? ' step="' + f.step + '"' : '') + '>';
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

  try {
    container.find('input[data-toggle="toggle"]').bootstrapToggle();
  } catch (err) {}
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
      else if (f.type === 'number') cfg[f.name] = f.name === 'webdav_failure_timeout_hours' ? el.val() : (parseInt(el.val()) || 0);
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
  var button = $(this);
  if (button.prop('disabled')) return;
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
      else if (f.type === 'number') cfg[f.name] = f.name === 'webdav_failure_timeout_hours' ? el.val() : (parseInt(el.val()) || 0);
      else cfg[f.name] = el.val().trim();
    }
  }

  button.prop('disabled', true);
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/save_downloader',
    type: 'POST',
    data: { downloader_json: JSON.stringify(cfg) },
    dataType: 'json',
    success: function (data) {
      var result = data && typeof data.ret === 'string' ? data.ret : '';
      if ((result !== 'success' && result !== 'success_update') || !Array.isArray(data.downloaders)) {
        // 검증된 고정 문구만 표시하여 서버 응답에 섞인 인증 값의 노출을 막는다.
        var errors = {
          empty_name: '식별명을 입력하세요.',
          'WebDAV 실패 유예는 0~8760 사이의 정수 시간이어야 합니다.': 'WebDAV 실패 유예는 0~8760 사이의 정수 시간이어야 합니다.'
        };
        var message = Object.prototype.hasOwnProperty.call(errors, result) ? errors[result] : '다운로더 설정을 저장하지 못했습니다. 입력값을 확인하고 다시 시도하세요.';
        notify(message, 'warning');
        return;
      }
      notify('다운로더 설정이 저장되었습니다.', 'success');
      $('#downloader_modal').modal('hide');
      current_downloaders = data.downloaders || [];
      render_downloaders(current_downloaders);
    },
    error: function () {
      notify('다운로더 설정 저장 중 통신 오류가 발생했습니다. 입력값은 유지됩니다.', 'danger');
    },
    complete: function () {
      button.prop('disabled', false);
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
  var SKELETON = '# -*- coding: utf-8 -*-\nfrom feeder.util_download import BaseDownloadEngine\n\nclass CustomEngine(BaseDownloadEngine):\n    ENGINE_ID = "custom"\n    ENGINE_NAME = "커스텀 다운로더"\n    CONFIG_SCHEMA = []\n    def add_magnet(self, link, title=None, upload_path=None):\n        return True, "task_1", ""\n    def get_status(self, task_ids=None):\n        return [], ""\n    def delete_task(self, task_id):\n        return True\n';
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

function normalize_profile_transfer_config(values) {
  var result = Object.assign({}, values || {});
  var modes = {direct: [false, false], rotation: [true, false], impersonate: [true, true]};
  var mode = modes[result.gdrive_mode];
  if (mode) {
    if (result.use_gdrive_pool == null) result.use_gdrive_pool = mode[0];
    if (result.use_impersonate == null) result.use_impersonate = mode[1];
    delete result.gdrive_mode;
  }
  return result;
}

function profile_field_value(field, values) {
  values = normalize_profile_transfer_config(values);
  var value = values[field.name];
  if (field.allow_inherit) {
    return value === undefined || value === null || value === '' ? '' : (/^(true|on|1)$/i.test(String(value)) ? 'true' : 'false');
  }
  return value === undefined || value === null ? (field.default !== undefined ? field.default : '') : value;
}

function profile_radio_html(field, id, value, attributes) {
  var html = '<div id="' + id + '" class="d-flex flex-wrap">';
  (field.options || []).forEach(function (option, index) {
    html += '<label class="form-check form-check-inline mb-1" for="' + id + '_' + index + '">' +
      '<input type="radio" class="form-check-input ' + (attributes ? 'profile-engine-dest-field' : '') + '"' +
      ' id="' + id + '_' + index + '" name="' + id + '" value="' + option.value + '"' +
      (String(value) === option.value ? ' checked' : '') + (field.allow_inherit ? ' data-inherit-boolean="true"' : '') + (attributes || '') + '>' +
      '<span class="form-check-label">' + option.label + '</span></label>';
  });
  return html + '</div>';
}

function profile_boolean_options(value) {
  return [{value: '', label: '상위 설정 사용'}, {value: 'true', label: '사용'}, {value: 'false', label: '사용 안 함'}].map(function (option) {
    return '<option value="' + option.value + '"' + (String(value) === option.value ? ' selected' : '') + '>' + option.label + '</option>';
  }).join('');
}

function profile_pool_enabled(values, inherited) {
  if (values && values.type === 'gdrive_rotation') return true;
  var value = profile_field_value({name: 'use_gdrive_pool', allow_inherit: true}, values);
  return value === '' ? inherited : value === 'true';
}

function profile_inherited_pool(engineName) {
  var enabled = $('#download_use_gdrive_pool').is(':checked');
  var engine = current_downloaders.find(function (d) {
    return String(d.name).toLowerCase() === String(engineName || '').toLowerCase();
  });
  enabled = profile_pool_enabled(engine, enabled);
  return profile_pool_enabled(modal_profile_settings, enabled);
}

function profile_destination_pool(engineName) {
  if ($('#profile_dest_type').val() === 'gdrive_rotation') return true;
  var value = $('#dest_field_use_gdrive_pool input:checked').val();
  return value === 'true' || (value !== 'false' && profile_inherited_pool(engineName));
}

function toggle_profile_impersonation() {
  var engines = modal_profile_chain.length ? modal_profile_chain : [''];
  var enabled = engines.some(function (engineName) { return profile_destination_pool(engineName); });
  $('#profile_dynamic_dest_fields .dest-field-row-use_impersonate').toggle(
    $('#profile_dest_type').val() === 'gdrive_rotation' || enabled);
}

$(document).on('change', '#dest_field_use_gdrive_pool input', function () {
  toggle_profile_impersonation();
  render_engine_destinations();
});

function render_dynamic_dest_fields(transporter_id, current_values) {
  var container = $('#profile_dynamic_dest_fields');
  container.empty();

  var schemaObj = available_transporter_schemas.find(function (s) { return s.transporter_id === transporter_id; });
  if (!schemaObj || !schemaObj.config_schema || schemaObj.config_schema.length === 0) {
    container.html('<div class="text-muted small p-2 text-center">해당 목적지에 별도 세부 설정 항목이 없습니다.</div>');
    return;
  }

  var html = '<hr><h6 class="text-info text-center font-weight-bold mb-3"><i class="fa fa-folder-open mr-1"></i>' + schemaObj.transporter_name + ' 세부 설정</h6>';
  var fields = schemaObj.config_schema;

  for (var i = 0; i < fields.length; i++) {
    var f = fields[i];
    var val = profile_field_value(f, current_values);
    var ph = f.placeholder || '';
    var descHtml = f.desc ? '<small class="form-text text-muted">' + f.desc + '</small>' : '';

    html += '<div class="form-group row mb-2 dest-field-row-' + f.name + '">';
    html += '  <label class="col-sm-3 col-form-label text-right font-weight-bold">' + (f.depends_on ? '↳ ' : '') + f.label + '</label>';
    html += '  <div class="col-sm-9' + (f.depends_on ? ' border-left pl-3' : '') + '">';

    if (f.type === 'radio') {
      html += profile_radio_html(f, 'dest_field_' + f.name, val);
    } else if (f.allow_inherit && f.type === 'checkbox') {
      html += '<select id="dest_field_' + f.name + '" class="form-control form-control-sm">' + profile_boolean_options(val) + '</select>';
    } else if (f.type === 'checkbox') {
      var isChk = (val === true || val === 'true' || val === 'On' || val === 'on') ? 'checked' : '';
      html += '    <div><input type="checkbox" id="dest_field_' + f.name + '" data-toggle="toggle" data-on="On" data-off="Off" data-size="small" ' + isChk + '></div>';
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
  toggle_profile_impersonation();

  try {
    container.find('input[data-toggle="toggle"]').bootstrapToggle();
  } catch (err) {}

  function toggleStagingPathField() {
    var stagingChk = $('#dest_field_use_local_staging');
    if (stagingChk.length > 0) {
      var stagingValue = stagingChk.is('select') ? stagingChk.val() : stagingChk.find('input:checked').val();
      if (stagingChk.is(':checkbox') ? stagingChk.is(':checked') : stagingValue !== 'false') {
        $('.dest-field-row-staging_path').show();
      } else {
        $('.dest-field-row-staging_path').hide();
      }
    }
  }

  toggleStagingPathField();
  $('#dest_field_use_local_staging').on('change', function () {
    toggleStagingPathField();
  });
}

$('#profile_dest_type').change(function () {
  render_dynamic_dest_fields($(this).val(), null);
  var pName = $('#profile_name').val().trim() || 'default';
  var upField = $('#dest_field_upload_path');
  var stagingField = $('#dest_field_staging_path');
  var compField = $('#dest_field_complete_path');
  if (stagingField.length && (!stagingField.val() || stagingField.val() === 'default')) {
    stagingField.val(pName);
  }
  if (upField.length && (!upField.val() || upField.val() === 'default' || upField.val().startsWith('incoming/'))) {
    upField.val('incoming/' + pName);
  }
  if (compField.length && (!compField.val() || compField.val() === 'default' || compField.val().startsWith('uploads/'))) {
    compField.val('uploads/' + pName);
  }
});

$(document).on('input change', '#profile_name', function () {
  if ($('#profile_mode').val() !== 'add') return;
  var pName = $(this).val().trim() || 'default';
  var upField = $('#dest_field_upload_path');
  var stagingField = $('#dest_field_staging_path');
  var compField = $('#dest_field_complete_path');
  if (stagingField.length && (!stagingField.val() || stagingField.val() === 'default')) {
    stagingField.val(pName);
  }
  if (upField.length && (!upField.val() || upField.val() === 'default' || upField.val().startsWith('incoming/'))) {
    upField.val('incoming/' + pName);
  }
  if (compField.length && (!compField.val() || compField.val() === 'default' || compField.val().startsWith('uploads/'))) {
    compField.val('uploads/' + pName);
  }
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
    if (dest.type === 'rclone_simple' || dest.type === 'remote_worker') {
      if (dest.type === 'remote_worker') destInfo = '<span class="badge badge-primary">원격 워커</span>';
    } else if (dest.type === 'gdrive_rotation') {
      destInfo = '<span class="badge badge-success">GDrive 계정 풀</span>';
    } else if (dest.type && dest.type.indexOf('relay') !== -1) {
      destInfo = '<span class="badge badge-primary">원격 릴레이</span>';
    } else {
      // 커스텀 트랜스포터 기본 표기
      if (dest.type) destInfo = '<span class="badge badge-info">' + dest.type + '</span>';
    }
    if (dest.complete_path) destInfo += '<br><small class="text-muted">' + dest.complete_path + '</small>';

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
  render_engine_destinations();
}

function render_engine_destinations() {
  toggle_profile_impersonation();
  var container = $('#profile_engine_destinations');
  if (!container.length) return;
  container.empty();
  if (!modal_profile_chain || modal_profile_chain.length === 0) {
    container.html('<span class="text-muted small">우선순위 엔진을 먼저 추가하세요.</span>');
    return;
  }

  var html = '<div class="table-responsive border rounded"><table class="table table-sm mb-0">' +
    '<thead class="thead-light"><tr><th style="min-width: 80px; white-space: nowrap;">엔진</th><th>목적지 유형 및 세부 설정</th></tr></thead><tbody>';
  for (var i = 0; i < modal_profile_chain.length; i++) {
    var engineName = modal_profile_chain[i];
    var selectedConfig = modal_engine_destinations[engineName] || {};
    if (typeof selectedConfig === 'string') selectedConfig = { type: selectedConfig };
    selectedConfig = normalize_profile_transfer_config(selectedConfig);
    if (modal_engine_destinations[engineName]) modal_engine_destinations[engineName] = selectedConfig;
    var selected = selectedConfig.type || '';
    var fieldPrefix = 'dest_override_' + String(engineName).replace(/[^a-zA-Z0-9_-]/g, '_');
    html += '<tr><td class="font-weight-bold" style="min-width: 80px; white-space: nowrap;">' + engineName + '</td><td>' +
      '<select class="form-control form-control-sm profile-engine-dest-select" data-engine="' + engineName + '">';
    html += '<option value="">기본 목적지 사용</option>';
    for (var j = 0; j < available_transporter_schemas.length; j++) {
      var schema = available_transporter_schemas[j];
      html += '<option value="' + schema.transporter_id + '"' +
        (schema.transporter_id === selected ? ' selected' : '') + '>' +
        schema.transporter_name + ' (' + schema.transporter_id + ')</option>';
    }
    html += '</select>';
    if (selected) {
        var selectedSchema = available_transporter_schemas.find(function (schema) {
          return schema.transporter_id === selected;
        });
        if (selectedSchema && selectedSchema.config_schema) {
          html += '<div class="mt-2 pl-2 border-left">';
          for (var k = 0; k < selectedSchema.config_schema.length; k++) {
            var f = selectedSchema.config_schema[k];
            var value = profile_field_value(f, selectedConfig);
            var inputId = fieldPrefix + '_' + f.name;
            var hidden = f.depends_on === 'use_gdrive_pool' && !profile_pool_enabled(selectedConfig, profile_destination_pool(engineName));
            html += '<div class="form-group mb-1' + (f.depends_on ? ' pl-3 border-left' : '') + '"' + (hidden ? ' style="display:none"' : '') + '>';
            html += '<label class="small mb-1">' + (f.depends_on ? '↳ ' : '') + f.label + '</label>';
            if (f.type === 'radio') {
              html += profile_radio_html(f, inputId, value, ' data-engine="' + engineName + '" data-field="' + f.name + '"');
            } else if (f.allow_inherit && f.type === 'checkbox') {
              html += '<select class="form-control form-control-sm profile-engine-dest-field" id="' + inputId + '" data-engine="' + engineName + '" data-field="' + f.name + '" data-inherit-boolean="true">' + profile_boolean_options(value) + '</select>';
            } else if (f.type === 'checkbox') {
              var checked = (value === true || value === 'true' || value === 'On' || value === 'on') ? ' checked' : '';
              html += '<div><input type="checkbox" class="profile-engine-dest-field" id="' + inputId + '" data-engine="' + engineName + '" data-field="' + f.name + '"' + checked + '></div>';
            } else {
              html += '<input type="' + (f.type === 'number' ? 'number' : 'text') + '" class="form-control form-control-sm profile-engine-dest-field" id="' + inputId + '" data-engine="' + engineName + '" data-field="' + f.name + '" value="' + value + '" placeholder="' + (f.placeholder || '') + '">';
            }
            if (f.desc) html += '<small class="form-text text-muted">' + f.desc + '</small>';
            html += '</div>';
          }
          html += '</div>';
        }
    }
    html += '</td></tr>';
  }
  html += '</tbody></table></div>';
  container.html(html);
}

$(document).on('change', '.profile-engine-dest-select', function () {
  var engineName = $(this).data('engine');
  var value = $(this).val();
  if (value) modal_engine_destinations[engineName] = { type: value };
  else delete modal_engine_destinations[engineName];
  render_engine_destinations();
});

$(document).on('change input', '.profile-engine-dest-field', function () {
  var engineName = $(this).data('engine');
  var fieldName = $(this).data('field');
  if (!modal_engine_destinations[engineName]) return;
  var value = $(this).is(':checkbox') ? $(this).is(':checked') : $(this).val();
  if ($(this).is('[data-inherit-boolean]')) value = value === '' ? undefined : value === 'true';
  if ($(this).is(':radio') && value === '') value = undefined;
  if (value === undefined) {
    delete modal_engine_destinations[engineName][fieldName];
  } else {
    modal_engine_destinations[engineName][fieldName] = value;
  }
  if (fieldName === 'use_gdrive_pool') render_engine_destinations();
});

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
  $('#profile_sync_hours').val('');
  $('#profile_append_hash_on_conflict').prop('checked', true);
  modal_profile_settings = {};

  var default_trans = available_transporter_schemas.length > 0 ? available_transporter_schemas[0].transporter_id : '';
  update_profile_dest_type_dropdown(default_trans);
  render_dynamic_dest_fields(default_trans, null);

  var upField = $('#dest_field_upload_path');
  var stagingField = $('#dest_field_staging_path');
  var compField = $('#dest_field_complete_path');
  if (stagingField.length) stagingField.val('default');
  if (upField.length) upField.val('incoming/default');
  if (compField.length) compField.val('uploads/default');

  modal_profile_feeds = ['*'];
  modal_profile_chain = [];
  modal_engine_destinations = {};

  update_profile_modal_dropdowns();
  render_selected_feeds();
  render_chain_table();
  $('#profile_modal').modal('show');
});

$(document).on('click', '.edit_profile_btn', function (e) {
  e.preventDefault();
  var idx = $(this).data('index');
  var p = current_profiles[idx];
  modal_profile_settings = p;

  $('#profile_modal_title').text('다운로드 프로필 수정: ' + p.name);
  $('#profile_mode').val('edit');
  $('#profile_name').val(p.name).prop('readonly', true);
  var rawHours = (p.sync_hours !== undefined && p.sync_hours !== null) ? p.sync_hours : (p.sync_days ? p.sync_days * 24 : '');
  $('#profile_sync_hours').val(rawHours);
  $('#profile_append_hash_on_conflict').prop('checked', p.append_hash_on_conflict !== false);

  var dest = p.destination || {};
  update_profile_dest_type_dropdown(dest.type || '');
  render_dynamic_dest_fields(dest.type || '', dest);

  modal_profile_feeds = (p.feeds && p.feeds.length > 0) ? JSON.parse(JSON.stringify(p.feeds)) : ['*'];
  modal_profile_chain = (p.priority_chain && p.priority_chain.length > 0) ? JSON.parse(JSON.stringify(p.priority_chain)) : [];
  modal_engine_destinations = {};
  var savedDestinations = p.destination_by_engine || {};
  Object.keys(savedDestinations).forEach(function (engineName) {
    if (savedDestinations[engineName]) {
      modal_engine_destinations[engineName] = typeof savedDestinations[engineName] === 'string'
        ? { type: savedDestinations[engineName] }
        : JSON.parse(JSON.stringify(savedDestinations[engineName]));
    }
  });

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
      if (f.type === 'radio') {
        var selected = el.find('input:checked').val();
        if (selected) destObj[f.name] = f.allow_inherit ? selected === 'true' : selected;
      } else if (f.allow_inherit && f.type === 'checkbox') {
        if (el.val() !== '') destObj[f.name] = el.val() === 'true';
      } else if (f.type === 'checkbox') destObj[f.name] = el.is(':checked');
      else if (f.type === 'number') destObj[f.name] = parseInt(el.val()) || 0;
      else destObj[f.name] = el.val().trim();
    }
  }

  var profile_obj = {
    name: name,
    sync_hours: $('#profile_sync_hours').val().trim() !== '' ? parseInt($('#profile_sync_hours').val().trim(), 10) : '',
    feeds: modal_profile_feeds,
    priority_chain: modal_profile_chain,
    append_hash_on_conflict: $('#profile_append_hash_on_conflict').is(':checked'),
    destination: destObj,
    destination_by_engine: Object.keys(modal_engine_destinations).reduce(function (result, engineName) {
      result[engineName] = modal_engine_destinations[engineName];
      return result;
    }, {})
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
  if (stats) {
    $('#gdrive_stat_total_24h').text(format_bytes(stats.total_usage_24h || 0));
    $('#gdrive_stat_account_total').text(format_bytes(stats.account_total_usage || 0));
  }
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

$(document).on('click', '#gdrive_account_add_btn', function (e) {
  e.preventDefault();
  $('#modal_acc_index').val('-1');
  $('#gdrive_account_modal_title').text('구글 드라이브 계정 추가');
  $('#modal_acc_username').val('');
  $('#modal_acc_mydrive_id').val('');
  $('#modal_acc_remote_name').val('');
  $('#gdrive_account_modal').modal('show');
});

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

  var newAcc = {
    username: uname,
    mydrive_rclone_id: mydrive_id
  };
  if (rname) newAcc.remote_name = rname;

  if (idx >= 0 && idx < current_accounts.length) {
    current_accounts[idx] = newAcc;
  } else {
    var exists = current_accounts.some(function (acc) {
      return acc.username === uname;
    });
    if (exists) {
      notify('이미 등록된 계정 식별자입니다.', 'warning');
      return;
    }
    current_accounts.push(newAcc);
  }

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/save_accounts',
    type: 'POST',
    data: { accounts_json: JSON.stringify(current_accounts) },
    dataType: 'json',
    success: function (data) {
      notify(idx >= 0 ? '계정 정보가 수정되었습니다.' : '신규 계정이 등록되었습니다.', 'success');
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

// 차단 이력은 다운로드 행과 독립적으로 조회하고 관리한다.
var blacklistPage = 1;
$(document).on('click', '.block_orphan_btn', function() {
  if (!confirm('이 원격 작업을 즉시 중단하고 콘텐츠를 차단하시겠습니까?')) return;
  var button = $(this);
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/block_orphan', type: 'POST', dataType: 'json',
    data: {task_id: button.data('task-id'), engine_name: button.data('engine'), hash: button.data('hash'), title: button.data('title')},
    success: function(data) {
      notify(data.msg || '처리 완료', data.ret === 'success' ? 'info' : 'warning');
      if (data.ret === 'success') request_active_queue();
    }
  });
});
function request_blacklist() {
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/blacklist_list',
    type: 'POST', data: {page: blacklistPage}, dataType: 'json', global: false,
    success: function(data) {
      if (data.ret !== 'success') { notify(data.msg || '차단 목록 조회 실패', 'warning'); return; }
      var rows = $('#download_blacklist_rows').empty();
      if (!data.list.length) rows.text('차단된 콘텐츠가 없습니다.');
      data.list.forEach(function(it) {
        var row = $('<div class="border-bottom py-2"></div>');
        $('<div class="font-weight-bold"></div>').text(it.title || '(제목 없음)').appendTo(row);
        $('<div class="small text-muted"></div>').text(it.pending ? (it.error ? '정리 실패: ' + it.error : '임시 파일 정리 중/대기') : '임시 파일 정리 완료').appendTo(row);
        $('<button type="button" class="btn btn-sm btn-outline-danger mt-1 blacklist_action_btn"></button>')
          .text(it.pending ? '정리 재시도' : '차단 해제').attr('data-key', it.key)
          .attr('data-action', it.pending ? 'cleanup' : 'unblock').appendTo(row);
        rows.append(row);
      });
      $('#blacklist_page').text(blacklistPage + ' 페이지 · 전체 ' + data.total + '개');
      $('#blacklist_prev').prop('disabled', blacklistPage <= 1);
      $('#blacklist_next').prop('disabled', blacklistPage * 50 >= data.total);
    }
  });
}
$(document).on('click', '.blacklist_open_btn', function() {
  blacklistPage = 1; request_blacklist(); $('#download_blacklist_modal').modal('show');
});
$('#blacklist_prev').click(function() { if (blacklistPage > 1) { blacklistPage--; request_blacklist(); } });
$('#blacklist_next').click(function() { blacklistPage++; request_blacklist(); });
$(document).on('click', '.blacklist_action_btn', function() {
  var button = $(this);
  if (button.data('action') === 'unblock' && !confirm('이 콘텐츠의 차단을 해제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/blacklist_action',
    type: 'POST', dataType: 'json', data: {key: button.data('key'), action: button.data('action')},
    success: function(data) {
      notify(data.msg || '처리 완료', data.ret === 'success' ? 'info' : 'warning');
      request_blacklist();
      if ($('#download_list_tbody').length) window.globalRequestSearch(null, true);
    }
  });
});
$('#download_blacklist_modal').on('shown.bs.modal', function() {
  window.feederBlacklistTimer = setInterval(request_blacklist, 4000);
}).on('hidden.bs.modal', function() { clearInterval(window.feederBlacklistTimer); });
