var current_page = 1;
var cached_download_list = [];
var cached_download_profiles = [];

$(document).ready(function(){
  request_download_list(1);
  load_retry_profiles();
});

function load_retry_profiles() {
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/load_profiles',
    type: "POST",
    dataType: "json",
    success: function(data) {
      cached_download_profiles = data.profiles || [];
    }
  });
}

function request_download_list(page) {
  current_page = page || 1;
  var formData = {
    page: current_page,
    page_size: $('#page_size').val(),
    status_filter: $('#status_filter').val(),
    search_word: $('#search_word').val().trim()
  };

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/web_list',
    type: "POST",
    data: formData,
    dataType: "json",
    success: function(data) {
      cached_download_list = data.list || [];
      render_history_table(cached_download_list);
      render_paging(data.paging);
    }
  });
}

function format_bytes(bytes) {
  if (!bytes || bytes <= 0) return '0 B';
  var units = ['B', 'KB', 'MB', 'GB', 'TB'];
  var i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(2) + ' ' + units[i];
}

function render_history_table(list) {
  var tbody = $('#download_list_tbody');
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
    else if (it.status === 'move_failed') statusBadge = '<span class="badge badge-warning">이동 실패</span>';
    else if (it.status === 'failed') statusBadge = '<span class="badge badge-danger">실패</span>';
    else statusBadge = '<span class="badge badge-info">' + it.status + '</span>';

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

function render_paging(paging) {
  if (!paging) return;
  var str = make_page_html(paging.page, paging.total_page, 'request_download_list');
  $('#page1').html(str);
  $('#page2').html(str);
}

function make_page_html(current, total, func_name) {
  if (total <= 1) return '';
  var str = '<ul class="pagination pagination-sm justify-content-center">';
  for (var i = 1; i <= total; i++) {
    if (i === current) {
      str += '<li class="page-item active"><a class="page-link" href="#">' + i + '</a></li>';
    } else {
      str += '<li class="page-item"><a class="page-link" href="#" onclick="' + func_name + '(' + i + '); return false;">' + i + '</a></li>';
    }
  }
  str += '</ul>';
  return str;
}

$('#search_btn').click(function(e){
  e.preventDefault();
  request_download_list(1);
});

$('#status_filter, #page_size').change(function(){
  request_download_list(1);
});

$('#reset_btn').click(function(e){
  e.preventDefault();
  $('#status_filter').val('all');
  $('#search_word').val('');
  $('#page_size').val('25');
  request_download_list(1);
});

$('#modal_retry_profile_select').change(function(){
  var pName = $(this).val();
  var pObj = cached_download_profiles.find(function(p){ return p.name === pName; });
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

$(document).on('click', '.btn_list_action', function(e){
  e.preventDefault();
  var act = $(this).data('action');
  var id = $(this).data('id');
  var item = cached_download_list.find(function(x){ return String(x.id) === String(id); });
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
        var matchP = cached_download_profiles.find(function(p){
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
    if (!confirm('[' + id + '번 항목]\n"' + titleText + '"\n\n해당 작업을 최종 완료(completed) 상태로 변경하시겠습니까?\n더 이상 다운로드나 이송을 시도하지 않습니다.')) {
      return;
    }
  } else if (act === 'delete') {
    if (!confirm('[' + id + '번 항목]\n"' + titleText + '"\n\n해당 작업을 DB에서 완전히 삭제하시겠습니까?\n(피드 동기화 기간 내의 글일 경우 다음 주기에 다시 수집될 수 있습니다)')) {
      return;
    }
  }

  notify('작업을 요청 중입니다...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/item_action',
    type: "POST",
    data: {action: act, id: id},
    dataType: "json",
    success: function(data) {
      if (data.ret === 'success') {
        notify(data.msg || '작업이 처리되었습니다.', 'success');
        request_download_list(current_page);
      } else {
        notify(data.msg || '실패', 'warning');
      }
    }
  });
});

$(document).on('click', '#btn_confirm_retry_execute', function(e){
  e.preventDefault();
  var id = $('#modal_retry_item_id').val();
  var pName = $('#modal_retry_profile_select').val();

  notify('다운로드 재시도 등록 중...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/item_action',
    type: "POST",
    data: {
      action: 'retry',
      id: id,
      profile_name: pName
    },
    dataType: "json",
    success: function(data) {
      if (data.ret === 'success') {
        notify(data.msg || '재시도 대기열에 등록되었습니다.', 'success');
        $('#download_retry_modal').modal('hide');
        request_download_list(current_page);
      } else {
        notify(data.msg || '재시도 실패', 'warning');
      }
    }
  });
});
