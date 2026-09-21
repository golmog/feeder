var current_page = 1;

$(document).ready(function(){
  request_download_list(1);
});

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
      render_history_table(data.list || []);
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
    str += '      <button type="button" class="btn btn-outline-warning btn_list_action" data-action="retry" data-id="' + it.id + '">재시도</button>';
    str += '      <button type="button" class="btn btn-outline-success btn_list_action" data-action="force_complete" data-id="' + it.id + '">완료</button>';
    str += '      <button type="button" class="btn btn-outline-danger btn_list_action" data-action="delete" data-id="' + it.id + '">삭제</button>';
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

$(document).on('click', '.btn_list_action', function(e){
  e.preventDefault();
  var act = $(this).data('action');
  var id = $(this).data('id');

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/item_action',
    type: "POST",
    data: {action: act, id: id},
    dataType: "json",
    success: function(data) {
      if (data.ret === 'success') {
        notify('작업이 처리되었습니다.', 'info');
        request_download_list(current_page);
      } else {
        notify(data.msg || '실패', 'warning');
      }
    }
  });
});
