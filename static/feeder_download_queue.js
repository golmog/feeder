var sse_source = null;

$(document).ready(function(){
  request_active_queue();
  if (enable_sse) {
    init_sse_listener();
  }
});

function init_sse_listener() {
  if (window.EventSource) {
    var sseUrl = '/' + package_name + '/api/' + sub + '/sse' + (apikey ? '?apikey=' + apikey : '');
    sse_source = new EventSource(sseUrl);
    sse_source.onmessage = function(event) {
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
    type: "POST",
    data: {page: 1, page_size: 50, status_filter: 'active'},
    dataType: "json",
    success: function(data) {
      render_queue_rows(data.list || []);
    }
  });
}

function format_bytes(bytes) {
  if (!bytes || bytes <= 0) return '0 B';
  var units = ['B', 'KB', 'MB', 'GB', 'TB'];
  var i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(2) + ' ' + units[i];
}

function render_queue_rows(list) {
  var tbody = $('#active_queue_tbody');
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
    else if (it.status === 'pending_colab') statusBadge = '<span class="badge badge-warning">Colab 대기</span>';
    else if (it.status === 'colab_transferring') statusBadge = '<span class="badge badge-primary">Colab 전송 중</span>';
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

$('#queue_refresh_btn').click(function(e){
  e.preventDefault();
  request_active_queue();
  notify('작업 큐를 새로고침했습니다.', 'info');
});

$(document).on('click', '.queue_action_btn', function(e){
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
        request_active_queue();
      } else {
        notify(data.msg || '실패', 'warning');
      }
    }
  });
});
