$(document).ready(function(){
  try {
    localStorage.setItem('feeder_last_feed_page', 'list');
    sync_feeder_header_navbar();
  } catch(err) {}

  var saved_feed = localStorage.getItem(sub + '_feed_select');
  if (saved_feed) { $("#feed_select").val(saved_feed); }

  var saved_status = localStorage.getItem(sub + '_status_filter') || 'all';
  $("#status_filter").val(saved_status);

  var saved_size = localStorage.getItem(sub + '_page_size') || '25';
  $("#page_size").val(saved_size);

  var saved_word = localStorage.getItem(sub + '_search_word') || '';
  $("#search_word").val(saved_word);

  var saved_page = localStorage.getItem(sub + '_current_page') || '1';

  load_download_profiles();
  window.globalRequestSearch(saved_page, false);
});

$("#search").click(function(e) {
  e.preventDefault();
  window.globalRequestSearch('1', false);
});

$("#search_word").keydown(function(e) {
  if (e.which === 13) {
    e.preventDefault();
    window.globalRequestSearch('1', false);
  }
});

$("#feed_select, #status_filter, #page_size").change(function(){
  window.globalRequestSearch('1', false);
});

$("#reset_btn").click(function(e){
  e.preventDefault();
  $("#status_filter").val('all');
  $("#page_size").val('25');
  $("#search_word").val('');

  localStorage.removeItem(sub + '_search_word');
  localStorage.setItem(sub + '_status_filter', 'all');
  localStorage.setItem(sub + '_page_size', '25');
  localStorage.setItem(sub + '_current_page', '1');

  window.globalRequestSearch('1', false);
});

function make_list(data) {
  try {
    if (!data || data.length === 0) {
      document.getElementById("list_div").innerHTML = '<div class="text-center py-4 text-muted">선택한 피드 조건에 일치하는 콘텐츠가 없습니다.</div>';
      return;
    }

    var str = '';
    for (var i = 0; i < data.length; i++) {
      var item = data[i];
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
      str += j_col(2, site_col);

      var detail_col = '<div class="mb-2"><strong><a href="' + item.url + '" target="_blank">' + item.title + '</a></strong></div>';

      if (item.magnet && item.magnet.length > 0) {
        for (var j = 0; j < item.magnet.length; j++) {
          var mag = item.magnet[j];
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

          detail_col += '<div class="p-2 mb-2 rounded" style="background: rgba(128,128,128,0.1); font-size: 0.85rem;">';
          detail_col += '  <div class="text-truncate mb-2">' + link_badge + dl_badge + '<small><a href="' + mag + '">' + mag + '</a></small></div>';
          detail_col += '  <div class="btn-group btn-group-sm">';
          detail_col += '    <button type="button" class="btn btn-sm btn-secondary copy_magnet_btn text-white" data-hash="' + mag + '"><i class="fa fa-copy mr-1"></i>' + copy_btn_text + '</button>';
          detail_col += '    <button type="button" class="btn btn-sm btn-outline-info direct_download_btn" data-hash="' + mag + '" data-title="' + clean_title_attr(item.title) + '" data-feed="' + ($('#feed_select').val() || '') + '"><i class="fa fa-download mr-1"></i>다운로드 추가</button>';
          detail_col += '  </div>';
          detail_col += '</div>';
        }
      }

      str += j_col(9, detail_col);
      str += j_row_end();
      if (i != data.length - 1) str += j_hr();
    }
    document.getElementById("list_div").innerHTML = str;
  } catch (err) {
    console.error("make_list 렌더링 오류:", err);
    document.getElementById("list_div").innerHTML = '<div class="alert alert-danger m-3">목록 렌더링 오류: ' + err.message + '</div>';
  }
}
