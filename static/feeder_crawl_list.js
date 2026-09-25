var site_info = null;

$(document).ready(function(){
  try {
    localStorage.setItem('feeder_last_crawl_page', 'list');
    sync_feeder_header_navbar();
  } catch(err) {}

  var saved_site = localStorage.getItem(sub + '_site_select') || 'all';
  var saved_board = localStorage.getItem(sub + '_board_select') || 'all';
  var saved_status = localStorage.getItem(sub + '_status_filter') || 'all';
  var saved_order = localStorage.getItem(sub + '_order') || 'desc';
  var saved_size = localStorage.getItem(sub + '_page_size') || '25';
  var saved_search_select = localStorage.getItem(sub + '_search_select') || 'title';
  var saved_word = localStorage.getItem(sub + '_search_word') || '';
  var saved_page = localStorage.getItem(sub + '_current_page') || '1';

  if ($('#site_select').length > 0) {
    $('#site_select').val(saved_site);
  }
  $('#status_filter').val(saved_status);
  $('#order').val(saved_order);
  $('#page_size').val(saved_size);
  $('#search_select').val(saved_search_select);
  $('#search_word').val(saved_word);

  if (typeof server_site_info !== 'undefined' && server_site_info && server_site_info.site) {
    build_search_form(server_site_info);
  }

  load_download_profiles();
  globalRequestSearch(saved_page);
});

$('#search').click(function(e){
  e.preventDefault();
  window.globalRequestSearch('1', false);
});

$('#search_word').keydown(function(e){
  if (e.which === 13) {
    e.preventDefault();
    window.globalRequestSearch('1', false);
  }
});

$('#status_filter, #order, #page_size, #search_select').change(function(){
  window.globalRequestSearch('1', false);
});

$('#reset_btn').click(function(e){
  e.preventDefault();
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

$('body').on('change', '#site_select', function(e){
  e.preventDefault();
  var selected_site = $(this).val();
  localStorage.setItem(sub + '_site_select', selected_site);
  localStorage.setItem(sub + '_board_select', 'all');
  update_board_select(selected_site);
  window.globalRequestSearch('1', false);
});

$('body').on('change', '#board_select', function(e){
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
      document.getElementById("list_div").innerHTML = '<div class="text-center py-4 text-muted">수집된 콘텐츠가 없습니다.</div>';
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
            try { t_info = JSON.parse(t_info); } catch(e) { t_info = null; }
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
          detail_col += '  <i class="fa fa-lock mr-1 text-warning"></i>사이트 첨부파일 다운로드 권한(로그인)이 필요하여 마그넷 수집이 제외된 항목입니다. (다음 수집 주기 시 탐색 건너뜀)';
          detail_col += '</div>';
        } else {
          detail_col += '<div class="p-2 mb-1 rounded small text-muted" style="background: rgba(128, 128, 128, 0.06);">';
          detail_col += '  <i class="fa fa-info-circle mr-1"></i>수집된 마그넷 또는 첨부파일이 없습니다.';
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
    document.getElementById("list_div").innerHTML = '<div class="alert alert-danger m-3">목록 렌더링 중 오류가 발생했습니다: ' + err.message + '</div>';
  }
}
