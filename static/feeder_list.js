var current_data = null;
var site_info = null;

$(document).ready(function(){
  localStorage.setItem('feeder_last_feed_page', 'list');
  sync_feeder_header_navbar();

  var saved_size = localStorage.getItem(sub + '_page_size');
  if (saved_size) { $("#page_size").val(saved_size); }

  var saved_order = localStorage.getItem(sub + '_order');
  if (saved_order) { $("#order").val(saved_order); }

  var saved_search_select = localStorage.getItem(sub + '_search_select');
  if (saved_search_select) { $("#search_select").val(saved_search_select); }

  var saved_word = localStorage.getItem(sub + '_search_word');
  if (saved_word) { $("#search_word").val(saved_word); }

  var saved_page = localStorage.getItem(sub + '_current_page') || '1';
  globalRequestSearch(saved_page);
});

$("#search").click(function(e) {
  e.preventDefault();
  localStorage.setItem(sub + '_search_word', $('#search_word').val().trim());
  localStorage.setItem(sub + '_current_page', '1');
  globalRequestSearch('1');
});

$("#search_word").keydown(function(e) {
  if (e.which == 13) {
    e.preventDefault();
    localStorage.setItem(sub + '_search_word', $('#search_word').val().trim());
    localStorage.setItem(sub + '_current_page', '1');
    globalRequestSearch('1');
  }
});

$("body").on('click', '#page, #gloablSearchPageBtn', function(e){
  e.preventDefault();
  var targetPage = $(this).data('page');
  if (targetPage && !isNaN(targetPage)) {
    localStorage.setItem(sub + '_current_page', targetPage);
    globalRequestSearch(String(targetPage));
  }
});

$("#reset_btn").click(function(e){
  e.preventDefault();
  $("#site_select").val('all').trigger('change');
  $("#order").val('desc');
  $("#page_size").val('25');
  $("#search_select").val('title');
  $("#search_word").val('');

  localStorage.removeItem(sub + '_search_word');
  localStorage.setItem(sub + '_site_select', 'all');
  localStorage.setItem(sub + '_board_select', 'all');
  localStorage.setItem(sub + '_order', 'desc');
  localStorage.setItem(sub + '_page_size', '25');
  localStorage.setItem(sub + '_search_select', 'title');
  localStorage.setItem(sub + '_current_page', '1');

  globalRequestSearch('1');
});

$("body").on('change', '#site_select', function(e){
  e.preventDefault();
  var selected_site = $(this).val();
  localStorage.setItem(sub + '_site_select', selected_site);
  localStorage.setItem(sub + '_board_select', 'all');
  localStorage.setItem(sub + '_current_page', '1');
  update_board_select(selected_site);
  globalRequestSearch('1');
});

$("body").on('change', '#board_select', function(e){
  e.preventDefault();
  localStorage.setItem(sub + '_board_select', $(this).val());
  localStorage.setItem(sub + '_current_page', '1');
  globalRequestSearch('1');
});

$("body").on('change', '#order', function(e){
  e.preventDefault();
  localStorage.setItem(sub + '_order', $(this).val());
  localStorage.setItem(sub + '_current_page', '1');
  globalRequestSearch('1');
});

$("body").on('change', '#page_size', function(e){
  e.preventDefault();
  localStorage.setItem(sub + '_page_size', $(this).val());
  localStorage.setItem(sub + '_current_page', '1');
  globalRequestSearch('1');
});

$("body").on('change', '#search_select', function(e){
  e.preventDefault();
  localStorage.setItem(sub + '_search_select', $(this).val());
});

function build_search_form(data) {
  if (!data || site_info) return;
  site_info = data;

  var saved_site = localStorage.getItem(sub + '_site_select') || 'all';
  var site_str = '<select id="site_select" name="site_select" class="form-control form-control-sm"><option value="all">전체 사이트</option>';
  if (data.site) {
    for (var i = 0; i < data.site.length; i++) {
      var sName = data.site[i];
      var isSel = (sName === saved_site) ? 'selected' : '';
      site_str += '<option value="' + sName + '" ' + isSel + '>' + sName + '</option>';
    }
  }
  site_str += '</select>';
  $('#site_select_div').html(site_str);

  update_board_select(saved_site);
}

function update_board_select(selected_site) {
  var saved_board = localStorage.getItem(sub + '_board_select') || 'all';
  var str = '<select id="board_select" name="board_select" class="form-control form-control-sm"';
  
  if (selected_site === 'all') {
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
      var bName = (typeof item === 'object' && item !== null && item.name) ? item.name : item;
      if (bKey && bKey !== 'None' && bKey !== 'null') {
        var isSel = (bKey === saved_board) ? 'selected' : '';
        var displayName = bKey;
        str += '<option value="' + bKey + '" ' + isSel + '>' + displayName + '</option>';
      }
    }
  }
  str += '</select>';
  $('#board_select_div').html(str);
}

function make_list(data) {
  try {
    if (current_data && current_data.info) {
      build_search_form(current_data.info);
    }

    if (!data || data.length === 0) {
      document.getElementById("list_div").innerHTML = '<div class="text-center py-4 text-muted">수집된 콘텐츠가 없습니다.</div>';
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
          detail_col += '  <div class="text-truncate mb-2">' + link_badge + '<small><a href="' + mag + '">' + mag + '</a></small></div>';
          detail_col += '  <div class="btn-group btn-group-sm">';
          detail_col += '    <button type="button" class="btn btn-sm btn-secondary copy_magnet_btn text-white" data-hash="' + mag + '"><i class="fa fa-copy mr-1"></i>' + copy_btn_text + '</button>';
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
          var file_url = clean_ddns + '/' + package_name + '/api/download?id=' + item.id + '_' + f_idx + '&apikey=' + apikey;
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

$(document).on('click', '.copy_magnet_btn', function(e){
  e.preventDefault();
  var magnet = $(this).data('hash');
  var is_ed2k = String(magnet).toLowerCase().startsWith('ed2k://');
  var target_name = is_ed2k ? 'ed2k 주소가' : '마그넷 주소가';

  if (navigator.clipboard && window.isSecureContext) {
    navigator.clipboard.writeText(magnet).then(function() {
      notify(target_name + ' 복사되었습니다.', 'success');
    }).catch(function() {
      fallbackCopyText(magnet, target_name);
    });
  } else {
    fallbackCopyText(magnet, target_name);
  }
});

function fallbackCopyText(text, target_name) {
  var noticeText = target_name || '링크가';
  var tempInput = $('<textarea>');
  tempInput.css({position: 'fixed', left: '-9999px', top: '0'});
  $('body').append(tempInput);
  tempInput.val(text).select();
  try {
    document.execCommand('copy');
    notify(noticeText + ' 복사되었습니다.', 'success');
  } catch (err) {
    notify('복사에 실패했습니다.', 'warning');
  }
  tempInput.remove();
}

$(document).on('click', '.global_torrent_info_btn', function(e){
  e.preventDefault();
  var magnet_hash = $(this).data('hash');
  notify('토렌트 정보 조회를 요청했습니다...', 'info');

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/torrent_info',
    type: "POST",
    data: {hash: magnet_hash},
    dataType: "json",
    success: function(data) {
      if (data) {
        $('#torrent_info_title').text(data.name || '토렌트 정보');
        var html_str = '<p><strong>해시:</strong> <code>' + data.info_hash + '</code></p>';
        if (data.total_size) {
          var size_mb = (data.total_size / (1024 * 1024)).toFixed(2);
          html_str += '<p><strong>전체 크기:</strong> ' + size_mb + ' MB</p>';
        }
        if (data.files && data.files.length > 0) {
          html_str += '<hr><h6>포함된 파일 목록 (' + data.files.length + '개):</h6><ul class="list-group list-group-flush small" style="max-height: 350px; overflow-y: auto;">';
          for (var i = 0; i < data.files.length; i++) {
            var f = data.files[i];
            var f_size = f.size ? ' (' + (f.size / (1024 * 1024)).toFixed(2) + ' MB)' : '';
            html_str += '<li class="list-group-item py-1 px-2">' + (f.name || f) + f_size + '</li>';
          }
          html_str += '</ul>';
        }
        $('#torrent_info_content').html(html_str);
        $('#torrent_info_modal').modal('show');
      } else {
        notify('토렌트 정보를 획득하지 못했습니다.', 'warning');
      }
    },
    error: function(xhr, status, error) {
      notify('정보 조회 실패: ' + error, 'danger');
    }
  });
});

function sync_feeder_header_navbar() {
  var lastFeed = localStorage.getItem('feeder_last_feed_page') || 'setting';
  var lastDl = localStorage.getItem('feeder_last_download_page') || 'setting';

  $('.navbar a[href*="/' + package_name + '/feed"]').attr('href', '/' + package_name + '/feed/' + lastFeed);
  $('.navbar a[href*="/' + package_name + '/download"]').attr('href', '/' + package_name + '/download/' + lastDl);
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
