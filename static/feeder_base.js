var cached_download_profiles = [];
var current_search_xhr = null;
var current_data = null;
var feeder_ace_instances = [];

// =============================================================================
// 플랫폼 공통 유틸리티
// =============================================================================
function format_bytes(bytes) {
  if (!bytes || bytes <= 0) return '0 B';
  var units = ['B', 'KB', 'MB', 'GB', 'TB'];
  var i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(2) + ' ' + units[i];
}

function clean_title_attr(title) {
  if (!title) return '';
  return title.replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function fallbackCopyText(text, target_name) {
  var noticeText = target_name || '링크가';
  var tempInput = $('<textarea>');
  tempInput.css({ position: 'fixed', left: '-9999px', top: '0' });
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

// =============================================================================
// 상단 내비바 및 탭 상태 유지
// =============================================================================
function sync_feeder_header_navbar() {
  try {
    var lastCrawl = localStorage.getItem('feeder_last_crawl_page') || 'setting';
    var lastFeed = localStorage.getItem('feeder_last_feed_page') || 'setting';
    var lastDl = localStorage.getItem('feeder_last_download_page') || 'setting';
    $('.navbar a[href*="/' + package_name + '/crawl"]').attr('href', '/' + package_name + '/crawl/' + lastCrawl);
    $('.navbar a[href*="/' + package_name + '/feed"]').attr('href', '/' + package_name + '/feed/' + lastFeed);
    $('.navbar a[href*="/' + package_name + '/download"]').attr('href', '/' + package_name + '/download/' + lastDl);
  } catch (e) {}
}

function restore_active_subtab(module_name) {
  try {
    var targetSub = module_name || sub;
    var saved_tab = localStorage.getItem(package_name + '_' + targetSub + '_active_tab');
    if (saved_tab) {
      var tabElem = $('#nav-tab a[href="' + saved_tab + '"]');
      if (tabElem.length > 0 && !tabElem.hasClass('active')) {
        tabElem.tab('show');
      }
    }
  } catch (e) {}
}

$(document).on('shown.bs.tab', '#nav-tab a[data-toggle="tab"]', function (e) {
  try {
    var targetTab = $(e.target).attr('href');
    if (targetTab && targetTab.startsWith('#')) {
      localStorage.setItem(package_name + '_' + sub + '_active_tab', targetTab);
    }
  } catch (err) {}
});

$(document).on('click', '.navbar a', function (e) {
  var href = $(this).attr('href') || '';
  if (href.indexOf('/' + package_name + '/crawl') !== -1) {
    var lastCrawl = localStorage.getItem('feeder_last_crawl_page');
    if (lastCrawl && lastCrawl !== 'setting') {
      e.preventDefault();
      window.location.href = '/' + package_name + '/crawl/' + lastCrawl;
    }
  } else if (href.indexOf('/' + package_name + '/feed') !== -1) {
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

// =============================================================================
// Ace Editor 전체화면 버튼 공통 처리
// =============================================================================
$(document).on('click', '.modal-fullscreen-btn', function (e) {
  e.preventDefault();
  var modalDialog = $(this).closest('.modal-dialog');
  modalDialog.toggleClass('modal-fullscreen');
  var icon = $(this).find('i');
  if (modalDialog.hasClass('modal-fullscreen')) {
    icon.removeClass('fa-expand').addClass('fa-compress');
  } else {
    icon.removeClass('fa-compress').addClass('fa-expand');
  }
  setTimeout(function () {
    for (var i = 0; i < feeder_ace_instances.length; i++) {
      if (feeder_ace_instances[i]) feeder_ace_instances[i].resize();
    }
  }, 150);
});

// =============================================================================
// 페이징 생성 및 검색 엔진 공통
// =============================================================================
function load_download_profiles() {
  $.ajax({
    url: '/' + package_name + '/ajax/download/load_profiles',
    type: 'POST',
    dataType: 'json',
    success: function (data) {
      cached_download_profiles = data.profiles || [];
    }
  });
}

function make_page_html(paging) {
  if (!paging || !paging.total_page || paging.total_page <= 1) {
    $('#page1').html('');
    $('#page2').html('');
    return '';
  }

  var cur_page = parseInt(paging.current_page || paging.page || 1, 10);
  var total_page = parseInt(paging.total_page || 1, 10);
  var start_p = parseInt(paging.start_page || 1, 10);
  var end_p = parseInt(paging.end_page || total_page, 10);

  var has_prev = false;
  var prev_p = start_p - 1;
  if (start_p > 1) {
    has_prev = true;
    if (typeof paging.prev_page === 'object' && paging.prev_page !== null && paging.prev_page.page) {
      prev_p = parseInt(paging.prev_page.page, 10);
    } else if (typeof paging.prev_page === 'number' && paging.prev_page > 0) {
      prev_p = paging.prev_page;
    }
  }

  var has_next = false;
  var next_p = end_p + 1;
  if (end_p < total_page) {
    has_next = true;
    if (typeof paging.next_page === 'object' && paging.next_page !== null && paging.next_page.page) {
      next_p = parseInt(paging.next_page.page, 10);
    } else if (typeof paging.next_page === 'number' && paging.next_page > 0) {
      next_p = paging.next_page;
    }
  }

  var str = '<div class="btn-toolbar justify-content-center my-2" role="toolbar">';
  str += '<div class="btn-group btn-group-sm" role="group">';

  if (has_prev) {
    str += '<button type="button" class="btn btn-secondary db-page-btn" data-page="1" title="첫 페이지 (1페이지)">1</button>';
    str += '<button type="button" class="btn btn-secondary db-page-btn" data-page="' + prev_p + '" title="이전 10페이지">&lt;</button>';
  }

  for (var i = start_p; i <= end_p; i++) {
    if (i === cur_page) {
      str += '<button type="button" class="btn btn-primary active font-weight-bold db-page-btn" data-page="' + i + '">' + i + '</button>';
    } else {
      str += '<button type="button" class="btn btn-secondary db-page-btn" data-page="' + i + '">' + i + '</button>';
    }
  }

  if (has_next) {
    str += '<button type="button" class="btn btn-secondary db-page-btn" data-page="' + next_p + '" title="다음 10페이지">&gt;</button>';
    str += '<button type="button" class="btn btn-secondary db-page-btn" data-page="' + total_page + '" title="마지막 페이지 (' + total_page + '페이지)">' + total_page + '</button>';
  }

  str += '</div></div>';
  $('#page1').html(str);
  $('#page2').html(str);
  return str;
}

function render_pagination(paging) {
  make_page_html(paging);
}

window.globalRequestSearch = function (page, preserveScroll) {
  var storage_pfx = sub + '_';
  var page_val = (page !== undefined && page !== null && page !== '')
    ? page.toString()
    : (localStorage.getItem(storage_pfx + 'current_page') || '1');

  var search_word = ($('#search_word').val() || '').trim();
  var page_size = $('#page_size').val() || '25';
  var status_filter = $('#status_filter').val() || 'all';

  localStorage.setItem(storage_pfx + 'current_page', page_val);
  localStorage.setItem(storage_pfx + 'search_word', search_word);
  localStorage.setItem(storage_pfx + 'page_size', page_size);
  localStorage.setItem(storage_pfx + 'status_filter', status_filter);

  var postData = {
    page: page_val,
    page_size: page_size,
    status_filter: status_filter,
    search_word: search_word
  };

  if (sub === 'crawl') {
    var site_select = $('#site_select').val() || localStorage.getItem(storage_pfx + 'site_select') || 'all';
    var board_select = $('#board_select').val() || localStorage.getItem(storage_pfx + 'board_select') || 'all';
    var order = $('#order').val() || 'desc';
    var search_select = $('#search_select').val() || 'title';

    localStorage.setItem(storage_pfx + 'site_select', site_select);
    localStorage.setItem(storage_pfx + 'board_select', board_select);
    localStorage.setItem(storage_pfx + 'order', order);
    localStorage.setItem(storage_pfx + 'search_select', search_select);

    postData.site_select = site_select;
    postData.board_select = board_select;
    postData.order = order;
    postData.search_select = search_select;
  } else if (sub === 'feed') {
    var feed_select = $('#feed_select').val() || localStorage.getItem(storage_pfx + 'feed_select') || '';
    if (feed_select) {
      localStorage.setItem(storage_pfx + 'feed_select', feed_select);
      postData.feed_select = feed_select;
    }
  }

  var savedScrollTop = (preserveScroll === true) ? window.scrollY : 0;
  $('#page1').html('');
  $('#page2').html('');

  var skeletonCount = Math.min(parseInt(page_size, 10) || 10, 8);
  if ($('#list_div').length > 0) {
    var skeletonHtml = '';
    for (var sk = 0; sk < skeletonCount; sk++) {
      skeletonHtml += '<div class="row align-items-center py-3 px-1 border-bottom border-secondary" style="opacity: 0.65;">';
      skeletonHtml += '  <div class="col-1"><div class="skeleton-box" style="height: 20px; width: 40px;"></div></div>';
      skeletonHtml += '  <div class="col-2"><div class="skeleton-box mb-1" style="height: 14px; width: 80%;"></div><div class="skeleton-box" style="height: 22px; width: 60%;"></div></div>';
      skeletonHtml += '  <div class="col-9"><div class="skeleton-box mb-2" style="height: 22px; width: 75%;"></div><div class="skeleton-box" style="height: 38px; width: 90%;"></div></div>';
      skeletonHtml += '</div>';
    }
    $('#list_div').html(skeletonHtml);
  } else if ($('#download_list_tbody').length > 0) {
    var tblSkeleton = '';
    for (var tk = 0; tk < skeletonCount; tk++) {
      tblSkeleton += '<tr><td colspan="6" class="py-3"><div class="skeleton-box" style="height: 24px; width: 100%;"></div></td></tr>';
    }
    $('#download_list_tbody').html(tblSkeleton);
  }

  if (current_search_xhr && typeof current_search_xhr.abort === 'function') {
    current_search_xhr.abort();
    current_search_xhr = null;
  }

  current_search_xhr = $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/web_list',
    type: 'POST',
    cache: false,
    data: postData,
    dataType: 'json',
    success: function (ret) {
      current_search_xhr = null;
      if (!ret) {
        notify('서버 응답이 없습니다.', 'warning');
        return;
      }
      current_data = ret;
      if (ret.info && typeof build_search_form === 'function') {
        build_search_form(ret.info);
      }
      var listData = ret.list || ret;
      if (typeof listData === 'string') {
        try { listData = JSON.parse(listData); } catch (e) { listData = []; }
      }
      try {
        if (typeof make_list === 'function') {
          make_list(listData);
        }
      } catch (renderErr) {
        console.error('make_list 렌더링 에러:', renderErr);
        if ($('#list_div').length > 0) {
          $('#list_div').html('<div class="alert alert-danger m-3">렌더링 오류: ' + renderErr.message + '</div>');
        }
        return;
      }
      if (ret.paging) {
        make_page_html(ret.paging);
      }
      if (preserveScroll && savedScrollTop > 0) {
        setTimeout(function () {
          window.scrollTo({ top: savedScrollTop, behavior: 'instant' });
        }, 10);
      }
    },
    error: function (request, status, error) {
      if (status === 'abort') return;
      current_search_xhr = null;
      if ($('#list_div').length > 0) {
        $('#list_div').html('<div class="col-12 text-center p-4 text-danger">목록 요청 실패: ' + (error || status || '서버 응답 없음') + '</div>');
      } else if ($('#download_list_tbody').length > 0) {
        $('#download_list_tbody').html('<tr><td colspan="6" class="text-center p-4 text-danger">목록 요청 실패: ' + (error || status) + '</td></tr>');
      }
    }
  });
};

$(document).off('click', '.db-page-btn, #page, #gloablSearchPageBtn').on('click', '.db-page-btn, #page, #gloablSearchPageBtn', function (e) {
  e.preventDefault();
  e.stopPropagation();
  var targetPage = $(this).attr('data-page') || $(this).data('page') || $(this).text().trim();
  if (targetPage && !isNaN(targetPage)) {
    window.globalRequestSearch(targetPage.toString(), false);
  }
});

// =============================================================================
// 마그넷 복사, 토렌트 정보 및 다운로드 추가 모달 공통
// =============================================================================
$(document).on('click', '.copy_magnet_btn', function (e) {
  e.preventDefault();
  var magnet = $(this).data('hash');
  var is_ed2k = String(magnet).toLowerCase().startsWith('ed2k://');
  var target_name = is_ed2k ? 'ed2k 주소가' : '마그넷 주소가';

  if (navigator.clipboard && window.isSecureContext) {
    navigator.clipboard.writeText(magnet).then(function () {
      notify(target_name + ' 복사되었습니다.', 'success');
    }).catch(function () {
      fallbackCopyText(magnet, target_name);
    });
  } else {
    fallbackCopyText(magnet, target_name);
  }
});

$(document).on('click', '.global_torrent_info_btn', function (e) {
  e.preventDefault();
  var magnet_hash = $(this).data('hash');
  notify('토렌트 정보 조회를 요청했습니다...', 'info');

  $.ajax({
    url: '/' + package_name + '/ajax/crawl/torrent_info',
    type: 'POST',
    data: { hash: magnet_hash },
    dataType: 'json',
    success: function (data) {
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
    error: function (xhr, status, error) {
      notify('정보 조회 실패: ' + error, 'danger');
    }
  });
});

$(document).on('click', '.direct_download_btn', function (e) {
  e.preventDefault();
  var mag = $(this).data('hash');
  var title = $(this).data('title');
  var feed = $(this).data('feed') || 'DIRECT';

  $('#modal_direct_title').text(title);
  $('#modal_direct_magnet').val(mag);
  $('#modal_direct_feed_name').val(feed);

  var pSelect = $('#modal_direct_profile_select');
  pSelect.empty();
  if (cached_download_profiles && cached_download_profiles.length > 0) {
    for (var i = 0; i < cached_download_profiles.length; i++) {
      var p = cached_download_profiles[i];
      pSelect.append('<option value="' + p.name + '">' + p.name + '</option>');
    }
  } else {
    pSelect.append('<option value="">-- 기본 다운로더 전체 사용 --</option>');
  }
  pSelect.trigger('change');
  $('#direct_download_modal').modal('show');
});

$(document).on('change', '#modal_direct_profile_select', function () {
  var pName = $(this).val();
  var pObj = cached_download_profiles.find(function (p) { return p.name === pName; });
  if (pObj) {
    var chainStr = (pObj.priority_chain || []).join(' -> ') || '(비어있음)';
    var dest = pObj.destination || {};
    var destStr = dest.type || 'local';
    if (dest.type === 'colab_gdrive') destStr += ' (Colab 무트래픽)';
    else if (dest.type === 'gdrive_rotation') destStr += ' (SA 15GB 우회)';

    $('#modal_direct_chain_preview').text(chainStr);
    $('#modal_direct_dest_preview').text(destStr);
  } else {
    $('#modal_direct_chain_preview').text('(기본 활성 다운로더 전체)');
    $('#modal_direct_dest_preview').text('local (로컬 디스크 보존)');
  }
});

$(document).on('click', '#btn_confirm_direct_download', function (e) {
  e.preventDefault();
  var mag = $('#modal_direct_magnet').val();
  var title = $('#modal_direct_title').text();
  var pName = $('#modal_direct_profile_select').val();
  var feed = $('#modal_direct_feed_name').val() || 'DIRECT';

  notify('다운로드 큐 등록 요청 중...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/download/direct_add',
    type: 'POST',
    data: {
      title: title,
      magnet: mag,
      profile_name: pName,
      feed_name: feed
    },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify('다운로드 큐에 성공적으로 등록되었습니다.', 'success');
        $('#direct_download_modal').modal('hide');
        window.globalRequestSearch(null, true);
      } else {
        notify(data.msg || '다운로드 등록 실패', 'warning');
      }
    }
  });
});
