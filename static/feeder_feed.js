// =============================================================================
// FEED 모듈 전용 스크립트 (List & Setting 통합)
// =============================================================================
var current_feeds = [];
var current_crawlers = [];
var modal_feed_sources = [];

// -----------------------------------------------------------------------------
// 초기화 분기 (List 화면 vs Setting 화면 자동 감지)
// -----------------------------------------------------------------------------
$(document).ready(function () {
  try {
    sync_feeder_header_navbar();
  } catch (err) {}

  // List 화면 진입 시
  if ($('#list_div').length > 0 && $('#feed_select').length > 0) {
    try {
      localStorage.setItem('feeder_last_feed_page', 'list');
      sync_feeder_header_navbar();
    } catch (err) {}

    var saved_feed = localStorage.getItem(sub + '_feed_select') || 'all';
    $('#feed_select').val(saved_feed);

    var saved_status = localStorage.getItem(sub + '_status_filter') || 'all';
    $('#status_filter').val(saved_status);

    var saved_size = localStorage.getItem(sub + '_page_size') || '25';
    $('#page_size').val(saved_size);

    var saved_word = localStorage.getItem(sub + '_search_word') || '';
    $('#search_word').val(saved_word);

    var saved_page = localStorage.getItem(sub + '_current_page') || '1';

    load_download_profiles();
    window.globalRequestSearch(saved_page, false);
  }

  // Setting 화면 진입 시
  if ($('#feed_list_tbody').length > 0) {
    try {
      localStorage.setItem('feeder_last_feed_page', 'setting');
      sync_feeder_header_navbar();
    } catch (err) {}

    use_collapse('feed_make_rss_file');
    use_collapse('feed_use_proxy');
    load_all_feed_data();
    restore_active_subtab('feed');
    setTimeout(function () { restore_active_subtab('feed'); }, 80);
  }
});

// -----------------------------------------------------------------------------
// [FEED: LIST 화면 로직]
// -----------------------------------------------------------------------------
$('#search').click(function (e) {
  e.preventDefault();
  window.globalRequestSearch('1', false);
});

$('#search_word').keydown(function (e) {
  if (e.which === 13) {
    e.preventDefault();
    localStorage.setItem(sub + '_search_word', $('#search_word').val().trim());
    localStorage.setItem(sub + '_current_page', '1');
    window.globalRequestSearch('1', false);
  }
});

$('#feed_select').change(function () {
  var selectedFeed = $(this).val();
  localStorage.setItem(sub + '_feed_select', selectedFeed);
  localStorage.setItem(sub + '_current_page', '1');
  window.globalRequestSearch('1', false);
});

$('#status_filter').change(function () {
  localStorage.setItem(sub + '_status_filter', $(this).val());
  localStorage.setItem(sub + '_current_page', '1');
  window.globalRequestSearch('1', false);
});

$('#feed_select, #status_filter, #page_size').change(function () {
  if ($('#list_div').length > 0 && $('#feed_select').length > 0) {
    window.globalRequestSearch('1', false);
  }
});

$('#reset_btn').click(function (e) {
  e.preventDefault();
  if ($('#feed_select').length === 0) return;

  $('#feed_select').val('all');
  localStorage.setItem(sub + '_feed_select', 'all');
  $('#status_filter').val('all');
  $('#page_size').val('25');
  $('#search_word').val('');

  localStorage.removeItem(sub + '_search_word');
  localStorage.setItem(sub + '_status_filter', 'all');
  localStorage.setItem(sub + '_page_size', '25');
  localStorage.setItem(sub + '_current_page', '1');

  window.globalRequestSearch('1', false);
});

function make_list(data) {
  try {
    if (!data || data.length === 0) {
      document.getElementById('list_div').innerHTML = '<div class="text-center py-4 text-muted">선택한 피드 조건에 일치하는 콘텐츠가 없습니다.</div>';
      return;
    }

    var str = '';
    for (var i = 0; i < data.length; i++) {
      var item = data[i];
      str += j_row_start();
      str += j_col(1, item.id);

      var site_col = '<small class="text-muted">' + (item.created_time || '') + '</small><br>';
      site_col += '<span class="badge badge-info">' + (item.feed_name || 'Feed') + '</span> ';

      var srcStr = String(item.source_name || '');
      if (srcStr && srcStr !== 'None' && srcStr !== 'null') {
        var isRss = (item.source_type === 'rss');
        var badgeClass = isRss ? 'badge-warning' : 'badge-secondary';
        site_col += '<span class="badge ' + badgeClass + '" style="font-size: 0.76rem;" title="' + srcStr + '">' + (isRss ? 'RSS' : srcStr) + '</span>';
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
          detail_col += '    <button type="button" class="btn btn-sm btn-outline-info direct_download_btn" data-hash="' + mag + '" data-title="' + clean_title_attr(item.title) + '" data-feed="' + (item.feed_name || $('#feed_select').val() || '') + '"><i class="fa fa-download mr-1"></i>다운로드 추가</button>';
          detail_col += '  </div>';
          detail_col += '</div>';
        }
      }

      str += j_col(9, detail_col);
      str += j_row_end();
      if (i !== data.length - 1) str += j_hr();
    }
    document.getElementById('list_div').innerHTML = str;
  } catch (err) {
    console.error('make_list 렌더링 오류:', err);
    document.getElementById('list_div').innerHTML = '<div class="alert alert-danger m-3">목록 렌더링 오류: ' + err.message + '</div>';
  }
}

// -----------------------------------------------------------------------------
// [FEED: SETTING 화면 로직]
// -----------------------------------------------------------------------------
$('#feed_make_rss_file').change(function () { use_collapse('feed_make_rss_file'); });
$('#feed_use_proxy').change(function () { use_collapse('feed_use_proxy'); });

function load_all_feed_data() {
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/load_feeds',
    type: 'POST',
    dataType: 'json',
    success: function (data) {
      current_feeds = data.feeds || [];
      render_feeds(current_feeds);
    }
  });

  $.ajax({
    url: '/' + package_name + '/ajax/crawl/load_crawlers',
    type: 'POST',
    dataType: 'json',
    success: function (data) {
      current_crawlers = data.crawlers || [];
    }
  });
}

function render_feeds(data) {
  var tbody = $('#feed_list_tbody');
  if (!tbody.length) return;
  if (!data || data.length === 0) {
    tbody.html('<tr><td colspan="4" class="py-4 text-muted">등록된 피드(FEEDS)가 없습니다.</td></tr>');
    return;
  }
  var str = '';
  for (var i = 0; i < data.length; i++) {
    var item = data[i];
    var isRssFile = (item.use_rss_file === true || item.use_rss_file === 'True' || item.use_rss_file === 'true' || item.use_rss_file === 'on');

    var sourcesHtml = '';
    var sources = item.sources || [];
    if (sources.length > 0) {
      sourcesHtml += '<div class="mt-1">';
      for (var s = 0; s < sources.length; s++) {
        var subTag = sources[s].subcat ? ':' + sources[s].subcat : '';
        sourcesHtml += '<span class="badge badge-info mr-1 mb-1 font-weight-normal">' + sources[s].site + ' [' + sources[s].board + subTag + ']</span>';
      }
      sourcesHtml += '</div>';
    } else {
      sourcesHtml += '<div class="text-muted small">소스 없음</div>';
    }

    var filterSummary = [];
    if (item.quality) filterSummary.push('화질: ' + item.quality);
    if (item.regexp && item.regexp.reject && item.regexp.reject.length > 0) filterSummary.push('거부(' + item.regexp.reject.length + '개)');
    if (item.regexp && item.regexp.accept && item.regexp.accept.length > 0) filterSummary.push('허용(' + item.regexp.accept.length + '개)');
    if (item.regexp && item.regexp.reject_excluding && item.regexp.reject_excluding.length > 0) filterSummary.push('필수(' + item.regexp.reject_excluding.length + '개)');
    if (item.accept_all) filterSummary.push('accept_all');
    var filterText = filterSummary.length > 0 ? filterSummary.join(' / ') : '<span class="text-muted">전역 설정 적용</span>';

    str += '<tr>';
    str += '  <td class="font-weight-bold">' + item.id + '</td>';
    str += '  <td class="text-left">';
    str += '    <strong>' + item.name + '</strong><br>' + sourcesHtml;
    str += '  </td>';
    str += '  <td class="text-left small" style="line-height: 1.6;">';
    str += '    ' + filterText + '<br>';
    str += '    공유 XML: ' + (isRssFile ? '<span class="text-success font-weight-bold">' + (item.rss_file || (item.name + '.xml')) + '</span>' : '<span class="text-muted">미생성</span>');
    str += '  </td>';
    str += '  <td class="text-left">';
    str += '    <div class="d-flex justify-content-between align-items-center">';
    str += '      <div class="btn-group btn-group-sm">';
    str += '        <button type="button" class="btn btn-primary text-white feed_edit_btn" data-id="' + item.id + '" data-index="' + i + '">수정</button>';
    str += '        <button type="button" class="btn btn-danger text-white remove_feed_btn" data-id="' + item.id + '">삭제</button>';
    str += '        <button type="button" class="btn btn-secondary text-white clear_feed_db_btn" data-name="' + item.name + '" title="해당 피드의 적재 DB만 초기화">DB 비우기</button>';
    if (isRssFile) {
      str += '        <button type="button" class="btn btn-outline-success generate_feed_file_btn" data-id="' + item.id + '" title="XML 파일 즉시 생성">XML 갱신</button>';
    }
    str += '      </div>';
    str += '      <a href="' + item.api + '" target="_blank" class="btn btn-sm text-white" style="background-color: #f26522; font-weight: 500;"><i class="fa fa-rss"></i> RSS 피드</a>';
    str += '    </div>';
    str += '    <div class="mt-1"><small class="text-muted" style="word-break: break-all;">' + item.api + '</small></div>';
    str += '  </td>';
    str += '</tr>';
  }
  tbody.html(str);
}

function set_modal_checkbox(id, is_checked) {
  var bool_val = (is_checked === true || is_checked === 'True' || is_checked === 'true' || is_checked === 'on');
  var elem = $('#' + id);
  elem.prop('checked', bool_val);
  try {
    if (elem.data('bs.toggle') || elem.parent().hasClass('toggle')) {
      elem.bootstrapToggle(bool_val ? 'on' : 'off');
    } else {
      elem.trigger('change');
    }
  } catch (err) {
    elem.trigger('change');
  }
}

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

function render_modal_feed_sources() {
  var tbody = $('#modal_feed_sources_tbody');
  if (!modal_feed_sources || modal_feed_sources.length === 0) {
    tbody.html('<tr><td colspan="4" class="text-muted py-2">지정된 소스 게시판이 없습니다.</td></tr>');
    $('#sources_json').val('[]');
    return;
  }

  var str = '';
  for (var i = 0; i < modal_feed_sources.length; i++) {
    var s = modal_feed_sources[i];
    var is_rss = (s.type === 'rss' || (s.url && !s.site));
    var site_badge = is_rss ? '<span class="badge badge-warning">외부 RSS</span>' : '<span class="badge badge-secondary">' + s.site + '</span>';
    var board_display = is_rss ? ('<span class="small text-truncate d-inline-block" style="max-width: 240px;" title="' + (s.url || s.board) + '">' + (s.url || s.board) + '</span>') : ('<strong>' + s.board + '</strong>');
    var subcat_display = is_rss ? '<span class="text-muted">-</span>' : (s.subcat ? '<span class="badge badge-secondary">' + s.subcat + '</span>' : '<span class="text-muted">-</span>');

    str += '<tr>';
    str += '  <td>' + site_badge + '</td>';
    str += '  <td class="text-left">' + board_display + '</td>';
    str += '  <td>' + subcat_display + '</td>';
    str += '  <td><button type="button" class="btn btn-xs btn-danger text-white remove_feed_source_btn" data-index="' + i + '">제외</button></td>';
    str += '</tr>';
  }
  tbody.html(str);
  $('#sources_json').val(JSON.stringify(modal_feed_sources));
}

function update_crawler_source_select_options() {
  var select = $('#feed_source_crawler_select');
  select.empty();
  var count = 0;
  for (var i = 0; i < current_crawlers.length; i++) {
    var c = current_crawlers[i];
    for (var j = 0; j < (c.boards || []).length; j++) {
      var b = c.boards[j];
      var subTag = b.subcat ? ':' + b.subcat : '';
      select.append('<option value="' + c.site + '|' + b.board + '|' + (b.subcat || '') + '">[' + c.site + '] ' + b.board + subTag + '</option>');
      count++;
    }
  }
  if (count === 0) {
    select.append('<option value="">-- 등록된 수집기 게시판 없음 --</option>');
  }
}

$(document).on('click', '#add_feed_source_btn', function (e) {
  e.preventDefault();
  var rawVal = $('#feed_source_crawler_select').val();
  if (!rawVal) return;
  var parts = rawVal.split('|');
  var site = parts[0];
  var board = parts[1];
  var subcat = parts[2] || '';

  var exists = modal_feed_sources.some(function (s) {
    return s.site === site && s.board === board && (s.subcat || '') === subcat;
  });
  if (exists) {
    notify('이미 소스 목록에 포함되어 있습니다.', 'info');
    return;
  }

  modal_feed_sources.push({
    site: site,
    board: board,
    subcat: subcat,
    full_board_key: board + (subcat ? ':' + subcat : '')
  });
  render_modal_feed_sources();
});

$(document).on('click', '#add_feed_rss_source_btn', function (e) {
  e.preventDefault();
  var rssUrl = $('#feed_source_rss_url_input').val().trim();
  if (!rssUrl) {
    notify('외부 RSS 피드 URL을 입력하세요.', 'warning');
    return;
  }
  if (!rssUrl.startsWith('http://') && !rssUrl.startsWith('https://')) {
    notify('올바른 URL(http:// 또는 https://)을 입력하세요.', 'warning');
    return;
  }

  var exists = modal_feed_sources.some(function (s) {
    return (s.url || s.board) === rssUrl;
  });
  if (exists) {
    notify('이미 소스 목록에 포함되어 있습니다.', 'info');
    return;
  }

  modal_feed_sources.push({
    type: 'rss',
    site: '외부RSS',
    board: rssUrl,
    url: rssUrl,
    subcat: '',
    full_board_key: rssUrl
  });
  render_modal_feed_sources();
  $('#feed_source_rss_url_input').val('');
});

$(document).on('click', '.clear_feed_db_btn', function (e) {
  e.preventDefault();
  var feed_name = $(this).data('name');
  if (!confirm('[' + feed_name + '] 피드의 저장 데이터(DB)만 초기화하시겠습니까?\n(피드 설정은 그대로 유지됩니다)')) return;

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/clear_feed_db',
    type: 'POST',
    data: { feed_name: feed_name },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify(data.msg || '[' + feed_name + '] 피드 DB가 초기화되었습니다.', 'success');
      } else {
        notify('DB 비우기 실패: ' + (data.msg || data.ret), 'warning');
      }
    }
  });
});

$(document).on('click', '.remove_feed_source_btn', function (e) {
  e.preventDefault();
  var idx = $(this).data('index');
  modal_feed_sources.splice(idx, 1);
  render_modal_feed_sources();
});

$('#use_feed_filter').change(function () {
  if ($(this).is(':checked')) $('#modal_use_feed_filter_div').collapse('show');
  else $('#modal_use_feed_filter_div').collapse('hide');
});

$('#use_rss_file').change(function () {
  if ($(this).is(':checked')) {
    $('#modal_use_feed_rss_file_div').collapse('show');
    if (!$('#rss_file').val()) {
      var fname = $('#feed_name').val().trim() || 'feed';
      $('#rss_file').val(fname + '.xml');
    }
  } else {
    $('#modal_use_feed_rss_file_div').collapse('hide');
  }
});

$(document).on('click', '#feed_add_btn', function (e) {
  e.preventDefault();
  $('#feed_modal_title').text('피드(Feed) 추가');
  $('#modal_feed_id').val('-1');
  $('#feed_name').val('');
  $('#quality').val('');
  $('#filter_reject').val('');
  $('#filter_accept').val('');
  $('#filter_reject_excluding').val('');
  $('#rss_file').val('');
  $('#rss_file_path').val('');
  $('#rss_file_days').val('');
  $('#rss_file_items').val('');

  $('#modal_use_feed_filter_div').collapse('hide');
  $('#modal_use_feed_rss_file_div').collapse('hide');

  set_modal_checkbox('use_feed_filter', false);
  set_modal_checkbox('accept_all', false);
  set_modal_checkbox('use_rss_file', false);

  modal_feed_sources = [];
  render_modal_feed_sources();
  update_crawler_source_select_options();

  $('#feed_modal').modal('show');
});

$(document).on('click', '.feed_edit_btn', function (e) {
  e.preventDefault();
  var index = $(this).data('index');
  var item = current_feeds[index];

  $('#feed_modal_title').text('피드(Feed) 수정: ' + item.name);
  $('#modal_feed_id').val(item.id);
  $('#feed_name').val(item.name || '');
  $('#quality').val(item.quality || '');

  var regexp = item.regexp || {};
  var reject_str = rules_to_string(regexp.reject);
  var accept_str = rules_to_string(regexp.accept);
  var reject_ex_str = rules_to_string(regexp.reject_excluding);
  var is_accept_all = (item.accept_all === true || item.accept_all === 'True' || item.accept_all === 'true' || item.accept_all === 'yes' || item.accept_all === 'on');
  var has_filter = Boolean(reject_str || accept_str || reject_ex_str || is_accept_all);

  $('#filter_reject').val(reject_str);
  $('#filter_accept').val(accept_str);
  $('#filter_reject_excluding').val(reject_ex_str);

  $('#rss_file').val(item.rss_file || '');
  $('#rss_file_path').val(item.rss_file_path || '');
  $('#rss_file_days').val(item.rss_file_days || '');
  $('#rss_file_items').val(item.rss_file_items || '');

  set_modal_checkbox('use_feed_filter', has_filter);
  set_modal_checkbox('accept_all', is_accept_all);
  set_modal_checkbox('use_rss_file', item.use_rss_file);

  if (has_filter) $('#modal_use_feed_filter_div').collapse('show');
  else $('#modal_use_feed_filter_div').collapse('hide');

  if (item.use_rss_file) $('#modal_use_feed_rss_file_div').collapse('show');
  else $('#modal_use_feed_rss_file_div').collapse('hide');

  modal_feed_sources = item.sources ? JSON.parse(JSON.stringify(item.sources)) : [];
  render_modal_feed_sources();
  update_crawler_source_select_options();

  $('#feed_modal').modal('show');
});

$(document).on('click', '#feed_save_btn', function (e) {
  e.preventDefault();
  var formData = $('#feed_form').serialize();
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/add_feed',
    type: 'POST',
    data: formData,
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success' || data.ret === 'success_update') {
        notify('피드(Feed) 설정이 저장되었습니다.', 'success');
        $('#feed_modal').modal('hide');
        current_feeds = data.feeds || [];
        render_feeds(current_feeds);
      } else {
        notify('저장 실패: ' + (data.log || data.ret), 'danger');
      }
    }
  });
});

$(document).on('click', '.remove_feed_btn', function (e) {
  e.preventDefault();
  var target_id = $(this).data('id');
  if (!confirm('해당 피드 설정을 삭제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/remove_feed',
    type: 'POST',
    data: { target_id: target_id },
    dataType: 'json',
    success: function (data) {
      notify('피드가 삭제되었습니다.', 'success');
      current_feeds = data.feeds || [];
      render_feeds(current_feeds);
    }
  });
});

$(document).on('click', '.generate_feed_file_btn', function (e) {
  e.preventDefault();
  var target_id = $(this).data('id');
  notify('공유 XML 파일 생성 요청 중...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/generate_feed_file',
    type: 'POST',
    data: { target_id: target_id },
    dataType: 'json',
    success: function (data) {
      if (data.ret === 'success') {
        notify('XML 파일이 갱신되었습니다.', 'success');
      } else {
        notify('파일 생성 실패', 'warning');
      }
    }
  });
});

$(document).on('click', '#feed_reload_btn', function (e) {
  e.preventDefault();
  load_all_feed_data();
  notify('새로고침 완료', 'info');
});
