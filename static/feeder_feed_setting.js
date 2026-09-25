var current_feeds = [];
var current_crawlers = [];
var modal_feed_sources = [];

$(document).ready(function(){
  use_collapse("feed_make_rss_file");
  use_collapse("feed_use_proxy");
  load_all_feed_data();

  try {
    localStorage.setItem('feeder_last_feed_page', 'setting');
  } catch(err) {}

  restore_feed_subtab();
  setTimeout(restore_feed_subtab, 80);
});

function restore_feed_subtab() {
  try {
    var saved_tab = localStorage.getItem(package_name + '_' + sub + '_active_tab');
    if (saved_tab) {
      var tabElem = $('#nav-tab a[href="' + saved_tab + '"]');
      if (tabElem.length > 0 && !tabElem.hasClass('active')) {
        tabElem.tab('show');
      }
    }
  } catch(e) {}
}

$(document).on('shown.bs.tab', '#nav-tab a[data-toggle="tab"]', function(e){
  try {
    var targetTab = $(e.target).attr('href');
    if (targetTab && targetTab.startsWith('#')) {
      localStorage.setItem(package_name + '_' + sub + '_active_tab', targetTab);
    }
  } catch(err) {}
});

$('#feed_make_rss_file').change(function() { use_collapse('feed_make_rss_file'); });
$('#feed_use_proxy').change(function() { use_collapse('feed_use_proxy'); });

function load_all_feed_data() {
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/load_feeds',
    type: "POST",
    dataType: "json",
    success: function(data) {
      current_feeds = data.feeds || [];
      render_feeds(current_feeds);
    }
  });

  $.ajax({
    url: '/' + package_name + '/ajax/crawl/load_crawlers',
    type: "POST",
    dataType: "json",
    success: function(data) {
      current_crawlers = data.crawlers || [];
    }
  });
}

function render_feeds(data) {
  var tbody = $('#feed_list_tbody');
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
    if (isRssFile) {
      str += '      <button type="button" class="btn btn-outline-success generate_feed_file_btn" data-id="' + item.id + '" title="XML 파일 즉시 생성">XML 갱신</button>';
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
    if (elem.data('bs.toggle')) {
      elem.bootstrapToggle(bool_val ? 'on' : 'off');
    } else if (elem.parent().hasClass('toggle')) {
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
    str += '<tr>';
    str += '  <td><span class="badge badge-secondary">' + s.site + '</span></td>';
    str += '  <td><strong>' + s.board + '</strong></td>';
    str += '  <td>' + (s.subcat ? '<span class="badge badge-secondary">' + s.subcat + '</span>' : '<span class="text-muted">-</span>') + '</td>';
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

$(document).on('click', '#add_feed_source_btn', function(e){
  e.preventDefault();
  var rawVal = $('#feed_source_crawler_select').val();
  if (!rawVal) return;
  var parts = rawVal.split('|');
  var site = parts[0];
  var board = parts[1];
  var subcat = parts[2] || '';

  var exists = modal_feed_sources.some(function(s){
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

$(document).on('click', '.remove_feed_source_btn', function(e){
  e.preventDefault();
  var idx = $(this).data('index');
  modal_feed_sources.splice(idx, 1);
  render_modal_feed_sources();
});

$('#use_feed_filter').change(function(){
  if ($(this).is(':checked')) {
    $('#modal_use_feed_filter_div').collapse('show');
  } else {
    $('#modal_use_feed_filter_div').collapse('hide');
  }
});

$('#use_rss_file').change(function(){
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

$(document).on('click', '#feed_add_btn', function(e){
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

$(document).on('click', '.feed_edit_btn', function(e){
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

$(document).on('click', '#feed_save_btn', function(e){
  e.preventDefault();
  var formData = $('#feed_form').serialize();
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/add_feed',
    type: "POST",
    data: formData,
    dataType: "json",
    success: function(data) {
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

$(document).on('click', '.remove_feed_btn', function(e){
  e.preventDefault();
  var target_id = $(this).data('id');
  if (!confirm('해당 피드 설정을 삭제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/remove_feed',
    type: "POST",
    data: {target_id: target_id},
    dataType: "json",
    success: function(data) {
      notify('피드가 삭제되었습니다.', 'success');
      current_feeds = data.feeds || [];
      render_feeds(current_feeds);
    }
  });
});

$(document).on('click', '.generate_feed_file_btn', function(e){
  e.preventDefault();
  var target_id = $(this).data('id');
  notify('공유 XML 파일 생성 요청 중...', 'info');
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/generate_feed_file',
    type: "POST",
    data: {target_id: target_id},
    dataType: "json",
    success: function(data) {
      if (data.ret === 'success') {
        notify('XML 파일이 갱신되었습니다.', 'success');
      } else {
        notify('파일 생성 실패', 'warning');
      }
    }
  });
});

$(document).on('click', '#feed_reload_btn', function(e){
  e.preventDefault();
  load_all_feed_data();
  notify('새로고침 완료', 'info');
});
