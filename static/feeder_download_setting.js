var current_downloaders = [];
var current_profiles = [];
var current_accounts = [];
var available_feeds = [];
var modal_profile_feeds = [];
var modal_profile_chain = [];

$(document).ready(function(){
  load_all_download_config();
});

function load_all_download_config() {
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/load_downloaders',
    type: "POST",
    dataType: "json",
    success: function(data) {
      current_downloaders = data.downloaders || [];
      render_downloaders(current_downloaders);
    }
  });

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/load_profiles',
    type: "POST",
    dataType: "json",
    success: function(data) {
      current_profiles = data.profiles || [];
      available_feeds = data.feeds || [];
      render_profiles(current_profiles);
    }
  });

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/load_accounts',
    type: "POST",
    dataType: "json",
    success: function(data) {
      current_accounts = data.accounts || [];
      render_accounts(current_accounts, data.stats || {});
    }
  });
}

function render_downloaders(data) {
  var tbody = $('#downloader_list_tbody');
  if (!data || data.length === 0) {
    tbody.html('<tr><td colspan="5" class="py-4 text-muted">등록된 다운로더 엔진이 없습니다.</td></tr>');
    return;
  }
  var str = '';
  for (var i = 0; i < data.length; i++) {
    var item = data[i];
    var isEnabled = (item.enabled === true || item.enabled === 'True' || item.enabled === 'on');
    var statusBadge = isEnabled ? '<span class="badge badge-success">활성</span>' : '<span class="badge badge-secondary">중지</span>';
    var timeoutStr = (item.stalled_timeout_hours || 24) + '시간 타임아웃';

    var connDetail = '';
    if (item.engine_type === 'alldebrid') {
      connDetail = '리모트: <code>' + (item.remote_name || 'ad') + ':' + (item.rclone_base_path || 'magnets') + '</code>';
    } else if (item.engine_type === '115') {
      connDetail = 'CD2: ' + item.cd2_addr + ':' + item.cd2_port + ' | 마운트: ' + (item.cd2_mount_path || '-');
    } else if (item.engine_type === 'qbittorrent') {
      connDetail = 'URL: ' + item.url + ' | 저장: ' + (item.save_path || '-');
    }

    str += '<tr>';
    str += '  <td class="font-weight-bold">' + item.name + '</td>';
    str += '  <td><span class="badge badge-info">' + item.engine_type + '</span></td>';
    str += '  <td>' + statusBadge + '<br><small class="text-muted">' + timeoutStr + '</small></td>';
    str += '  <td class="text-left small">' + connDetail + '</td>';
    str += '  <td>';
    str += '    <div class="btn-group btn-group-sm">';
    str += '      <button type="button" class="btn btn-outline-primary edit_dl_btn" data-index="' + i + '">수정</button>';
    str += '      <button type="button" class="btn btn-outline-danger delete_dl_btn" data-name="' + item.name + '">삭제</button>';
    str += '    </div>';
    str += '  </td>';
    str += '</tr>';
  }
  tbody.html(str);
}

$('#dl_engine_type').change(function(){
  var val = $(this).val();
  $('.dl-engine-fields').hide();
  $('#dl_fields_' + val).show();
});

$(document).on('click', '#downloader_add_btn', function(e){
  e.preventDefault();
  $('#downloader_modal_title').text('다운로더 엔진 추가');
  $('#downloader_mode').val('add');
  $('#dl_name').val('').prop('readonly', false);
  $('#dl_engine_type').val('alldebrid').trigger('change');
  $('#dl_enabled').prop('checked', true);
  $('#dl_stalled_timeout_hours').val('24');
  $('#dl_ad_apikey').val('');
  $('#downloader_modal').modal('show');
});

$(document).on('click', '.edit_dl_btn', function(e){
  e.preventDefault();
  var idx = $(this).data('index');
  var item = current_downloaders[idx];

  $('#downloader_modal_title').text('다운로더 엔진 수정: ' + item.name);
  $('#downloader_mode').val('edit');
  $('#dl_name').val(item.name).prop('readonly', true);
  $('#dl_engine_type').val(item.engine_type).trigger('change');
  $('#dl_enabled').prop('checked', item.enabled);
  $('#dl_stalled_timeout_hours').val(item.stalled_timeout_hours || 24);

  if (item.engine_type === 'alldebrid') {
    $('#dl_ad_apikey').val(item.apikey || '');
    $('#dl_ad_remote').val(item.remote_name || 'ad');
    $('#dl_ad_base_path').val(item.rclone_base_path || 'magnets');
  } else if (item.engine_type === '115') {
    $('#dl_115_addr').val(item.cd2_addr || '127.0.0.1');
    $('#dl_115_port').val(item.cd2_port || 19798);
    $('#dl_115_token').val(item.cd2_token || '');
    $('#dl_115_vpath').val(item.cd2_virtual_path || '115open/云下载');
    $('#dl_115_mpath').val(item.cd2_mount_path || '');
    $('#dl_115_cpath').val(item.cd2_completed_path || '');
  } else if (item.engine_type === 'qbittorrent') {
    $('#dl_qb_url').val(item.url || 'http://127.0.0.1:8080');
    $('#dl_qb_user').val(item.username || 'admin');
    $('#dl_qb_pass').val(item.password || '');
    $('#dl_qb_save_path').val(item.save_path || '');
  }
  $('#downloader_modal').modal('show');
});

$(document).on('click', '#downloader_save_btn', function(e){
  e.preventDefault();
  var name = $('#dl_name').val().trim();
  var e_type = $('#dl_engine_type').val();
  if (!name) { notify('식별명을 입력하세요.', 'warning'); return; }

  var cfg = {
    name: name,
    engine_type: e_type,
    enabled: $('#dl_enabled').is(':checked'),
    stalled_timeout_hours: parseInt($('#dl_stalled_timeout_hours').val()) || 24
  };

  if (e_type === 'alldebrid') {
    cfg.apikey = $('#dl_ad_apikey').val().trim();
    cfg.remote_name = $('#dl_ad_remote').val().trim() || 'ad';
    cfg.rclone_base_path = $('#dl_ad_base_path').val().trim() || 'magnets';
  } else if (e_type === '115') {
    cfg.cd2_addr = $('#dl_115_addr').val().trim();
    cfg.cd2_port = parseInt($('#dl_115_port').val()) || 19798;
    cfg.cd2_token = $('#dl_115_token').val().trim();
    cfg.cd2_virtual_path = $('#dl_115_vpath').val().trim();
    cfg.cd2_mount_path = $('#dl_115_mpath').val().trim();
    cfg.cd2_completed_path = $('#dl_115_cpath').val().trim();
  } else if (e_type === 'qbittorrent') {
    cfg.url = $('#dl_qb_url').val().trim();
    cfg.username = $('#dl_qb_user').val().trim();
    cfg.password = $('#dl_qb_pass').val();
    cfg.save_path = $('#dl_qb_save_path').val().trim();
  }

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/save_downloader',
    type: "POST",
    data: {downloader_json: JSON.stringify(cfg)},
    dataType: "json",
    success: function(data) {
      notify('다운로더 설정이 저장되었습니다.', 'success');
      $('#downloader_modal').modal('hide');
      current_downloaders = data.downloaders || [];
      render_downloaders(current_downloaders);
    }
  });
});

$(document).on('click', '.delete_dl_btn', function(e){
  e.preventDefault();
  var target_name = $(this).data('name');
  if (!confirm('[' + target_name + '] 다운로더를 삭제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/delete_downloader',
    type: "POST",
    data: {name: target_name},
    dataType: "json",
    success: function(data) {
      notify('삭제되었습니다.', 'success');
      current_downloaders = data.downloaders || [];
      render_downloaders(current_downloaders);
    }
  });
});

// 프로필 목적지 유형에 따른 폼 필드 토글
$('#profile_dest_type').change(function(){
  var val = $(this).val();
  $('.dest-type-fields').hide();
  if (val === 'gdrive_rotation') {
    $('#profile_fields_gdrive_rotation').show();
  } else if (val === 'rclone_simple') {
    $('#profile_fields_rclone_simple').show();
  }
});
function render_profiles(data) {
  var tbody = $('#profile_list_tbody');
  if (!data || data.length === 0) {
    tbody.html('<tr><td colspan="5" class="py-4 text-muted">등록된 다운로드 프로필이 없습니다.</td></tr>');
    return;
  }
  var str = '';
  for (var i = 0; i < data.length; i++) {
    var p = data[i];
    var feedsHtml = (p.feeds || []).map(function(f){ return '<span class="badge badge-info mr-1">' + f + '</span>'; }).join('');
    var chainHtml = (p.priority_chain || []).map(function(c, idx){
      return '<span class="badge badge-secondary mr-1">' + (idx + 1) + '. ' + c + '</span>';
    }).join(' <i class="fa fa-arrow-right text-muted small mr-1"></i> ');

    var dest = p.destination || {};
    var destInfo = '<span class="badge badge-dark">' + (dest.type || 'local') + '</span>';
    if (dest.type === 'rclone_simple' && dest.remote_path) {
      destInfo += '<br><small class="text-muted">' + dest.remote_path + '</small>';
    }

    str += '<tr>';
    str += '  <td class="font-weight-bold">' + p.name + '</td>';
    str += '  <td>' + feedsHtml + '</td>';
    str += '  <td>' + chainHtml + '</td>';
    str += '  <td>' + destInfo + '</td>';
    str += '  <td>';
    str += '    <div class="btn-group btn-group-sm">';
    str += '      <button type="button" class="btn btn-outline-primary edit_profile_btn" data-index="' + i + '">수정</button>';
    str += '      <button type="button" class="btn btn-outline-danger delete_profile_btn" data-name="' + p.name + '">삭제</button>';
    str += '    </div>';
    str += '  </td>';
    str += '</tr>';
  }
  tbody.html(str);
}

// 모달 내 피드 및 다운로더 선택 드롭다운 갱신
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

// 선택된 피드 뱃지 렌더링
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

// 우선순위 체인 테이블 렌더링
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
    str += '      <button type="button" class="btn btn-xs btn-outline-secondary chain-move-btn" data-dir="up" data-index="' + i + '" ' + (i === 0 ? 'disabled' : '') + '>▲</button>';
    str += '      <button type="button" class="btn btn-xs btn-outline-secondary chain-move-btn" data-dir="down" data-index="' + i + '" ' + (i === modal_profile_chain.length - 1 ? 'disabled' : '') + '>▼</button>';
    str += '      <button type="button" class="btn btn-xs btn-outline-danger chain-remove-btn" data-index="' + i + '">제외</button>';
    str += '    </div>';
    str += '  </td>';
    str += '</tr>';
    tbody.append(str);
  }
}

// 피드 추가 버튼 클릭
$(document).on('click', '#btn_add_profile_feed', function(e){
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

// 전체 피드(*) 원클릭 추가
$(document).on('click', '#btn_add_all_feed', function(e){
  e.preventDefault();
  if (modal_profile_feeds.indexOf('*') !== -1) return;
  modal_profile_feeds.push('*');
  render_selected_feeds();
});

// 선택된 피드 뱃지 삭제
$(document).on('click', '.remove-profile-feed-btn', function(e){
  e.preventDefault();
  var target = $(this).data('feed');
  modal_profile_feeds = modal_profile_feeds.filter(function(f){ return f !== target; });
  render_selected_feeds();
});

// 체인 엔진 추가 버튼 클릭
$(document).on('click', '#btn_add_profile_engine', function(e){
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

// 체인 순서 위/아래 이동 및 제외
$(document).on('click', '.chain-move-btn', function(e){
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

$(document).on('click', '.chain-remove-btn', function(e){
  e.preventDefault();
  var idx = parseInt($(this).data('index'));
  modal_profile_chain.splice(idx, 1);
  render_chain_table();
});

// 프로필 추가 모달 열기
$(document).on('click', '#profile_add_btn', function(e){
  e.preventDefault();
  $('#profile_modal_title').text('다운로드 프로필 추가');
  $('#profile_mode').val('add');
  $('#profile_name').val('').prop('readonly', false);
  $('#profile_append_hash_on_conflict').prop('checked', true);
  $('#profile_dest_type').val('gdrive_rotation').trigger('change');
  $('#profile_gdrive_upload_path').val('incoming/default');
  $('#profile_gdrive_complete_path').val('uploads/default');
  $('#profile_gdrive_remote_id').val('');
  $('#profile_simple_remote_path').val('');

  modal_profile_feeds = ['*'];
  modal_profile_chain = [];

  update_profile_modal_dropdowns();
  render_selected_feeds();
  render_chain_table();
  $('#profile_modal').modal('show');
});

// 프로필 수정 모달 열기
$(document).on('click', '.edit_profile_btn', function(e){
  e.preventDefault();
  var idx = $(this).data('index');
  var p = current_profiles[idx];

  $('#profile_modal_title').text('다운로드 프로필 수정: ' + p.name);
  $('#profile_mode').val('edit');
  $('#profile_name').val(p.name).prop('readonly', true);
  $('#profile_append_hash_on_conflict').prop('checked', p.append_hash_on_conflict !== false);

  var dest = p.destination || {};
  $('#profile_dest_type').val(dest.type || 'gdrive_rotation').trigger('change');
  $('#profile_gdrive_upload_path').val(dest.upload_path || 'incoming/default');
  $('#profile_gdrive_complete_path').val(dest.complete_path || 'uploads/default');
  $('#profile_gdrive_remote_id').val(dest.shared_drive_id || '');
  $('#profile_simple_remote_path').val(dest.remote_path || '');

  modal_profile_feeds = (p.feeds && p.feeds.length > 0) ? JSON.parse(JSON.stringify(p.feeds)) : ['*'];
  modal_profile_chain = (p.priority_chain && p.priority_chain.length > 0) ? JSON.parse(JSON.stringify(p.priority_chain)) : [];

  update_profile_modal_dropdowns();
  render_selected_feeds();
  render_chain_table();
  $('#profile_modal').modal('show');
});

// 프로필 저장 버튼 클릭
$(document).on('click', '#profile_save_btn', function(e){
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
  var destObj = {type: destType};
  if (destType === 'gdrive_rotation') {
    destObj.upload_path = $('#profile_gdrive_upload_path').val().trim();
    destObj.complete_path = $('#profile_gdrive_complete_path').val().trim();
    destObj.shared_drive_id = $('#profile_gdrive_remote_id').val().trim();
  } else if (destType === 'rclone_simple') {
    destObj.remote_path = $('#profile_simple_remote_path').val().trim();
  }

  var profile_obj = {
    name: name,
    feeds: modal_profile_feeds,
    priority_chain: modal_profile_chain,
    append_hash_on_conflict: $('#profile_append_hash_on_conflict').is(':checked'),
    destination: destObj
  };

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/save_profile',
    type: "POST",
    data: {profile_json: JSON.stringify(profile_obj)},
    dataType: "json",
    success: function(data) {
      notify('프로필이 저장되었습니다.', 'success');
      $('#profile_modal').modal('hide');
      current_profiles = data.profiles || [];
      render_profiles(current_profiles);
    }
  });
});

$(document).on('click', '.delete_profile_btn', function(e){
  e.preventDefault();
  var target_name = $(this).data('name');
  if (!confirm('[' + target_name + '] 프로필을 삭제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/delete_profile',
    type: "POST",
    data: {name: target_name},
    dataType: "json",
    success: function(data) {
      notify('삭제되었습니다.', 'success');
      current_profiles = data.profiles || [];
      render_profiles(current_profiles);
    }
  });
});

function render_accounts(accounts, stats) {
  var tbody = $('#gdrive_account_list_tbody');
  if (!accounts || accounts.length === 0) {
    tbody.html('<tr><td colspan="4" class="py-4 text-muted">등록된 구글 드라이브 계정이 없습니다.</td></tr>');
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
    str += '  <td>' + gb + ' GB (' + mb + ' MB)</td>';
    str += '  <td><button type="button" class="btn btn-xs btn-outline-danger remove_acc_btn" data-index="' + i + '">제외</button></td>';
    str += '</tr>';
  }
  tbody.html(str);
}

$(document).on('click', '#gdrive_batch_modal_btn', function(e){
  e.preventDefault();
  $('#gdrive_batch_textarea').val('');
  $('#gdrive_batch_modal').modal('show');
});

$(document).on('click', '#gdrive_batch_save_btn', function(e){
  e.preventDefault();
  var text = $('#gdrive_batch_textarea').val().trim();
  if (!text) { notify('입력된 계정 정보가 없습니다.', 'warning'); return; }

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/batch_add_accounts',
    type: "POST",
    data: {batch_text: text},
    dataType: "json",
    success: function(data) {
      notify(data.added_count + '개 계정이 일괄 등록되었습니다.', 'success');
      $('#gdrive_batch_modal').modal('hide');
      current_accounts = data.accounts || [];
      render_accounts(current_accounts, {});
    }
  });
});

$(document).on('click', '.remove_acc_btn', function(e){
  e.preventDefault();
  var idx = $(this).data('index');
  current_accounts.splice(idx, 1);
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/save_accounts',
    type: "POST",
    data: {accounts_json: JSON.stringify(current_accounts)},
    dataType: "json",
    success: function(data) {
      notify('계정이 삭제되었습니다.', 'info');
      current_accounts = data.accounts || [];
      render_accounts(current_accounts, {});
    }
  });
});

$(document).on('click', '#gdrive_reset_blocks_btn', function(e){
  e.preventDefault();
  if (!confirm('모든 구글 드라이브 계정의 차단 상태를 즉시 해제하시겠습니까?')) return;
  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/reset_blocked_accounts',
    type: "POST",
    dataType: "json",
    success: function(data) {
      notify('모든 계정 차단이 해제되었습니다.', 'success');
      render_accounts(current_accounts, data.stats || {});
    }
  });
});

$(document).on('click', '#history_import_modal_btn', function(e){
  e.preventDefault();
  $('#import_text_content').val('');
  $('#history_import_modal').modal('show');
});

// 텍스트 마그넷 목록 임포트
$(document).on('click', '#btn_run_import_text', function(e){
  e.preventDefault();
  var text = $('#import_text_content').val().trim();
  if (!text) {
    notify('임포트할 마그넷 목록을 입력하세요.', 'warning');
    return;
  }
  notify('마그넷 목록 임포트 중...', 'info');

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/import_history_text',
    type: "POST",
    data: {import_text: text},
    dataType: "json",
    success: function(data) {
      if (data.ret === 'success') {
        notify('이력 등록 완료: ' + data.added + '건 추가 (중복 ' + data.skipped + '건 스킵)', 'success');
        $('#history_import_modal').modal('hide');
      } else {
        notify(data.msg || '임포트 실패', 'danger');
      }
    }
  });
});

// SQLite DB 파일 통째 임포트
$(document).on('click', '#btn_run_import_db', function(e){
  e.preventDefault();
  var dbPath = $('#import_db_path').val().trim();
  if (!dbPath) {
    notify('SQLite DB 파일 경로를 입력하세요.', 'warning');
    return;
  }

  var formData = {
    db_path: dbPath,
    table_name: $('#import_table_name').val().trim(),
    magnet_col: $('#import_magnet_col').val().trim(),
    title_col: $('#import_title_col').val().trim(),
    file_name_col: $('#import_file_name_col').val().trim(),
    where_clause: $('#import_where_clause').val().trim()
  };

  notify('기존 DB 파일 이력 흡수 중 (잠시 기다려주세요)...', 'info');

  $.ajax({
    url: '/' + package_name + '/ajax/' + sub + '/import_history_db',
    type: "POST",
    data: formData,
    dataType: "json",
    success: function(data) {
      if (data.ret === 'success') {
        notify('DB 이력 흡수 완료: 총 ' + data.added + '건이 완료 상태로 등록되었습니다 (중복 ' + data.skipped + '건 스킵).', 'success');
        $('#history_import_modal').modal('hide');
      } else {
        notify('DB 임포트 실패: ' + (data.msg || '알 수 없는 오류'), 'danger');
      }
    }
  });
});

