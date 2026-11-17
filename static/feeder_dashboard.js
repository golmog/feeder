// 데이터 원천별 상태를 보존한다. 부분 응답은 전체 큐를 대체하지 않는다.
function queue_sort_progress(value) {
  if ((typeof value !== 'number' && typeof value !== 'string') || String(value).trim() === '') return null;
  var progress = Number(value);
  return Number.isFinite(progress) && progress >= 0 && progress <= 100 ? progress : null;
}

function FeederQueueState() {
  this.dbItemsById = new Map();
  this.engineItemsByKey = new Map();
  this.loadedPages = [];
  this.engineSnapshots = new Map();
  this.progress = new Map();
  this.lastProgress = new Map();
  this.counts = {};
  this.requestGeneration = 0;
  this.dbReady = false;
  this.total = 0;
  this.dbTotal = 0;
  this.nextCursor = null;
  this.dbAt = 0;
  this.activeStage = undefined;
  this.stageAt = 0;
  this.queueTime = null;
  this.queueTimeAt = 0;
  this.orphanTotal = 0;
  this.unverifiedTotal = 0;
}
FeederQueueState.prototype.mergeDB = function (data, generation, page) {
  if (generation !== this.requestGeneration || !data || data.success !== true || !Array.isArray(data.items)) return false;
  if (!page && data.observed_at < this.dbAt) return false;
  if (!page) this.dbAt = data.observed_at;
  if (data.observed_at >= this.queueTimeAt && typeof data.queue_time === 'string') {
    this.queueTime = data.queue_time;
    this.queueTimeAt = data.observed_at;
  }
  // 늦게 도착한 페이지가 최신 전역 단계 컨텍스트를 되돌리지 않게 한다.
  if (data.observed_at >= this.stageAt &&
      [null, 'uploading', 'local_staging', 'relay_transferring'].indexOf(data.active_stage) >= 0) {
    this.activeStage = data.active_stage;
    this.stageAt = data.observed_at;
  }
  if (Array.isArray(data.auto_disabled_engines) && typeof document !== 'undefined') {
    var warnings = $('#engine_auth_warning_items').empty();
    data.auto_disabled_engines.forEach(function (engine) {
      $('<li>').text(engine.name + ' · ' + engine.reason + ' · ' + engine.at).appendTo(warnings);
    });
    $('#engine_auth_warning').toggle(data.auto_disabled_engines.length > 0);
  }
  var self = this;
  data.items.forEach(function (item) {
    if (String(item.row_key || '').startsWith('engine:')) {
      self.engineItemsByKey.set(item.row_key, item);
      return;
    }
    var old = self.dbItemsById.get(String(item.id));
    if (!old || (item.row_version || '') >= (old.row_version || '')) self.dbItemsById.set(String(item.id), item);
  });
  (data.removed_ids || []).forEach(function (id) {
    self.dbItemsById.delete(String(id)); self.progress.delete(String(id)); self.lastProgress.delete(String(id));
  });
  (data.removed_keys || []).forEach(function (key) { self.engineItemsByKey.delete(key); });
  this.loadedPages.push(data.items.map(function (item) { return item.id; }));
  if (this.loadedPages.length > 100) this.loadedPages.shift();
  if (data.counts) this.counts = data.counts;
  this.total = data.total;
  this.dbTotal = data.db_total === undefined ? data.total : data.db_total;
  this.orphanTotal = data.orphan_total || 0;
  this.unverifiedTotal = data.unverified_total || 0;
  this.nextCursor = data.next_cursor;
  this.dbReady = true;
  return true;
};
FeederQueueState.prototype.mergeEngines = function (data, generation) {
  if (generation !== this.requestGeneration || !data || data.success !== true || !Array.isArray(data.engines)) return false;
  var self = this, enabled = new Set();
  data.engines.forEach(function (snapshot) {
    enabled.add(snapshot.engine_key);
    var old = self.engineSnapshots.get(snapshot.engine_key);
    if (old && old.observed_at > data.observed_at) return;
    var items = old && old.config_revision === snapshot.config_revision ? new Map(old.items) : new Map();
    if (snapshot.match_verified === true && snapshot.complete && snapshot.state === 'ready') items.clear();
    var received = new Set(snapshot.items.map(function (item) { return String(item.task_id || item.hash); }));
    // 이번 목록에서 빠진 보존 항목은 이전 객체·실제 수신 시각을 유지하며 지연 정보로 구별한다.
    items.forEach(function (item, key) {
      if (!received.has(key)) items.set(key, Object.assign({}, item, {stale:true}));
    });
    snapshot.items.forEach(function (item) {
      var key = String(item.task_id || item.hash), previous = items.get(key);
      var sameHash = previous && /^[0-9a-f]{40}$/i.test(item.hash || '') && previous.hash === item.hash;
      var verified = snapshot.match_verified === true && item.match_verified !== false;
      items.set(key, Object.assign({}, item, {
        match_verified: verified,
        db_id: verified ? item.db_id : (sameHash && previous.db_id),
        registered: verified ? item.registered : (sameHash && previous.registered)
      }));
    });
    items.forEach(function (item, key) { if (data.observed_at - item.last_seen_at > 300) items.delete(key); });
    self.engineSnapshots.set(snapshot.engine_key, Object.assign({}, snapshot, {items: items, observed_at: data.observed_at}));
  });
  this.engineSnapshots.forEach(function (snapshot, key) { if (!enabled.has(key)) self.engineSnapshots.delete(key); });
  if (data.orphan_total !== undefined) this.orphanTotal = data.orphan_total;
  if (data.unverified_total !== undefined) this.unverifiedTotal = data.unverified_total;
  this.total = this.dbTotal + this.orphanTotal + this.unverifiedTotal;
  return true;
};
FeederQueueState.prototype.mergeProgress = function (data, generation) {
  if (generation !== this.requestGeneration || !data || !data.success) return false;
  var self = this;
  (data.items || []).forEach(function (item) {
    var old = self.progress.get(String(item.id));
    if (!old || old.observed_at <= data.observed_at) self.progress.set(String(item.id), Object.assign({}, item, {observed_at: data.observed_at}));
  });
  return true;
};
FeederQueueState.prototype.view = function (now) {
  var self = this, rows = new Map(), remoteLive = new Map(), downloadSpeeds = [];
  this.dbItemsById.forEach(function (item, id) {
    var row = Object.assign({}, item), live = self.progress.get(id);
    row.eta = ''; row.transfer_active = false;
    if (row.status === 'downloading') row.engine_stale = true;
    if (live && live.status === row.status && live.row_version === row.row_version) {
      Object.assign(row, live);
      if (!Number.isFinite(live.observed_at) || now - live.observed_at > 15 || live.progress_stale) {
        row.download_speed = 0; row.speed_str = '갱신 지연'; row.progress_stale = true;
        row.eta = ''; row.transfer_active = false;
      } else if (!live.transfer_active) row.eta = '';
    }
    row.sort_progress = queue_sort_progress(row.progress);
    rows.set(id, row);
  });
  this.engineSnapshots.forEach(function (snapshot) {
    var speed = 0, speedKnown = false;
    snapshot.items.forEach(function (item, identity) {
      if (now - item.last_seen_at > 300) return;
      remoteLive.set(item.row_key || ('engine:' + snapshot.engine_key + ':' + identity), {snapshot:snapshot, item:item});
      var stale = item.stale || snapshot.state === 'error' || snapshot.error_type || now - item.last_seen_at > 60;
      // DB 행의 로딩·매칭 여부와 관계없이 수신한 엔진 작업에서 속도를 합산한다.
      if (!stale && item.status === 'downloading' && typeof item.download_speed === 'number' &&
          Number.isFinite(item.download_speed) && item.download_speed >= 0) {
        speed += item.download_speed;
        speedKnown = true;
      }
      if (item.db_id) {
        var row = rows.get(String(item.db_id));
        var matches = row && row.current_engine_name === snapshot.engine_name &&
          /^[0-9a-f]{40}$/i.test(item.hash || '') && row.infohash &&
          String(row.infohash).toLowerCase() === String(item.hash).toLowerCase();
        var preferred = row && snapshot.items.get(String(row.engine_task_id));
        if (matches && preferred && preferred !== item && preferred.hash === item.hash) matches = false;
        if (matches && row.status === 'downloading') {
          if (!row.file_size) row.file_size = item.file_size;
          row.progress = item.progress || (item.file_size > 0 ? item.downloaded_bytes / item.file_size * 100 : 0);
          row.downloaded_bytes = item.downloaded_bytes;
          row.download_speed = stale ? 0 : item.download_speed;
          row.engine_stale = !!stale;
          var confirmed = queue_sort_progress(item.sort_progress === undefined ? item.progress : item.sort_progress);
          if (item.sort_progress === undefined && confirmed === null && item.file_size > 0 && typeof item.downloaded_bytes === 'number')
            confirmed = queue_sort_progress(item.downloaded_bytes / item.file_size * 100);
          if (confirmed !== null) row.sort_progress = confirmed;
        }
      }
    });
    if (snapshot.download_speed_total !== undefined) {
      speedKnown = Number.isFinite(snapshot.download_speed_total);
      speed = speedKnown ? snapshot.download_speed_total : 0;
    }
    if (speedKnown) downloadSpeeds.push({engine_name:snapshot.engine_name, speed:speed});
  });
  rows.forEach(function (row, id) {
    // 같은 작업·상태·행 버전에서만 마지막 유효 값을 보존한다. 속도/ETA는 보존하지 않는다.
    var context = JSON.stringify([row.status, row.row_version, row.current_engine_name, row.engine_task_id, row.infohash]);
    var previous = self.lastProgress.get(id);
    if (previous && previous.context !== context) { self.lastProgress.delete(id); previous = null; }
    if (row.sort_progress !== null) self.lastProgress.set(id, {context:context, value:row.sort_progress});
    else if (previous) row.sort_progress = previous.value;
  });
  this.engineItemsByKey.forEach(function (item, key) {
    var live = remoteLive.get(key), row = Object.assign({}, item);
    if (live) {
      if (live.item.registered) return;
      row.status = live.snapshot.match_verified === true && live.item.match_verified === true ? 'engine_unmanaged' : 'engine_unverified';
      row.title = row.file_name = live.item.filename;
      row.file_size = live.item.file_size; row.local_path = live.item.source_path;
      row.raw_status = live.item.status; row.last_seen_at = live.item.last_seen_at;
      row.engine_stale = !!(live.item.stale || live.snapshot.error_type || now - row.last_seen_at > 60);
    } else row.engine_stale = !!(row.engine_stale || now - row.last_seen_at > 60);
    if (now - row.last_seen_at <= 300) rows.set(key, row);
  });
  return {rows: Array.from(rows.values()), orphanTotal: this.orphanTotal,
    unverifiedTotal: this.unverifiedTotal, downloadSpeeds:downloadSpeeds};
};

function FeederDashboard() {
  this.state = new FeederQueueState();
  this.closed = false;
  this.requests = new Set();
  this.busy = {};
  this.last = {};
  this.lastRender = '';
  this.engineStatusSignature = null;
  this.dbError = false;
  this.uploadHasCounts = false;
  this.uploadPendingSince = null;
  this.uploadFailures = 0;
  this.uploadPollDelay = 5000;
  this.pageState = 'idle';
  this.consumedCursors = new Set();
  this.awaitingBootstrap = true;
  this.expiredResynced = false;
  this.observer = null;
  this.autoUnavailable = false;
  this.autoVisible = false;
  this.lastScrollY = window.scrollY || 0;
  this.autoReady = false;
  this.prefetchMargin = 300;
}
FeederDashboard.prototype.request = function (kind, data, done, fail) {
  if (this.closed || this.busy[kind]) return;
  var self = this, generation = this.state.requestGeneration;
  this.busy[kind] = true; this.last[kind] = Date.now();
  var request = $.ajax({url: '/' + package_name + '/ajax/' + sub + '/' + kind, type: 'POST',
    data: data || {}, dataType: 'json', global: false, timeout: 10000});
  this.requests.add(request);
  request.done(function (result) {
    if (self.closed || generation !== self.state.requestGeneration) return;
    if (!result || result.success !== true) { if (fail) fail(result || {}); return; }
    done(result, generation);
  }).fail(function () {
    if (!self.closed && generation === self.state.requestGeneration && fail) fail({});
  }).always(function () {
    self.requests.delete(request);
    if (generation === self.state.requestGeneration) {
      self.busy[kind] = false;
      self.updatePageControls();
      self.tryAutoLoad();
    }
  });
};
FeederDashboard.prototype.bootstrap = function () {
  if (this.busy.queue_page) return;
  var self = this, known = {};
  this.state.dbItemsById.forEach(function (item, id) { known['db:' + id] = item.row_version; });
  this.state.engineItemsByKey.forEach(function (item, key) { known[key] = item.row_version; });
  this.request('queue_bootstrap', {known: JSON.stringify(known)}, function (data, generation) {
    if (!self.state.mergeDB(data, generation, false)) return;
    var initial = self.awaitingBootstrap;
    self.consumedCursors.clear();
    self.dbError = false;
    self.awaitingBootstrap = false;
    $('#queue_db_state').text('DB 최신 확인').attr('data-state', 'ready');
    $('#active_queue_tbody').attr('aria-busy', 'false');
    if (data.counts) self.renderCounts(data.counts);
    if (data.counts_error) $('#queue_counts_state').text('통계 갱신 지연 · 기존 값 유지').attr('data-state', 'stale');
    self.render();
    self.lastScrollY = window.scrollY || 0;
    if (initial) self.waitForPagePaint();
  }, function () {
    self.dbError = true;
    $('#queue_db_state').text('DB 불러오기 실패 · 새로고침으로 재시도').attr('data-state', 'error');
    $('#queue_counts_state').text('DB 통계 갱신 지연 · 재시도 중').attr('data-state', 'stale');
    $('#active_queue_tbody').attr('aria-busy', 'false');
    if (!self.state.dbReady && !self.state.view(Date.now()/1000).rows.length)
      $('#active_queue_tbody').html('<tr><td colspan="3" class="py-4 text-warning">DB 큐 불러오기 실패 · 수동 새로고침으로 재시도할 수 있습니다.</td></tr>');
    $('#queue_db_wait_row td').text('DB 큐 불러오기 실패 · 수동 새로고침으로 재시도');
  });
};
FeederDashboard.prototype.renderCounts = function (counts) {
  ['pending', 'downloading', 'completed', 'failed'].forEach(function (key) {
    if (counts[key] !== undefined) $('#stat_' + key).text(counts[key]);
  });
  $('#queue_counts_state').text('DB 통계 최신 확인').attr('data-state', 'ready');
  ['download', 'upload'].forEach(function (kind) {
    if (counts['total_' + kind + '_24h'] !== undefined)
      $('#queue_daily_' + kind + '_badge').text('24시간 ' + (kind === 'download' ? '완료: ' : '업로드: ') + format_bytes(counts['total_' + kind + '_24h']));
  });
};
FeederDashboard.prototype.more = function (automatic) {
  if (this.closed || !this.autoReady || !this.state.dbReady || !this.state.nextCursor || this.busy.queue_bootstrap || this.busy.queue_page) return;
  var self = this, cursor = this.state.nextCursor;
  if (this.consumedCursors.has(cursor)) { this.pageState = 'error'; this.updatePageControls(); return; }
  this.lastScrollY = window.scrollY || 0;
  this.autoReady = false;
  if (!automatic) this.expiredResynced = false;
  this.pageState = 'loading';
  this.updatePageControls();
  this.request('queue_page', {cursor: cursor}, function (data, generation) {
    try {
      if (!self.state.mergeDB(data, generation, true)) throw new Error('페이지 응답 형식 오류');
      self.consumedCursors.add(cursor);
      self.pageState = data.next_cursor === cursor ? 'error' : 'idle';
      self.render();
      self.lastScrollY = window.scrollY || 0;
      self.waitForPagePaint();
    } catch (error) {
      self.autoReady = true;
      self.pageState = 'error';
      self.lastRender = '';
      self.updatePageControls();
    }
  }, function (data) {
    self.autoReady = true;
    self.pageState = 'error';
    self.updatePageControls();
    if (data.error === 'snapshot_expired' && !self.expiredResynced) {
      self.expiredResynced = true;
      self.refresh(true);
    }
  });
};
FeederDashboard.prototype.waitForPagePaint = function () {
  var self = this, generation = this.state.requestGeneration;
  this.autoReady = false;
  var frame = window.requestAnimationFrame ? window.requestAnimationFrame.bind(window) : function (done) { return setTimeout(done, 16); };
  frame(function () { frame(function () {
    if (self.closed || generation !== self.state.requestGeneration) return;
    self.lastScrollY = window.scrollY || 0;
    self.autoReady = true;
    self.updatePageControls();
  }); });
};
FeederDashboard.prototype.disconnectAutoLoad = function () {
  if (this.observer) this.observer.disconnect();
  this.observer = null;
  this.autoVisible = false;
  this.lastScrollY = window.scrollY || 0;
};
FeederDashboard.prototype.updatePageControls = function () {
  var hasDB = this.state.dbReady && !!this.state.nextCursor;
  if (!hasDB) this.disconnectAutoLoad();
  else if (!this.closed && !this.awaitingBootstrap && !this.observer && !this.autoUnavailable) {
    var sentinel = document.getElementById && document.getElementById('queue_load_sentinel');
    if (!sentinel || typeof window.IntersectionObserver !== 'function') this.autoUnavailable = true;
    else {
      var self = this, generation = this.state.requestGeneration;
      try {
        var observer = new window.IntersectionObserver(function (entries) {
          if (self.closed || self.observer !== observer || generation !== self.state.requestGeneration) return;
          entries.forEach(function (entry) {
            if (entry.target !== sentinel) return;
            self.autoVisible = entry.isIntersecting;
          });
          self.updatePageControls();
          self.tryAutoLoad();
        }, {root:null, rootMargin:'0px 0px ' + this.prefetchMargin + 'px 0px', threshold:0});
        this.observer = observer;
        observer.observe(sentinel);
      } catch (err) {
        this.disconnectAutoLoad();
        this.autoUnavailable = true;
      }
    }
  }
  var scrollRoot = document.scrollingElement || document.documentElement;
  var fallback = this.autoUnavailable || !this.observer || this.pageState === 'error' ||
    (scrollRoot && Math.ceil((window.scrollY || 0) + window.innerHeight) >= scrollRoot.scrollHeight);
  var loading = this.pageState === 'loading' || !!this.busy.queue_page || !this.autoReady;
  var pageLoading = !this.closed && this.pageState === 'loading';
  $('#queue_page_loading').toggle(pageLoading).attr('aria-hidden', pageLoading ? 'false' : 'true');
  $('#queue_more_btn').toggle(hasDB && fallback).prop('disabled', loading || !!this.busy.queue_bootstrap)
    .text(this.pageState === 'error' ? '다시 시도' : loading ? '로딩 중' : '더 보기');
  var message = loading ? '로딩 중' : this.pageState === 'error' ? '추가 로딩 실패 · 다시 시도해주세요.' :
    !this.state.dbReady ? '초기 작업 확인 중' : !hasDB ? '현재 항목을 모두 표시했습니다.' :
    fallback ? '버튼으로 다음 항목을 확인할 수 있습니다.' : '아래로 스크롤하면 다음 항목을 불러옵니다.';
  $('#queue_load_status').text(message);
};
FeederDashboard.prototype.tryAutoLoad = function (scrolledDown) {
  if (!scrolledDown || this.closed || !this.autoReady || this.awaitingBootstrap ||
      this.pageState !== 'idle' || this.busy.queue_page || this.busy.queue_bootstrap || document.hidden) return;
  var sentinel = document.getElementById('queue_load_sentinel'), rect = sentinel.getBoundingClientRect();
  if (rect.top > window.innerHeight + this.prefetchMargin || rect.bottom < 0) return;
  if (this.state.nextCursor) this.more(true);
};
FeederDashboard.prototype.uploadResult = function (data) {
  var now = Date.now();
  this.last.queue_upload_counts = now;
  if (data.counts && data.counts.upload_current !== undefined && data.counts.upload_total !== undefined) {
    this.uploadHasCounts = true;
    render_upload_queue_count(data.counts);
  }
  if (data.state === 'ready' && this.uploadHasCounts) {
    this.uploadFailures = 0;
    this.uploadPendingSince = null;
    this.uploadPollDelay = 15000;
    $('#queue_upload_state').text('업로드 큐 확인 완료').attr('data-state', 'ready');
    return;
  }
  if (this.uploadPendingSince === null) this.uploadPendingSince = now;
  var warming = data.state === 'loading' && !this.uploadHasCounts && now - this.uploadPendingSince < 30000;
  if (warming) this.uploadPollDelay = 5000;
  else {
    this.uploadFailures = Math.min(this.uploadFailures + 1, 5);
    this.uploadPollDelay = Math.min(60000,
      (!this.uploadHasCounts && now - this.uploadPendingSince < 30000 ? 5000 : 15000) * Math.pow(2, this.uploadFailures - 1));
  }
  $('#queue_upload_state').text(warming ? '업로드 큐 집계 중' : '업로드 큐 갱신 지연 · ' +
    (this.uploadHasCounts ? '이전 값 유지' : '재확인 대기')).attr('data-state', warming ? 'loading' : 'stale');
};
// 엔진별 상태를 독립 텍스트 요소로 표시한다. 전체 요청 실패는 별도 안내로만 구분한다.
FeederDashboard.prototype.renderEngineStatus = function (requestFailed) {
  var host = document.getElementById && document.getElementById('queue_engine_state');
  if (!host) return;
  var entries = [];
  this.state.engineSnapshots.forEach(function (snapshot) {
    var at = snapshot.last_success_at ? new Date(snapshot.last_success_at * 1000).toLocaleTimeString('ko-KR', {hour12:false}) : '정상 수신 대기';
    var status = snapshot.state === 'ready' ? 'ready' : snapshot.state === 'loading' ? 'loading' :
      snapshot.state === 'error' ? 'error' : 'stale';
    var label = snapshot.engine_name + ': ' +
      (status === 'ready' ? '정상' : status === 'loading' ? '확인 중' : '갱신 지연') +
      (snapshot.match_verified === true ? '' : ' · DB 연결 확인 중 · 원격 항목 읽기 전용') +
      (snapshot.complete ? '' : ' · 부분/미확인 목록') + ' · 마지막 정상 ' + at;
    entries.push({text:label, status:status});
  });
  if (requestFailed) entries.push({text:'엔진 정보 갱신 지연 · 이전 수신 정보 유지', status:'stale'});
  if (!entries.length) entries.push({text:'활성 엔진 없음', status:null});
  var signature = JSON.stringify(entries);
  if (signature === this.engineStatusSignature) return;
  var fragment = document.createDocumentFragment();
  entries.forEach(function (entry, index) {
    if (index) fragment.appendChild(document.createTextNode(' / '));
    if (!entry.status) {
      fragment.appendChild(document.createTextNode(entry.text));
      return;
    }
    var span = document.createElement('span');
    span.className = 'queue-engine-state-item';
    span.setAttribute('data-state', entry.status);
    span.textContent = entry.text; // 엔진명과 원격 상태 문자열을 HTML로 해석하지 않는다.
    fragment.appendChild(span);
  });
  host.replaceChildren(fragment);
  this.engineStatusSignature = signature;
};
FeederDashboard.prototype.render = function () {
  var view = this.state.view(Date.now() / 1000), signature = String(this.state.dbReady) + JSON.stringify([this.state.activeStage, this.state.queueTime, view.rows]);
  render_queue_speed_badge(view.downloadSpeeds);
  var uploadSpeed = 0, stagingSpeed = 0, hasUploading = false, hasStaging = false;
  view.rows.forEach(function (item) {
    if (item.status === 'uploading') hasUploading = true;
    else if (item.status === 'local_staging') hasStaging = true;
    else return;
    if (item.progress_stale || typeof item.download_speed !== 'number' ||
        !Number.isFinite(item.download_speed) || item.download_speed <= 0) return;
    if (item.status === 'uploading') uploadSpeed += item.download_speed;
    else stagingSpeed += item.download_speed;
  });
  var showStaging = !hasUploading && hasStaging;
  $('#stat_transfer_speed_label').text(showStaging ? '스테이징 속도' : '업로드 속도');
  var parts = format_bytes(showStaging ? stagingSpeed : uploadSpeed).split(' ');
  $('#stat_upload_speed_value').text(parts[0]);
  $('#stat_upload_speed_unit').text((parts[1] || 'B') + '/s');
  if ((this.state.dbReady || view.rows.length) && signature !== this.lastRender) {
    this.lastRender = signature;
    render_queue_rows(view.rows, this.state.activeStage, this.state.queueTime);
    // DOM 갱신으로 생긴 스크롤 앵커 변화는 새 페이지를 여는 신호로 사용하지 않는다.
    var sentinel = document.getElementById('queue_load_sentinel');
    if (sentinel) sentinel.getBoundingClientRect();
    this.lastScrollY = window.scrollY || 0;
    if (!this.state.dbReady) $('#active_queue_tbody').append('<tr id="queue_db_wait_row"><td colspan="3" class="py-3 text-muted">' +
      (this.dbError ? 'DB 불러오기 실패 · 자동 재시도 중' : 'DB 큐 불러오는 중…') + '</td></tr>');
  }
  $('#queue_page_summary').text((this.state.dbReady ? view.rows.length + ' / 전체 ' + this.state.total + '건' : '큐 확인 중') +
    ' · 미등록 ' + view.orphanTotal + '건' + (view.unverifiedTotal ? ' · DB 연결 확인 대기 ' + view.unverifiedTotal + '건' : ''));
  this.updatePageControls();
};
FeederDashboard.prototype.poll = function () {
  if (this.closed) return;
  var self = this, now = Date.now(), generation = this.state.requestGeneration;
  if (!this.last.queue_bootstrap || now - this.last.queue_bootstrap >= 10000) this.bootstrap();
  var visible = [];
  this.state.dbItemsById.forEach(function (item, id) { visible.push('db:' + id); });
  this.state.engineItemsByKey.forEach(function (item, key) { visible.push(key); });
  if (!this.last.queue_engines || now - this.last.queue_engines >= 5000) this.request('queue_engines', {visible:JSON.stringify(visible)}, function (data, gen) {
    if (!self.state.mergeEngines(data, gen)) return;
    self.renderEngineStatus(false);
    self.render();
    if (self.state.dbReady && !self.state.nextCursor &&
        self.state.orphanTotal + self.state.unverifiedTotal > self.state.engineItemsByKey.size) self.bootstrap();
  }, function () { self.renderEngineStatus(true); });
  if (!this.last.queue_progress || now - this.last.queue_progress >= 5000) {
    var ids = [];
    this.state.dbItemsById.forEach(function (item) {
      if (ids.length < 200 && (['uploading','local_staging','relay_transferring'].indexOf(item.status) >= 0 ||
          (item.status === 'downloaded' && item.destination_type === 'rclone_simple'))) ids.push(item.id);
    });
    if (ids.length) this.request('queue_progress', {ids: JSON.stringify(ids)}, function (data, gen) { self.state.mergeProgress(data, gen); self.render(); });
  }
  if (!document.hidden && (this.last.queue_upload_counts === undefined || now - this.last.queue_upload_counts >= this.uploadPollDelay)) this.request('queue_upload_counts', {}, function (data) {
    if (generation !== self.state.requestGeneration) return;
    self.uploadResult(data);
  }, function () { self.uploadResult({state:'error'}); });
};
FeederDashboard.prototype.resetEntry = function () {
  var generation = this.state.requestGeneration + 1;
  this.state = new FeederQueueState();
  this.state.requestGeneration = generation;
  this.consumedCursors.clear();
  this.lastRender = ''; this.engineStatusSignature = null;
  this.dbError = false;
  $('#active_queue_tbody').html('<tr><td colspan="3" class="py-4 text-muted">DB 첫 페이지 확인 중…</td></tr>');
  this.render();
  if (window.scrollTo) window.scrollTo({top:0, behavior:'instant'});
};
FeederDashboard.prototype.refresh = function (fromExpiry, freshEntry) {
  if (this.closed) return;
  $('#queue_page_loading').toggle(false).attr('aria-hidden', 'true');
  this.disconnectAutoLoad();
  this.state.requestGeneration++;
  this.requests.forEach(function (request) { request.abort(); });
  this.requests.clear(); this.busy = {}; this.last = {};
  if (freshEntry) this.resetEntry();
  this.awaitingBootstrap = true;
  this.pageState = fromExpiry ? 'error' : 'idle';
  if (!fromExpiry) this.expiredResynced = false;
  $('#queue_db_state').text(freshEntry ? 'DB 첫 페이지 확인 중' : 'DB 재확인 중 · 현재 목록 유지').attr('data-state', 'loading');
  this.poll();
};
FeederDashboard.prototype.start = function () {
  var self = this;
  if (this.timer) this.close();
  this.closed = false;
  this.resetEntry();
  this.awaitingBootstrap = true;
  this.pageState = 'idle';
  this.expiredResynced = false;
  this.autoUnavailable = false;
  this.lastScrollY = window.scrollY || 0;
  this.autoReady = false;
  this.busy = {}; this.last = {};
  $(window).off('scroll.feederQueue')
    .on('scroll.feederQueue', function () {
      var y = window.scrollY || 0;
      var down = y > self.lastScrollY;
      self.lastScrollY = y;
      self.tryAutoLoad(down);
      self.updatePageControls();
    });
  this.poll();
  this.timer = setInterval(function () { self.poll(); }, 5000);
  $('#queue_more_btn').off('click.feederQueue').on('click.feederQueue', function () { self.more(); });
};
FeederDashboard.prototype.close = function () {
  this.closed = true; this.state.requestGeneration++;
  $('#queue_page_loading').toggle(false).attr('aria-hidden', 'true');
  this.disconnectAutoLoad();
  clearInterval(this.timer);
  this.timer = null;
  $('#queue_more_btn').off('click.feederQueue');
  $(window).off('scroll.feederQueue');
  this.requests.forEach(function (request) { request.abort(); });
  this.requests.clear();
};
