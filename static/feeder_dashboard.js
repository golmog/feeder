// 데이터 원천별 상태를 보존한다. 부분 응답은 전체 큐를 대체하지 않는다.
function FeederQueueState() {
  this.dbItemsById = new Map();
  this.loadedPages = [];
  this.engineSnapshots = new Map();
  this.progress = new Map();
  this.counts = {};
  this.requestGeneration = 0;
  this.dbReady = false;
  this.total = 0;
  this.nextCursor = null;
  this.dbAt = 0;
  this.orphanLimit = 40;
}
FeederQueueState.prototype.mergeDB = function (data, generation, page) {
  if (generation !== this.requestGeneration || !data || data.success !== true || !Array.isArray(data.items)) return false;
  if (!page && data.observed_at < this.dbAt) return false;
  if (!page) this.dbAt = data.observed_at;
  var self = this;
  data.items.forEach(function (item) {
    var old = self.dbItemsById.get(String(item.id));
    if (!old || (item.row_version || '') >= (old.row_version || '')) self.dbItemsById.set(String(item.id), item);
  });
  (data.removed_ids || []).forEach(function (id) { self.dbItemsById.delete(String(id)); self.progress.delete(String(id)); });
  this.loadedPages.push(data.items.map(function (item) { return item.id; }));
  if (this.loadedPages.length > 100) this.loadedPages.shift();
  if (data.counts) this.counts = data.counts;
  this.total = data.total;
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
      items.set(key, Object.assign({}, item, {
        match_verified: snapshot.match_verified === true,
        db_id: snapshot.match_verified === true ? item.db_id : (previous && previous.db_id)
      }));
    });
    items.forEach(function (item, key) { if (data.observed_at - item.last_seen_at > 300) items.delete(key); });
    self.engineSnapshots.set(snapshot.engine_key, Object.assign({}, snapshot, {items: items, observed_at: data.observed_at}));
  });
  this.engineSnapshots.forEach(function (snapshot, key) { if (!enabled.has(key)) self.engineSnapshots.delete(key); });
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
  var self = this, rows = new Map(), orphans = [], orphanTotal = 0, unverifiedTotal = 0,
    registeredTasks = new Set(), registeredHashes = new Map();
  this.dbItemsById.forEach(function (item, id) {
    var row = Object.assign({}, item), live = self.progress.get(id);
    if (row.status === 'downloading') row.engine_stale = true;
    if (live && live.status === row.status && live.row_version === row.row_version) {
      Object.assign(row, live);
      if (now - live.observed_at > 15) { row.download_speed = 0; row.speed_str = '갱신 지연'; }
    }
    rows.set(id, row);
    var prefix = row.current_engine_name + '\u0000';
    if (row.engine_task_id) registeredTasks.add(prefix + row.engine_task_id);
    if (row.infohash) registeredHashes.set(prefix + String(row.infohash).toLowerCase(), row.engine_task_id || '');
  });
  this.engineSnapshots.forEach(function (snapshot) {
    snapshot.items.forEach(function (item, identity) {
      if (now - item.last_seen_at > 300) return;
      var stale = item.stale || snapshot.state === 'error' || snapshot.error_type || now - item.last_seen_at > 60;
      if (item.db_id) {
        var row = rows.get(String(item.db_id));
        var matches = row && row.current_engine_name === snapshot.engine_name &&
          (row.engine_task_id && item.task_id ? String(row.engine_task_id) === String(item.task_id) :
            row.infohash && String(row.infohash).toLowerCase() === String(item.hash).toLowerCase());
        if (matches && row.status === 'downloading') {
          if (!row.file_size) row.file_size = item.file_size;
          row.progress = item.progress || (item.file_size > 0 ? item.downloaded_bytes / item.file_size * 100 : 0);
          row.downloaded_bytes = item.downloaded_bytes;
          row.download_speed = stale ? 0 : item.download_speed;
          row.engine_stale = !!stale;
        }
      } else {
        var prefix = snapshot.engine_name + '\u0000';
        var hashKey = prefix + String(item.hash || '').toLowerCase();
        var registered = registeredTasks.has(prefix + item.task_id) ||
          (registeredHashes.has(hashKey) && (!registeredHashes.get(hashKey) || !item.task_id));
        if (registered) return;
        var verified = snapshot.match_verified === true && item.match_verified === true;
        if (verified) orphanTotal++; else unverifiedTotal++;
        orphans.push({id: String(item.task_id || item.hash).slice(0, 10),
          row_key: 'engine:' + snapshot.engine_key + ':' + identity, task_id: item.task_id, hash: item.hash,
          title: item.filename, file_name: item.filename, file_size: item.file_size, local_path: item.source_path,
          feed_name: snapshot.engine_name, current_engine_name: snapshot.engine_name, status: verified ? 'engine_unmanaged' : 'engine_unverified',
          raw_status: item.status, engine_stale: !!stale, created_time: '원격 보관', last_seen_at: item.last_seen_at});
      }
    });
  });
  return {rows: Array.from(rows.values()).concat(orphans.slice(0, this.orphanLimit)),
    orphanTotal: orphanTotal, unverifiedTotal: unverifiedTotal};
};

function FeederDashboard() {
  this.state = new FeederQueueState();
  this.closed = false;
  this.requests = new Set();
  this.busy = {};
  this.last = {};
  this.lastRender = '';
  this.dbError = false;
  this.uploadHasCounts = false;
  this.uploadPendingSince = null;
  this.uploadFailures = 0;
  this.uploadPollDelay = 5000;
  this.pageState = 'idle';
  this.awaitingBootstrap = true;
  this.expiredResynced = false;
  this.observer = null;
  this.autoUnavailable = false;
  this.autoVisible = false;
  this.autoBatch = 0;
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
  this.state.dbItemsById.forEach(function (item, id) { known[id] = item.row_version; });
  this.request('queue_bootstrap', {known: JSON.stringify(known)}, function (data, generation) {
    if (!self.state.mergeDB(data, generation, false)) return;
    self.dbError = false;
    self.awaitingBootstrap = false;
    $('#queue_db_state').text('DB 최신 확인').attr('data-state', 'ready');
    $('#active_queue_tbody').attr('aria-busy', 'false');
    if (data.counts) self.renderCounts(data.counts);
    if (data.counts_error) $('#queue_counts_state').text('통계 갱신 지연 · 기존 값 유지').attr('data-state', 'stale');
    self.render();
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
  ['pending', 'downloading', 'staging', 'completed', 'failed'].forEach(function (key) {
    if (counts[key] !== undefined) $('#stat_' + key).text(counts[key]);
  });
  $('#queue_counts_state').text('DB 통계 최신 확인').attr('data-state', 'ready');
  ['download', 'upload'].forEach(function (kind) {
    if (counts['total_' + kind + '_24h'] !== undefined)
      $('#queue_daily_' + kind + '_badge').text('24시간 ' + (kind === 'download' ? '완료: ' : '업로드: ') + format_bytes(counts['total_' + kind + '_24h']));
  });
};
FeederDashboard.prototype.more = function (automatic) {
  if (this.closed || !this.state.dbReady || !this.state.nextCursor || this.busy.queue_bootstrap || this.busy.queue_page) return;
  var self = this, cursor = this.state.nextCursor;
  if (!automatic) { this.autoBatch = 0; this.expiredResynced = false; }
  this.pageState = 'loading';
  this.updatePageControls();
  this.request('queue_page', {cursor: cursor}, function (data, generation) {
    if (self.state.mergeDB(data, generation, true)) {
      self.pageState = data.next_cursor === cursor ? 'error' : 'idle';
      self.render();
    }
  }, function (data) {
    self.pageState = 'error';
    self.updatePageControls();
    if (data.error === 'snapshot_expired' && !self.expiredResynced) {
      self.expiredResynced = true;
      self.refresh(true);
    }
  });
};
FeederDashboard.prototype.disconnectAutoLoad = function () {
  if (this.observer) this.observer.disconnect();
  this.observer = null;
  this.autoVisible = false;
  this.autoBatch = 0;
};
FeederDashboard.prototype.updatePageControls = function () {
  var view = this.state.view(Date.now() / 1000);
  var hasDB = this.state.dbReady && !!this.state.nextCursor;
  var hasRemote = view.orphanTotal + view.unverifiedTotal > this.state.orphanLimit;
  if (!hasDB && !hasRemote) this.disconnectAutoLoad();
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
            if (!entry.isIntersecting) self.autoBatch = 0;
          });
          self.updatePageControls();
          self.tryAutoLoad();
        }, {root:null, rootMargin:'300px 0px', threshold:0});
        this.observer = observer;
        observer.observe(sentinel);
      } catch (err) {
        this.disconnectAutoLoad();
        this.autoUnavailable = true;
      }
    }
  }
  var fallback = this.autoUnavailable || !this.observer || this.pageState === 'error' || this.autoBatch >= 2;
  var loading = this.pageState === 'loading' || !!this.busy.queue_page;
  $('#queue_more_btn').toggle(hasDB && fallback).prop('disabled', loading || !!this.busy.queue_bootstrap)
    .text(this.pageState === 'error' ? '다시 시도' : loading ? '로딩 중' : '더 보기');
  $('#queue_engine_more_btn').toggle(hasRemote && fallback).prop('disabled', loading);
  var message = loading ? '로딩 중' : this.pageState === 'error' ? '추가 로딩 실패 · 다시 시도해주세요.' :
    !this.state.dbReady ? '초기 작업 확인 중' : !hasDB && !hasRemote ? '현재 항목을 모두 표시했습니다.' :
    fallback ? '버튼으로 다음 항목을 확인할 수 있습니다.' : '아래로 스크롤하면 다음 항목을 불러옵니다.';
  $('#queue_load_status').text(message);
};
FeederDashboard.prototype.tryAutoLoad = function () {
  if (this.closed || this.awaitingBootstrap || !this.observer || !this.autoVisible || this.autoBatch >= 2 ||
      this.pageState !== 'idle' || this.busy.queue_page || this.busy.queue_bootstrap || document.hidden) return;
  var sentinel = document.getElementById('queue_load_sentinel'), rect = sentinel.getBoundingClientRect();
  // 행 추가로 sentinel이 화면 밖으로 밀렸다면 다음 교차까지 기다린다.
  if (rect.top > window.innerHeight + 300 || rect.bottom < -300) return;
  if (this.state.nextCursor) { this.autoBatch++; this.more(true); }
  else {
    var view = this.state.view(Date.now() / 1000);
    if (view.orphanTotal + view.unverifiedTotal > this.state.orphanLimit) {
      this.autoBatch++;
      this.state.orphanLimit += 40;
      this.render();
      this.tryAutoLoad();
    }
  }
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
FeederDashboard.prototype.render = function () {
  var view = this.state.view(Date.now() / 1000), signature = String(this.state.dbReady) + JSON.stringify(view.rows);
  if ((this.state.dbReady || view.rows.length) && signature !== this.lastRender) {
    this.lastRender = signature;
    render_queue_rows(view.rows);
    if (!this.state.dbReady) $('#active_queue_tbody').append('<tr id="queue_db_wait_row"><td colspan="3" class="py-3 text-muted">' +
      (this.dbError ? 'DB 불러오기 실패 · 수동 새로고침으로 재시도' : 'DB 큐 불러오는 중…') + '</td></tr>');
  }
  $('#queue_page_summary').text((this.state.dbReady ? this.state.dbItemsById.size + ' / 전체 ' + this.state.total + '건' : 'DB 큐 확인 중') +
    ' · 미등록 ' + view.orphanTotal + '건' + (view.unverifiedTotal ? ' · DB 연결 확인 대기 ' + view.unverifiedTotal + '건' : ''));
  this.updatePageControls();
};
FeederDashboard.prototype.poll = function () {
  if (this.closed) return;
  var self = this, now = Date.now(), generation = this.state.requestGeneration;
  if (!this.last.queue_bootstrap || now - this.last.queue_bootstrap >= 10000) this.bootstrap();
  if (!this.last.queue_engines || now - this.last.queue_engines >= 5000) this.request('queue_engines', {}, function (data, gen) {
    if (!self.state.mergeEngines(data, gen)) return;
    var labels = [];
    self.state.engineSnapshots.forEach(function (snapshot) {
      var at = snapshot.last_success_at ? new Date(snapshot.last_success_at * 1000).toLocaleTimeString('ko-KR', {hour12:false}) : '정상 수신 대기';
      labels.push(snapshot.engine_name + ': ' + (snapshot.state === 'ready' ? '정상' : '갱신 지연') +
        (snapshot.match_verified === true ? '' : ' · DB 연결 확인 중 · 원격 항목 읽기 전용') +
        (snapshot.complete ? '' : ' · 부분/미확인 목록') + ' · 마지막 정상 ' + at);
    });
    $('#queue_engine_state').text(labels.join(' / ') || '활성 엔진 없음').attr('data-state',
      Array.from(self.state.engineSnapshots.values()).some(function (s) { return s.state !== 'ready'; }) ? 'stale' : 'ready');
    self.render();
  }, function () { $('#queue_engine_state').text('엔진 정보 갱신 지연 · 이전 수신 정보 유지').attr('data-state', 'stale'); });
  if (!this.last.queue_progress || now - this.last.queue_progress >= 5000) {
    var ids = [];
    this.state.dbItemsById.forEach(function (item) { if (['uploading','local_staging','relay_transferring'].indexOf(item.status) >= 0) ids.push(item.id); });
    if (ids.length) this.request('queue_progress', {ids: JSON.stringify(ids)}, function (data, gen) { self.state.mergeProgress(data, gen); self.render(); });
  }
  if (!document.hidden && (this.last.queue_upload_counts === undefined || now - this.last.queue_upload_counts >= this.uploadPollDelay)) this.request('queue_upload_counts', {}, function (data) {
    if (generation !== self.state.requestGeneration) return;
    self.uploadResult(data);
  }, function () { self.uploadResult({state:'error'}); });
};
FeederDashboard.prototype.refresh = function (fromExpiry) {
  if (this.closed) return;
  this.disconnectAutoLoad();
  this.state.requestGeneration++;
  this.requests.forEach(function (request) { request.abort(); });
  this.requests.clear(); this.busy = {}; this.last = {};
  this.awaitingBootstrap = true;
  this.pageState = fromExpiry ? 'error' : 'idle';
  if (!fromExpiry) this.expiredResynced = false;
  $('#queue_db_state').text('DB 재확인 중 · 현재 목록 유지').attr('data-state', 'loading');
  this.poll();
};
FeederDashboard.prototype.start = function () {
  var self = this;
  if (this.timer) this.close();
  this.closed = false;
  this.awaitingBootstrap = true;
  this.pageState = 'idle';
  this.expiredResynced = false;
  this.autoUnavailable = false;
  this.busy = {}; this.last = {};
  this.poll();
  this.timer = setInterval(function () { self.poll(); }, 5000);
  $('#queue_more_btn').off('click.feederQueue').on('click.feederQueue', function () { self.more(); });
  $('#queue_engine_more_btn').off('click.feederQueue').on('click.feederQueue', function () {
    self.autoBatch = 0; self.state.orphanLimit += 40; self.render();
  });
};
FeederDashboard.prototype.close = function () {
  this.closed = true; this.state.requestGeneration++;
  this.disconnectAutoLoad();
  clearInterval(this.timer);
  this.timer = null;
  $('#queue_more_btn, #queue_engine_more_btn').off('click.feederQueue');
  this.requests.forEach(function (request) { request.abort(); });
  this.requests.clear();
};
