// 세 설정 화면에서 같은 YAML 원문 편집 모달을 사용한다.
(function ($) {
  var editor = null, initialized = false, revision = null, baseline = '';
  var loading = false, saving = false, generation = 0, request = null;
  var resizeObserver = null;

  function value() { return editor ? editor.getValue() : ($('#feeder_yaml_text').val() || ''); }
  function dirty() { return value() !== baseline; }
  function controls() {
    $('#feeder_yaml_save').prop('disabled', loading || saving || !revision || !dirty());
    $('#feeder_yaml_reload').prop('disabled', loading || saving);
    if (editor) editor.setReadOnly(loading || saving || !revision);
    else $('#feeder_yaml_text').prop('readonly', loading || saving || !revision);
  }
  function resize() { if (editor) editor.resize(); }
  function initialize() {
    if (initialized) return;
    initialized = true;
    if (!window.ace) {
      $('#feeder_yaml_code').hide();
      $('#feeder_yaml_text').show();
      return;
    }
    editor = ace.edit('feeder_yaml_code');
    editor.setTheme('ace/theme/monokai');
    editor.session.setMode('ace/mode/yaml');
    editor.session.setTabSize(2);
    editor.session.setUseSoftTabs(true);
    editor.session.setUseWrapMode($('#feeder_yaml_wrap').is(':checked'));
    editor.setShowPrintMargin(false);
    editor.setFontSize(13);
    editor.on('change', controls);
    feeder_ace_instances.push(editor);
    if (window.ResizeObserver) {
      resizeObserver = new window.ResizeObserver(resize);
      resizeObserver.observe(document.getElementById('feeder_yaml_code'));
    }
  }
  function load() {
    if (loading || saving || (dirty() && !confirm('저장하지 않은 편집 내용을 버리고 YAML을 다시 불러오시겠습니까?'))) return;
    initialize();
    loading = true;
    revision = null;
    var current = ++generation;
    controls();
    $('#feeder_yaml_status').text('YAML 원문을 불러옵니다…');
    request = $.ajax({url: '/' + package_name + '/ajax/crawl/yaml_editor_read', type: 'POST', dataType: 'json', global: false, cache: false, timeout: 10000,
      success: function (data) {
        if (current !== generation) return;
        if (data.ret !== 'success') { $('#feeder_yaml_status').text(data.msg || 'YAML을 읽지 못했습니다.'); return; }
        if (editor) {
          editor.session.setNewLineMode(data.newline || 'unix');
          editor.setValue(data.content, -1);
        } else $('#feeder_yaml_text').val(data.content);
        baseline = value();
        revision = data.revision;
        $('#feeder_yaml_status').text(editor ? 'YAML 원문을 불러왔습니다.' : 'Ace를 불러오지 못해 텍스트 모드로 편집합니다.');
        resize();
      },
      error: function () { if (current === generation) $('#feeder_yaml_status').text('파일을 읽지 못했습니다. 연결을 확인한 뒤 다시 불러와주세요.'); },
      complete: function () {
        if (current !== generation) return;
        loading = false; request = null; controls();
      }
    });
  }
  function refreshSettings() {
    if (sub === 'crawl' && typeof load_all_crawl_data === 'function') load_all_crawl_data();
    if (sub === 'feed' && typeof load_all_feed_data === 'function') load_all_feed_data();
    if (sub === 'download' && typeof load_all_download_config === 'function') load_all_download_config();
  }

  $(document).on('click', '#feeder_yaml_edit_btn', function () {
    if (loading || saving) return;
    $('#feeder_yaml_modal').modal('show');
    load();
  });
  $(document).on('click', '#feeder_yaml_reload', load);
  $(document).on('shown.bs.modal', '#feeder_yaml_modal', function () { resize(); if (editor) editor.focus(); });
  $(document).on('input', '#feeder_yaml_text', controls);
  $(document).on('change', '#feeder_yaml_wrap', function () {
    if (editor) editor.session.setUseWrapMode($(this).is(':checked'));
  });
  $(document).on('hide.bs.modal', '#feeder_yaml_modal', function (event) {
    if (saving) { event.preventDefault(); return; }
    if (dirty() && !confirm('저장하지 않은 YAML 변경사항을 버리고 닫으시겠습니까?')) { event.preventDefault(); return; }
    generation++;
    if (request) request.abort();
    request = null; loading = false; revision = null; baseline = value();
    controls();
  });
  $(document).on('click', '#feeder_yaml_save', function () {
    if (loading || saving || !revision || !dirty()) return;
    saving = true;
    var current = generation, content = value();
    controls();
    $('#feeder_yaml_status').text('YAML 문법과 외부 변경을 확인하고 저장합니다…');
    request = $.ajax({url: '/' + package_name + '/ajax/crawl/yaml_editor_save', type: 'POST', dataType: 'json', global: false, timeout: 10000,
      data: {content: content, revision: revision},
      success: function (data) {
        if (current !== generation) return;
        $('#feeder_yaml_status').text(data.msg || '저장 결과를 확인하지 못했습니다.');
        if (data.ret === 'success' && data.revision) {
          revision = data.revision; baseline = content;
          refreshSettings();
        }
      },
      error: function () {
        if (current === generation) $('#feeder_yaml_status').text('저장 결과를 확인하지 못했습니다. 편집 내용은 유지됩니다. 서버의 저장 여부를 확인한 뒤 다시 시도해주세요.');
      },
      complete: function () {
        if (current !== generation) return;
        saving = false; request = null; controls();
      }
    });
  });
})($);
