(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const active = run => run && ['queued','running','stopping'].includes(run.status);
  const esc = value => String(value ?? '—').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  let runs = [], selected = localStorage.getItem('benchmark-run'), current = null, busy = false;
  const compared = new Set();
  const say = value => { $('bench-message').textContent = value; };
  const n = id => Number($(id).value);
  async function api(path, body) {
    const response = await fetch('/api/benchmarks' + path, body === undefined ? {} : {
      method:'POST', headers:{'Content-Type':'application/json','X-Benchmark-Control':'1'}, body:JSON.stringify(body)
    });
    const result = await response.json();
    if (!response.ok) throw new Error(typeof result.detail === 'string' ? result.detail : JSON.stringify(result.detail));
    return result;
  }
  function stage() {
    return {max_inflight:n('inflight'), request_interval:n('interval'), duration_seconds:n('duration'), warmup_seconds:n('warmup')};
  }
  function addStage(inflight = n('inflight'), interval = n('interval')) {
    const row = $('bench-plan').tBodies[0].insertRow();
    row.innerHTML = `<td><input aria-label="Request đồng thời của giai đoạn" type="number" min="1" max="32" value="${Number(inflight)}" required></td><td><input aria-label="Khoảng cách request của giai đoạn" type="number" min="0" step="any" value="${Number(interval)}" required></td><td><button type="button" class="secondary" aria-label="Xóa giai đoạn">×</button></td>`;
    row.querySelector('button').onclick = () => row.remove();
  }
  function mode() {
    $('auto-plan').hidden = $('mode').value !== 'auto';
    if ($('mode').value === 'auto' && !$('bench-plan').tBodies[0].rows.length) addStage();
    $('bench-plan').querySelectorAll('input').forEach(input => { input.disabled = $('mode').value !== 'auto'; });
  }
  function config() {
    const samples = $('samples').value.trim() ? JSON.parse($('samples').value) : [];
    const stages = $('mode').value === 'auto' ? [...$('bench-plan').tBodies[0].rows].map(row => ({
      ...stage(), max_inflight:Number(row.querySelectorAll('input')[0].value), request_interval:Number(row.querySelectorAll('input')[1].value)
    })) : [stage()];
    return {resource:'bid_opening', mode:$('mode').value, stages, samples,
      start_date:$('start-date').value || null, end_date:$('end-date').value || null,
      sample_size:n('sample-size'), max_requests:n('max-requests'), max_attempts:n('attempts'), cooldown_seconds:n('cooldown'),
      max_run_seconds:n('max-run'), max_errors_in_window:n('error-limit'), stop_on_rate_limit:$('stop-429').checked};
  }
  function stageRow(phase, prefix = '') {
    const cfg = phase.config || {}, records = phase.records || {};
    const count = Object.values(records).reduce((a,b) => a+b, 0);
    const errors = Object.values(phase.errors || {}).reduce((a,b) => a+b, 0);
    return `<tr><td>${esc(prefix)}${esc(phase.stage)} · ${esc(phase.phase)}${phase.completed_window === false ? ' (chưa đủ)' : ''}</td><td>${esc(cfg.max_inflight)} / ${esc(cfg.request_interval)}s</td><td>${esc(phase.records_per_minute)}</td><td>${esc(phase.requests_per_second)}</td><td>${esc(phase.p95_ms)}</td><td>${errors} / ${esc(phase.retries)}</td><td>${records.valid || 0} / ${count}</td></tr>`;
  }
  function renderCurrent() {
    current = runs.find(run => run.id === selected) || null;
    const run = current, summary = run?.summary || {}, phases = summary.phases || [];
    const metric = summary.current || phases[phases.length-1] || {};
    const manual = active(run) && run.status !== 'stopping' && run.config.mode === 'manual';
    $('start').disabled = runs.some(active) || busy;
    $('stop').disabled = !active(run) || run.status === 'stopping' || busy;
    ['faster','slower','apply-stage'].forEach(id => { $(id).disabled = !manual || busy; });
    $('rerun').disabled = !run || busy;
    $('run-status').textContent = run?.status || 'Chưa chọn';
    $('run-status').className = 'badge bench-status ' + (run?.status === 'completed' ? 'success' : run?.status === 'failed' ? 'failed' : 'running');
    $('run-info').textContent = run ? `${run.id} · ${summary.attempts || 0} lần gọi · ${summary.elapsed_seconds || 0}s${summary.reason ? ' · '+summary.reason : ''}${summary.pending_stage ? ' · Đang chờ đổi giai đoạn' : ''}` : 'Chọn lượt chạy trong lịch sử hoặc bắt đầu lượt mới.';
    $('metric-records').textContent = metric.records_per_minute ?? '—';
    $('metric-rps').textContent = metric.requests_per_second ?? '—';
    $('metric-p95').textContent = metric.p95_ms ?? '—';
    $('metric-errors').textContent = `${Object.values(metric.errors || {}).reduce((a,b) => a+b,0)} / ${metric.retries || 0}`;
    $('stage-rows').innerHTML = [...phases, ...(summary.current ? [summary.current] : [])].map(p => stageRow(p)).join('');
    $('endpoint-rows').innerHTML = Object.entries(metric.endpoints || {}).map(([path,m]) => `<tr><td class="wrap mono">${esc(path)}</td><td>${m.attempts}</td><td>${m.errors}</td><td>${esc(m.p50_ms)} / ${esc(m.p95_ms)}</td></tr>`).join('');
    $('phase-info').textContent = metric.phase ? `Giai đoạn ${metric.stage} · ${metric.phase} · thành công lần đầu: ${metric.first_attempt_success_rate === null ? '—' : (metric.first_attempt_success_rate*100).toFixed(1)+'%'} · cooldown: ${metric.cooldown_seconds}s` : 'Chưa có request.';
    $('error-detail').textContent = (metric.recent_errors || []).map(e => `${e.at} ${e.endpoint} · ${e.error} · lần ${e.attempt}`).join('\n') || 'Chưa có lỗi request trong giai đoạn này.';
    $('artifact-links').innerHTML = run ? ['config.json','samples.json','metadata.json','attempts.jsonl', ...(!active(run) ? ['summary.json','stages.csv','report.html'] : [])].map(name => `<a class="link" href="/api/benchmarks/${run.id}/artifacts/${name}">${name}</a>`).join('') : '';
    const measured = [...phases, ...(summary.current ? [summary.current] : [])].filter(p => p.phase === 'measure');
    const maximum = Math.max(1,...measured.map(p => p.records_per_minute));
    const bars = measured.map((p,i) => {
      const width = Math.min(70, 620 / measured.length - 6), x = 45 + i * 620 / measured.length, height = p.records_per_minute / maximum * 75;
      return `<rect x="${x}" y="${100-height}" width="${width}" height="${height}" fill="#1769e0" rx="3"/><text x="${x}" y="120">GĐ ${p.stage}</text><text x="${x}" y="${95-height}">${p.records_per_minute}</text>`;
    }).join('');
    $('rate-chart').innerHTML = `<text x="10" y="14">Hồ sơ hợp lệ / phút · các giai đoạn đo</text><line x1="40" y1="100" x2="690" y2="100" stroke="#e5e9ef"/>${bars}`;
  }
  function renderHistory() {
    $('history-rows').innerHTML = runs.map(run => `<tr><td><input type="checkbox" aria-label="So sánh ${run.id}" data-compare="${run.id}" ${compared.has(run.id) ? 'checked' : ''}></td><td><button class="secondary" data-select="${run.id}">${run.id.slice(0,10)}${run.id === selected ? ' · đang xem' : ''}</button></td><td>${esc(new Date(run.created_at).toLocaleString('vi-VN',{timeZone:'Asia/Ho_Chi_Minh'}))}</td><td>${esc(run.status)}</td><td>${run.summary.attempts || 0}</td><td>${esc(run.summary.reason || '')}</td></tr>`).join('');
    $('history-rows').querySelectorAll('[data-select]').forEach(button => { button.onclick = () => {
      selected = button.dataset.select; localStorage.setItem('benchmark-run',selected); renderHistory(); renderCurrent();
    }; });
    $('history-rows').querySelectorAll('[data-compare]').forEach(input => { input.onchange = () => {
      input.checked ? compared.add(input.dataset.compare) : compared.delete(input.dataset.compare); renderComparison();
    }; });
    renderComparison();
  }
  function renderComparison() {
    $('comparison-panel').hidden = !compared.size;
    $('comparison-rows').innerHTML = runs.filter(run => compared.has(run.id)).flatMap(run =>
      (run.summary.phases || []).filter(p => p.phase === 'measure').map(p => stageRow(p,run.id.slice(0,8)+' / '))
    ).join('');
  }
  async function refresh() {
    runs = await api('');
    if (!runs.some(run => run.id === selected)) selected = runs.find(active)?.id || runs[0]?.id || null;
    renderHistory(); renderCurrent();
  }
  async function action(fn) {
    if (busy) return;
    busy = true; renderCurrent(); say('');
    try { await fn(); await refresh(); } catch(error) { say(error.message); }
    finally { busy = false; renderCurrent(); }
  }
  $('bench-form').onsubmit = event => { event.preventDefault(); action(async () => {
    const run = await api('',config()); selected = run.id; localStorage.setItem('benchmark-run',selected);
  }); };
  $('stop').onclick = () => action(() => api('/'+selected+'/stop',{}));
  $('apply-stage').onclick = () => action(async () => {
    await api('/'+selected+'/stage',stage()); say('Đã gửi cấu hình. Giai đoạn mới bắt đầu sau khi các hồ sơ đang chạy hoàn tất và nghỉ.');
  });
  async function adjust(factor) {
    const summary = current.summary;
    const cfg = summary.pending_stage || summary.current?.config || summary.phases?.at(-1)?.config || current.config.stages[0];
    // Zero stays unpaced when increasing speed; slowing from zero starts at 1ms.
    const interval = cfg.request_interval === 0 ? (factor > 1 ? .001 : 0) : Number((cfg.request_interval*factor).toPrecision(6));
    const updated = {...cfg, request_interval:interval};
    await api('/'+selected+'/stage',updated);
    $('inflight').value = updated.max_inflight; $('interval').value = updated.request_interval;
    say('Đã yêu cầu giai đoạn mới với khoảng cách '+updated.request_interval+' giây.');
  }
  $('faster').onclick = () => action(() => adjust(1/1.2));
  $('slower').onclick = () => action(() => adjust(1/.8));
  $('rerun').onclick = () => action(async () => {
    const cfg = current.config;
    $('mode').value = cfg.mode; $('start-date').value = cfg.start_date || ''; $('end-date').value = cfg.end_date || '';
    for (const [id,key] of [['sample-size','sample_size'],['max-requests','max_requests'],['attempts','max_attempts'],['cooldown','cooldown_seconds']]) $(id).value = cfg[key];
    for (const [id,key] of [['inflight','max_inflight'],['interval','request_interval'],['duration','duration_seconds'],['warmup','warmup_seconds']]) $(id).value = cfg.stages[0][key];
    $('max-run').value = cfg.max_run_seconds ?? 7200;
    $('error-limit').value = cfg.max_errors_in_window ?? 5;
    $('stop-429').checked = cfg.stop_on_rate_limit ?? true;
    let samples = cfg.samples;
    const response = await fetch(`/api/benchmarks/${current.id}/artifacts/samples.json`);
    if (response.ok) samples = await response.json();
    $('samples').value = samples.length ? JSON.stringify(samples,null,2) : '';
    sampleRequirements();
    $('bench-plan').tBodies[0].replaceChildren(); cfg.stages.forEach(s => addStage(s.max_inflight,s.request_interval)); mode();
    say('Đã nạp cấu hình và mẫu. Kiểm tra rồi bấm Chạy benchmark.');
  });
  function sampleRequirements() { ['start-date','end-date'].forEach(id => { $(id).required = !$('samples').value.trim(); }); }
  $('samples').oninput = sampleRequirements;
  $('mode').onchange = mode; $('add-stage').onclick = () => addStage();
  $('refresh').onclick = () => action(refresh);
  // Calendar defaults follow Vietnam time; never auto-start a network benchmark.
  const yesterday = new Date(Date.now()-86400000).toLocaleDateString('en-CA',{timeZone:'Asia/Ho_Chi_Minh'});
  $('start-date').value = yesterday; $('end-date').value = yesterday;
  async function poll() { try { if (!busy) await refresh(); } catch(error) { say('Không thể cập nhật: '+error.message); } finally { setTimeout(poll,2000); } }
  mode(); poll();
})();
