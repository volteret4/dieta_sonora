/*
 * Panel de configuración compartido de tumtumpa. Archivo IDÉNTICO en cada
 * app (como theme-picker.js). Inyecta un botón de engranaje fijo que abre
 * un modal con las variables configurables del servicio, leídas/escritas
 * vía POST /api/settings y POST /api/settings/save -- y, si el servicio
 * declara jobs, una sección "Acciones" con un botón por job que relanza a
 * mano el mismo comando que ya corre Ofelia por cron (ver
 * /api/jobs/run más abajo).
 *
 * Contrato del backend:
 *   POST /api/settings         {password?} ->
 *     {requires_password, authorized, error?, vars: [{name, value, secret, help}],
 *      jobs?: [{id, label}]}
 *   POST /api/settings/save    {password?, values: {NAME: "..."}} ->
 *     {ok: true, message} | {error}
 *   POST /api/jobs/run         {job: "<id>"} ->
 *     {ok: true, message} | {error}
 *   -- /api/jobs/run no pide contraseña: no toca secretos, solo relanza
 *   un sync que ya corre solo a diario.
 *
 * La contraseña solo se guarda en memoria (variable JS), nunca en
 * localStorage ni en la URL.
 */
(function () {
  var authToken = null;
  var overlay, box;

  function el(tag, props) {
    var e = document.createElement(tag);
    if (props) Object.assign(e, props);
    return e;
  }

  function ensureButton() {
    if (document.getElementById('settings-gear-btn')) return;
    var btn = el('button', { id: 'settings-gear-btn', textContent: '⚙', title: 'Configuración' });
    Object.assign(btn.style, {
      position: 'fixed', bottom: '.6rem', right: '.8rem', zIndex: '9999',
      width: '2rem', height: '2rem', borderRadius: '50%', padding: '0',
      background: 'var(--surface, #1e2028)', color: 'var(--text, #e8e6e3)',
      border: '1px solid var(--border, #2e3340)', cursor: 'pointer', fontSize: '1rem',
    });
    btn.addEventListener('click', openPanel);
    document.body.appendChild(btn);
  }

  function ensureModal() {
    if (overlay) return;
    overlay = el('div', { id: 'settings-overlay' });
    Object.assign(overlay.style, {
      position: 'fixed', inset: '0', background: 'rgba(0,0,0,.7)',
      display: 'none', alignItems: 'center', justifyContent: 'center', zIndex: '10000',
    });
    box = el('div', { id: 'settings-box' });
    Object.assign(box.style, {
      background: 'var(--surface, #1e2028)', border: '1px solid var(--border, #2e3340)',
      borderRadius: '10px', padding: '1.2rem', width: '380px', maxWidth: '92vw',
      maxHeight: '82vh', overflowY: 'auto', fontFamily: 'system-ui, sans-serif',
      fontSize: '.85rem', color: 'var(--text, #e8e6e3)',
    });
    overlay.appendChild(box);
    overlay.addEventListener('click', function (e) { if (e.target === overlay) closePanel(); });
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && overlay.style.display !== 'none') closePanel();
    });
    document.body.appendChild(overlay);
  }

  function closePanel() { overlay.style.display = 'none'; }

  function openPanel() {
    ensureModal();
    overlay.style.display = 'flex';
    load();
  }

  function fieldStyle(input) {
    Object.assign(input.style, {
      width: '100%', boxSizing: 'border-box', background: 'var(--bg, #16181c)',
      color: 'var(--text, #e8e6e3)', border: '1px solid var(--border, #2e3340)',
      borderRadius: '6px', padding: '.4rem .55rem', fontSize: '.82rem',
      fontFamily: 'inherit', marginTop: '.2rem',
    });
  }

  function btnStyle(btn, primary) {
    Object.assign(btn.style, {
      background: primary ? 'var(--accent, #5a6475)' : 'transparent',
      color: primary ? '#fff' : 'var(--text-muted, #737880)',
      border: '1px solid ' + (primary ? 'var(--accent, #5a6475)' : 'var(--border, #2e3340)'),
      borderRadius: '6px', padding: '.4rem .9rem', fontSize: '.8rem', cursor: 'pointer',
    });
  }

  async function api(path, body) {
    var r = await fetch(path, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body || {}),
    });
    return r.json();
  }

  async function load() {
    box.textContent = 'Cargando…';
    var data = await api('/api/settings', { password: authToken });
    if (data.requires_password && !data.authorized) {
      renderPasswordForm(data.error);
      return;
    }
    renderVarsForm(data.vars || [], data.jobs || []);
  }

  function renderPasswordForm(error) {
    box.innerHTML = '';
    var h = el('div', { textContent: '⚙ Configuración', style: 'font-weight:600;margin-bottom:.7rem' });
    var p = el('input', { type: 'password', placeholder: 'Contraseña' });
    fieldStyle(p);
    var err = el('div', {
      textContent: error || '',
      style: 'color:var(--danger,#e07070);font-size:.75rem;margin-top:.4rem;min-height:1em',
    });
    var row = el('div', { style: 'display:flex;justify-content:flex-end;gap:.5rem;margin-top:.9rem' });
    var cancel = el('button', { textContent: 'Cancelar' }); btnStyle(cancel, false);
    var enter = el('button', { textContent: 'Entrar' }); btnStyle(enter, true);
    cancel.addEventListener('click', closePanel);
    enter.addEventListener('click', function () { authToken = p.value; load(); });
    p.addEventListener('keydown', function (e) { if (e.key === 'Enter') enter.click(); });
    row.appendChild(cancel); row.appendChild(enter);
    box.appendChild(h); box.appendChild(p); box.appendChild(err); box.appendChild(row);
    p.focus();
  }

  function renderJobsSection(jobs) {
    if (!jobs.length) return;
    var h2 = el('div', {
      textContent: '⏵ Acciones',
      style: 'font-weight:600;font-size:.78rem;margin-bottom:.4rem;color:var(--text-muted,#737880)',
    });
    box.appendChild(h2);
    jobs.forEach(function (job) {
      var wrap = el('div', { style: 'display:flex;align-items:center;gap:.5rem;margin-bottom:.5rem' });
      var btn = el('button', { textContent: job.label || job.id });
      btnStyle(btn, false);
      Object.assign(btn.style, { flex: '1', textAlign: 'left' });
      var status = el('span', { style: 'font-size:.72rem;min-height:1em' });
      btn.addEventListener('click', async function () {
        btn.disabled = true;
        var original = btn.textContent;
        btn.textContent = 'Ejecutando…';
        status.textContent = '';
        var r = await api('/api/jobs/run', { job: job.id });
        btn.disabled = false;
        btn.textContent = original;
        if (r.error) {
          status.textContent = '✗ ' + r.error;
          status.style.color = 'var(--danger, #e07070)';
        } else {
          status.textContent = '✓ ' + (r.message || 'Hecho');
          status.style.color = 'var(--success, #6fcf97)';
        }
      });
      wrap.appendChild(btn);
      wrap.appendChild(status);
      box.appendChild(wrap);
    });
    box.appendChild(el('div', {
      style: 'border-top:1px solid var(--border,#2e3340);margin:.6rem 0 .9rem',
    }));
  }

  function renderVarsForm(vars, jobs) {
    box.innerHTML = '';
    jobs = jobs || [];
    var h = el('div', { textContent: '⚙ Configuración', style: 'font-weight:600;margin-bottom:.7rem' });
    box.appendChild(h);

    renderJobsSection(jobs);

    if (!vars.length) {
      if (!jobs.length) {
        box.appendChild(el('div', {
          textContent: 'Este servicio no tiene variables configurables.',
          style: 'color:var(--text-muted,#737880)',
        }));
      }
      var closeRow = el('div', { style: 'display:flex;justify-content:flex-end;margin-top:1rem' });
      var closeBtn = el('button', { textContent: 'Cerrar' }); btnStyle(closeBtn, false);
      closeBtn.addEventListener('click', closePanel);
      closeRow.appendChild(closeBtn);
      box.appendChild(closeRow);
      return;
    }

    var inputs = {};
    vars.forEach(function (v) {
      var wrap = el('div', { style: 'margin-bottom:.7rem' });
      var label = el('label', { textContent: v.name, style: 'font-weight:600;font-size:.78rem' });
      wrap.appendChild(label);
      if (v.help) {
        wrap.appendChild(el('div', {
          textContent: v.help, style: 'color:var(--text-muted,#737880);font-size:.72rem;margin-top:.1rem',
        }));
      }
      var input = el('input', { type: v.secret ? 'password' : 'text', value: v.value || '' });
      fieldStyle(input);
      wrap.appendChild(input);
      inputs[v.name] = input;
      box.appendChild(wrap);
    });

    var msg = el('div', { style: 'font-size:.75rem;margin-top:.3rem;min-height:1em' });
    var row = el('div', { style: 'display:flex;justify-content:flex-end;gap:.5rem;margin-top:.9rem' });
    var cancel = el('button', { textContent: 'Cerrar' }); btnStyle(cancel, false);
    var save = el('button', { textContent: 'Guardar' }); btnStyle(save, true);
    cancel.addEventListener('click', closePanel);
    save.addEventListener('click', async function () {
      var values = {};
      Object.keys(inputs).forEach(function (k) { values[k] = inputs[k].value; });
      save.disabled = true; save.textContent = 'Guardando…';
      var r = await api('/api/settings/save', { password: authToken, values: values });
      save.disabled = false; save.textContent = 'Guardar';
      if (r.error) {
        msg.textContent = r.error;
        msg.style.color = 'var(--danger, #e07070)';
      } else {
        msg.textContent = r.message || 'Guardado.';
        msg.style.color = 'var(--success, #6fcf97)';
      }
    });
    row.appendChild(cancel); row.appendChild(save);
    box.appendChild(msg);
    box.appendChild(row);
  }

  function init() { ensureButton(); }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
