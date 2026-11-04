"use strict";
// Interface do painel: recebe o estado por SSE (Observer) e envia comandos à API do painel.

const $ = (id) => document.getElementById(id);
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const fmt = (v, d = 4) => (v == null || Number.isNaN(Number(v)) ? "—" : Number(v).toLocaleString("pt-BR", { maximumFractionDigits: d }));

const estado = { config: null, maquinas: [], experimento: null, relatorio: null, specs: {}, logs: {}, logSel: null };
const LIMITE_LOG = 800;

async function api(caminho, opcoes = {}) {
  const r = await fetch(caminho, { headers: { "Content-Type": "application/json" }, ...opcoes });
  const dados = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(dados.erro || `Erro ${r.status}`);
  return dados;
}

function aviso(texto) {
  const el = $("avisoForm");
  el.hidden = !texto;
  el.textContent = texto || "";
}

// ------------------------------------------------------------------ máquinas
function renderMaquinas() {
  const html = estado.maquinas.map((m) => {
    const procs = Object.values(m.processos || {}).map((p) => {
      const art = p.artefatos || {};
      const extra = art.progresso ? `consulta ${esc(art.progresso)}` : art.erro ? esc(art.erro) : art.multiaddr ? "endereço publicado" : "";
      return `<div class="proc"><span>${esc(p.papel)}${extra ? `<small>${extra}</small>` : ""}</span><span class="selo ${esc(p.estado)}">${esc(p.estado.replaceAll("_", " "))}</span></div>`;
    }).join("");
    const s = estado.specs[m.nome];
    const specs = !s ? "" : s.ok
      ? `<div class="specs-ok">✓ ${esc(s.resumo.cpu_modelo)} · ${esc(s.resumo.metodo_cpu)} · ${fmt(s.resumo.ram_total_gb, 1)} GB</div>`
      : `<div class="specs-erro">✗ ${esc(s.problemas.join("\n"))}</div>`;
    return `<div class="maquina"><header><h4>${esc(m.nome)}</h4><span class="selo ${m.online ? "on" : "off"}">${m.online ? "online" : "offline"}</span></header>
      <div class="meta">${esc(m.agente)} · ${esc(m.papeis.join(", "))}${m.num_blocks ? ` · ${m.num_blocks} blocos` : ""}</div>
      ${m.online ? procs : `<div class="specs-erro">${esc(m.erro || "agente indisponível")}</div>`}${specs}</div>`;
  }).join("");
  $("maquinas").innerHTML = html || "<p class='sub'>Nenhuma máquina no inventário.</p>";
  renderSeletorLogs();
}

// ------------------------------------------------------------------ experimento
function renderExperimento() {
  const e = estado.experimento;
  if (!e) return;
  $("seloExp").textContent = e.estado;
  $("seloExp").className = `selo ${e.estado}`;
  const executando = e.estado === "executando";
  $("btnP2P").disabled = $("btnCentral").disabled = executando;
  $("btnCancelar").disabled = !executando;
  $("estadoExp").textContent = e.cenario
    ? `Cenário ${e.cenario} · experimento “${e.parametros.experimento}”${e.erro ? " · falhou" : ""}`
    : "Nenhum experimento em execução.";
  $("passos").innerHTML = (e.historico || []).map((p) => {
    const dur = p.inicio && p.fim ? ` · ${((p.fim - p.inicio)).toFixed(0)}s` : "";
    return `<li class="${esc(p.status)}"><span>${esc(p.descricao)}</span><span class="selo ${esc(p.status)}">${esc(p.status)}${dur}</span>
      ${p.detalhe ? `<span class="detalhe">${esc(p.detalhe)}</span>` : ""}</li>`;
  }).join("");
  $("multiaddr").textContent = e.multiaddr || "—";
  if (e.parametros?.experimento && !$("expRel").value) $("expRel").value = e.parametros.experimento;
}

// ------------------------------------------------------------------ resultados
const INDICADORES = [
  ["total_wh", "Energia total (Wh/consulta)", 5],
  ["ti_bruta_wh", "Energia de TI bruta (Wh)", 5],
  ["ti_marginal_wh", "Energia de TI marginal (Wh)", 5],
  ["rede_wh", "Energia de rede (Wh)", 6],
  ["wh_por_token", "Energia por token (Wh/token)", 7],
  ["carbono_g", "CO₂ (g/consulta)", 5],
  ["mg_co2_por_token", "CO₂ por token (mg/token)", 5],
  ["agua_total_ml", "Água (mL/consulta)", 4],
  ["tokens_por_s", "Taxa de geração (tokens/s)", 3],
  ["duracao_s", "Duração (s/consulta)", 2],
];

function celula(m, d) {
  return m ? `${fmt(m.media, d)} ± ${fmt(m.desvio, d)} <small>(n=${m.n})</small>` : "—";
}

function renderRelatorio() {
  const r = estado.relatorio;
  if (!r) return;
  const a = r.agregado?.centralizado?.metricas || {};
  const b = r.agregado?.p2p?.metricas || {};
  $("comparativo").innerHTML = INDICADORES.map(([k, rotulo, d]) => {
    const va = a[k]?.media, vb = b[k]?.media;
    const pct = va != null && vb != null && va !== 0 ? ((vb - va) / va) * 100 : null;
    const dif = pct == null ? "—" : Math.abs(pct) < 0.05 ? "0%" : `${pct > 0 ? "+" : ""}${fmt(pct, 1)}%`;
    return `<tr><td>${rotulo}</td><td class="num">${celula(a[k], d)}</td><td class="num">${celula(b[k], d)}</td><td class="num">${dif}</td></tr>`;
  }).join("");
  const linhas = (r.consultas || []).slice().reverse().map((c) => `<tr>
    <td>${esc(c.consulta_id)}</td><td>${esc(c.cenario)}</td><td>${esc(c.prompt_id)}</td><td class="num">${esc(c.repeticao)}</td>
    <td class="num">${esc(c.tokens_saida)}</td><td class="num">${fmt(c.duracao_s, 1)} s</td><td class="num">${fmt(c.tokens_por_s, 2)}</td>
    <td class="num">${fmt(c.total_wh, 5)}</td><td class="num">${fmt(c.carbono_g, 5)}</td><td class="num">${esc(c.maquinas)}</td>
    <td title="${esc((c.problemas || []).join(" | "))}">${c.valido ? "sim" : "não ⚠"}</td></tr>`).join("");
  $("consultas").innerHTML = linhas || "<tr><td colspan='11'>Nenhuma consulta registrada.</td></tr>";
}

// ------------------------------------------------------------------ logs
function renderSeletorLogs() {
  const sel = $("logSel");
  const opcoes = [];
  for (const m of estado.maquinas) for (const p of Object.keys(m.processos || {})) opcoes.push(`${m.nome}/${p}`);
  const atual = estado.logSel || opcoes[0];
  const html = opcoes.map((o) => `<option ${o === atual ? "selected" : ""}>${esc(o)}</option>`).join("");
  if (sel.innerHTML !== html) sel.innerHTML = html;
  if (!estado.logSel && atual) selecionarLog(atual);
}

async function selecionarLog(chave) {
  estado.logSel = chave;
  const [maquina, papel] = chave.split("/");
  try {
    const r = await api(`/api/logs?maquina=${encodeURIComponent(maquina)}&papel=${encodeURIComponent(papel)}`);
    estado.logs[chave] = r.linhas.slice(-LIMITE_LOG);
  } catch (e) { /* agente offline: mantém o que veio por SSE */ }
  renderLogs();
}

function renderLogs() {
  const el = $("logs");
  const noFim = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
  el.textContent = (estado.logs[estado.logSel] || []).join("\n");
  if (noFim) el.scrollTop = el.scrollHeight;
}

// ------------------------------------------------------------------ formulário
function preencherSelects() {
  const maq = estado.config.maquinas;
  const opcoes = (papel) => maq.filter((m) => m.papeis.includes(papel)).map((m) => `<option>${esc(m.nome)}</option>`).join("");
  $("bootstrap").innerHTML = opcoes("servidor");
  $("cliente").innerHTML = opcoes("cliente");
  $("central").innerHTML = opcoes("centralizado");
  const blocos = maq.filter((m) => m.papeis.includes("servidor")).reduce((s, m) => s + (m.num_blocks || 0), 0);
  $("resumoConfig").textContent = `${estado.config.modelo} · ${estado.config.camadas} camadas · servidores somam ${blocos} blocos · telemetria em ${estado.config.telemetria}`;
}

function parametros() {
  const f = new FormData($("formExp"));
  return Object.fromEntries([...f.entries()]);
}

async function executar(cenario) {
  aviso("");
  if (!$("formExp").reportValidity()) return;
  try {
    estado.experimento = await api("/api/experimentos", { method: "POST", body: JSON.stringify({ cenario, parametros: parametros() }) });
    renderExperimento();
  } catch (e) { aviso(e.message); }
}

// ------------------------------------------------------------------ eventos (SSE)
function conectarEventos() {
  const fonte = new EventSource("/api/eventos");
  fonte.addEventListener("maquinas", (ev) => { estado.maquinas = JSON.parse(ev.data); renderMaquinas(); });
  fonte.addEventListener("experimento", (ev) => { estado.experimento = JSON.parse(ev.data); renderExperimento(); });
  fonte.addEventListener("relatorio", (ev) => { estado.relatorio = JSON.parse(ev.data); renderRelatorio(); });
  fonte.addEventListener("specs", (ev) => { for (const s of JSON.parse(ev.data)) estado.specs[s.nome] = s; renderMaquinas(); });
  fonte.addEventListener("log", (ev) => {
    const d = JSON.parse(ev.data);
    const chave = `${d.maquina}/${d.papel}`;
    const buf = d.reinicio ? [] : (estado.logs[chave] || []);
    estado.logs[chave] = buf.concat(d.linhas).slice(-LIMITE_LOG);
    if (chave === estado.logSel) renderLogs();
  });
  fonte.onerror = () => { $("resumoConfig").dataset.offline = "1"; };
}

// ------------------------------------------------------------------ inicialização
async function iniciar() {
  try {
    const s = await api("/api/estado");
    estado.config = s.config;
    estado.maquinas = s.maquinas || [];
    estado.experimento = s.experimento;
    preencherSelects();
    renderMaquinas();
    renderExperimento();
  } catch (e) {
    $("resumoConfig").textContent = `Falha ao carregar o painel: ${e.message}`;
    return;
  }
  $("btnP2P").onclick = () => executar("p2p");
  $("btnCentral").onclick = () => executar("centralizado");
  $("btnCancelar").onclick = () => api("/api/experimentos/cancelar", { method: "POST" }).catch((e) => aviso(e.message));
  $("btnPararTudo").onclick = () => {
    if (confirm("Parar servidores, clientes e centralizado em todas as máquinas?"))
      api("/api/parar-tudo", { method: "POST" }).catch((e) => aviso(e.message));
  };
  $("btnSpecs").onclick = async () => {
    $("btnSpecs").disabled = true;
    try { for (const s of await api("/api/specs", { method: "POST" })) estado.specs[s.nome] = s; renderMaquinas(); }
    catch (e) { aviso(e.message); }
    finally { $("btnSpecs").disabled = false; }
  };
  $("btnCopiar").onclick = () => navigator.clipboard?.writeText($("multiaddr").textContent);
  $("logSel").onchange = (ev) => selecionarLog(ev.target.value);
  $("formRel").onsubmit = async (ev) => {
    ev.preventDefault();
    const exp = $("expRel").value.trim();
    if (!exp) return;
    try { estado.relatorio = await api(`/api/relatorio?experimento=${encodeURIComponent(exp)}`); renderRelatorio(); }
    catch (e) { aviso(e.message); }
  };
  conectarEventos();
}

iniciar();
