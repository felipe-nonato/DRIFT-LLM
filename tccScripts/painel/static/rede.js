"use strict";
// Página da rede P2P: criar a rede (nó central) ou entrar colando o ID. Atualiza por polling.

const $ = (id) => document.getElementById(id);
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const fmt = (v, d = 4) => (v == null || Number.isNaN(Number(v)) ? "—" : Number(v).toLocaleString("pt-BR", { maximumFractionDigits: d }));
const ID_VALIDO = /^\/ip4\/\d{1,3}(\.\d{1,3}){3}\/tcp\/\d{1,5}\/p2p\/(12D3KooW[1-9A-HJ-NP-Za-km-z]{40,50}|Qm[1-9A-HJ-NP-Za-km-z]{44})$/;
const LIMITE_LOG = 800;
const INDICADORES = [
  ["total_wh", "Energia total", "Wh/consulta", 5],
  ["carbono_g", "CO₂", "g/consulta", 5],
  ["agua_total_ml", "Água", "mL/consulta", 4],
  ["wh_por_token", "Energia por token", "Wh/token", 7],
  ["mg_co2_por_token", "CO₂ por token", "mg/token", 5],
  ["ti_marginal_wh", "TI marginal", "Wh/consulta", 5],
  ["rede_wh", "Rede", "Wh/consulta", 6],
  ["tokens_por_s", "Velocidade", "tokens/s", 3],
  ["duracao_s", "Duração", "s/consulta", 2],
];

const st = { estado: null, tela: null, camadas: 28, logPapel: "servidor", logSeq: {}, logLinhas: {}, cenario: "p2p",
             relatorio: null, ultimoRelatorio: 0 };

async function api(caminho, corpo) {
  const opcoes = corpo === undefined ? {} : { method: "POST", body: JSON.stringify(corpo) };
  const r = await fetch(caminho, { headers: { "Content-Type": "application/json" }, ...opcoes });
  const dados = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(dados.erro || `Erro ${r.status}`);
  return dados;
}

function mostrar(el, texto) { el.hidden = !texto; el.textContent = texto || ""; }

function tela(nome) {
  st.tela = nome;
  for (const id of ["escolha", "formCentral", "formEntrar", "ativo"]) $(id).hidden = id !== nome;
}

// ------------------------------------------------------------------ escolha e formulários
$("btnModoCentral").addEventListener("click", async () => {
  tela("formCentral");
  const e = st.estado;
  const preferida = e.interfaces.find((i) => !i.nome.startsWith("tailscale") && !i.nome.startsWith("docker")) || e.interfaces[0];
  $("cInterface").innerHTML = e.interfaces.map((i) =>
    `<option value="${esc(i.nome)}" ${i === preferida ? "selected" : ""}>${esc(i.nome.startsWith("tailscale") ? "Tailscale" : "Rede local")} — ${esc(i.nome)} (${esc(i.ip)})</option>`).join("");
  try {
    const s = await api(`/api/sugestao?torch_dtype=${$("cDtype").value}`);
    st.camadas = s.camadas;
    $("cBlocos").value = s.num_blocks;
  } catch (err) { mostrar($("erroCentral"), err.message); }
});
$("btnModoEntrar").addEventListener("click", () => { tela("formEntrar"); $("eId").focus(); });
document.querySelectorAll("[data-voltar]").forEach((b) => b.addEventListener("click", () => tela("escolha")));

$("fCentral").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const btn = $("btnCriar");
  btn.disabled = true; btn.textContent = "Ligando telemetria e nó central…";
  mostrar($("erroCentral"), "");
  try {
    st.estado = await api("/api/central", {
      interface: $("cInterface").value, experimento: $("cExperimento").value.trim(), torch_dtype: $("cDtype").value,
      num_blocks: Number($("cBlocos").value) || null, sem_rapl: $("cSemRapl").checked,
    });
    st.logPapel = "servidor";
    render();
  } catch (err) { mostrar($("erroCentral"), err.message); }
  finally { btn.disabled = false; btn.textContent = "Criar rede"; }
});

async function conectar() {
  const id = $("eId").value.trim();
  if (!ID_VALIDO.test(id)) { mostrar($("erroEntrar"), "Isso não parece um ID da rede. Ele começa com /ip4/ e contém /p2p/."); return; }
  const btn = $("btnEntrar");
  btn.disabled = true; btn.textContent = "Conectando…";
  mostrar($("erroEntrar"), "");
  try {
    st.estado = await api("/api/entrar", { multiaddr: id, sem_rapl: $("eSemRapl").checked });
    st.logPapel = "servidor";
    history.replaceState(null, "", "/");
    render();
  } catch (err) { mostrar($("erroEntrar"), err.message); }
  finally { btn.disabled = false; btn.textContent = "Conectar"; }
}
$("fEntrar").addEventListener("submit", (ev) => { ev.preventDefault(); conectar(); });
// colou o ID -> conecta sozinho
$("eId").addEventListener("paste", () => setTimeout(() => { if (ID_VALIDO.test($("eId").value.trim())) conectar(); }, 0));

// ------------------------------------------------------------------ rede ativa
$("btnCopiar").addEventListener("click", async () => {
  const id = st.estado?.multiaddr;
  if (!id) return;
  try { await navigator.clipboard.writeText(id); }
  catch {
    const r = document.createRange(); r.selectNodeContents($("idRede"));
    const s = getSelection(); s.removeAllRanges(); s.addRange(r); document.execCommand("copy");
  }
  $("copiado").hidden = false;
  setTimeout(() => ($("copiado").hidden = true), 2500);
});

$("btnDesligar").addEventListener("click", async () => {
  const central = st.estado?.modo?.tipo === "central";
  if (!confirm(central ? "Desligar o nó central derruba a rede para todos os PCs. Continuar?" : "Sair da rede?")) return;
  try { st.estado = await api("/api/desligar", {}); render(); } catch (err) { alert(err.message); }
});

$("btnConsultas").addEventListener("click", async () => {
  mostrar($("erroConsultas"), "");
  try {
    await api("/api/consultas", { repeticoes: Number($("qRep").value), max_new_tokens: Number($("qTokens").value) });
    st.logPapel = "cliente";
    await atualizar();
  } catch (err) { mostrar($("erroConsultas"), err.message); }
});
$("btnPararConsultas").addEventListener("click", async () => {
  try { await api("/api/parar-consultas", {}); await atualizar(); } catch (err) { mostrar($("erroConsultas"), err.message); }
});

const TEXTO_ESTADO = {
  parado: "parado", iniciando: "validando specs / iniciando", linha_de_base: "medindo linha de base (não use o PC)",
  operando: "operando", parando: "parando", concluido: "concluído", falhou: "falhou — veja o log",
};

function renderAtivo() {
  const e = st.estado, m = e.modo, srv = e.processos.servidor, cli = e.processos.cliente;
  const central = m.tipo === "central";
  $("tituloAtivo").textContent = central ? `Nó central — ${m.experimento}` : `Conectado à rede — ${m.experimento}`;
  $("subAtivo").textContent = central
    ? `Este PC (${e.hostname}, ${m.ip} via ${m.interface}) serve ${m.num_blocks} blocos e hospeda a telemetria.`
    : `Este PC (${e.hostname}, ${m.ip} via ${m.interface}) serve ${m.num_blocks} blocos. Nó central: ${m.central || "?"}.`;
  const selo = $("seloServidor");
  selo.className = `selo ${srv.estado}`;
  selo.textContent = `servidor: ${TEXTO_ESTADO[srv.estado] || srv.estado}`;
  if (e.multiaddr) {
    $("idRede").textContent = e.multiaddr;
    $("btnCopiar").disabled = false;
  } else if (!srv.ativo) {
    $("idRede").textContent = `O servidor parou (${srv.estado}). Veja o log abaixo.`;
  }
  $("btnConsultas").disabled = cli.ativo || !e.multiaddr;
  $("btnPararConsultas").disabled = !cli.ativo;
  const prog = cli.artefatos?.progresso;
  $("estadoCliente").textContent = cli.ativo
    ? `Cliente: ${TEXTO_ESTADO[cli.estado] || cli.estado}${prog ? ` — consulta ${prog}` : ""}`
    : cli.estado === "concluido" ? "Consultas concluídas. Resultados abaixo." : cli.estado === "falhou" ? "O cliente falhou: veja o log 'cliente'." : "Cliente parado.";
  $("linkCsv").setAttribute("download", `${m.experimento}.csv`);

  const papeis = Object.values(e.processos).filter((p) => p.iniciado_em).map((p) => p.papel);
  $("abasLog").innerHTML = papeis.map((p) => `<button type="button" class="pequeno ${p === st.logPapel ? "ativa" : "ghost"}" data-log="${p}">${p}</button>`).join("");
}
$("abasLog").addEventListener("click", (ev) => {
  const p = ev.target.dataset?.log;
  if (!p) return;
  st.logPapel = p;
  $("logs").textContent = (st.logLinhas[p] || []).join("\n");
  renderAtivo();
});

function render() {
  const e = st.estado;
  $("lede").textContent = `Este PC: ${e.hostname} · ${e.ram_gb} GB de RAM · ${e.interfaces.map((i) => `${i.nome} ${i.ip}`).join(" · ")}`;
  if (e.modo) { tela("ativo"); renderAtivo(); }
  else if (!st.tela || st.tela === "ativo") tela("escolha");
}

async function renderMembros() {
  try {
    const { membros } = await api("/api/membros");
    const servidores = membros.filter((x) => x.papel === "servidor");
    const soma = servidores.reduce((s, x) => s + (x.blocos || 0), 0);
    $("cobertura").textContent = `${servidores.length} servidor(es) somando ${soma} de ${st.camadas} blocos` +
      (soma >= st.camadas ? " — modelo completo, pronto para consultas." : " — faltam blocos: conecte mais PCs.");
    $("membros").innerHTML = membros.map((x) => `<tr>
      <td>${esc(x.no_id)}${x.bootstrap ? " ★" : ""}</td><td>${esc(x.papel)}</td><td>${esc(x.ip)}</td>
      <td>${esc(x.cpu)}</td><td class="num">${fmt(x.ram_gb, 1)}</td><td class="num">${x.blocos ?? "—"}</td>
      <td>${esc(x.metodo === "rapl" ? "RAPL" : "estimada (TDP)")}</td></tr>`).join("") || `<tr><td colspan="7">Nenhum PC registrado ainda.</td></tr>`;
  } catch (err) {
    $("cobertura").textContent = err.message;
  }
}

async function renderRelatorio() {
  let r;
  try { r = await api("/api/relatorio"); } catch (err) { $("subResultados").textContent = err.message; return; }
  st.relatorio = r;
  const cenarios = Object.keys(r.agregado || {});
  if (cenarios.length && !cenarios.includes(st.cenario)) st.cenario = cenarios[0];
  $("abasCenario").innerHTML = cenarios.map((c) => {
    const a = r.agregado[c];
    return `<button type="button" class="pequeno ${c === st.cenario ? "ativa" : "ghost"}" data-cen="${esc(c)}">${esc(c)} · ${a.validas}/${a.consultas} válidas</button>`;
  }).join("");
  const met = r.agregado?.[st.cenario]?.metricas || {};
  $("kpis").innerHTML = Object.keys(met).length
    ? INDICADORES.filter(([k]) => met[k]).map(([k, rotulo, un, d]) =>
        `<div class="kpi"><small>${rotulo}</small><b>${fmt(met[k].media, d)}</b><span>${un} · ± ${fmt(met[k].desvio, d)} · n=${met[k].n}</span></div>`).join("")
    : `<p class="sub">Sem consultas válidas ainda. Clique em “Rodar consultas” quando todos os blocos estiverem cobertos.</p>`;
  $("consultas").innerHTML = (r.consultas || []).slice().reverse().map((c) => `<tr>
    <td>${c.consulta_id}</td><td>${esc(c.cenario)}</td><td>${esc(c.prompt_id)}</td><td>${c.repeticao ?? "—"}</td>
    <td class="num">${c.tokens_saida ?? "—"}</td><td class="num">${fmt(c.duracao_s, 2)}</td><td class="num">${fmt(c.tokens_por_s, 2)}</td>
    <td class="num">${fmt(c.total_wh, 5)}</td><td class="num">${fmt(c.carbono_g, 5)}</td><td class="num">${fmt(c.agua_total_ml, 3)}</td>
    <td class="num">${c.maquinas}</td><td title="${esc((c.problemas || []).join(" | "))}">${c.valido ? "sim" : "não"}</td></tr>`).join("");
}
$("abasCenario").addEventListener("click", (ev) => {
  const c = ev.target.dataset?.cen;
  if (c) { st.cenario = c; renderRelatorio(); }
});

async function atualizarLogs() {
  const p = st.logPapel;
  const desde = st.logSeq[p] || 0;
  const { linhas } = await api(`/api/logs?papel=${p}&desde=${desde}`);
  if (!linhas.length) return;
  if (linhas[0].seq <= desde) return;
  st.logSeq[p] = linhas[linhas.length - 1].seq;
  st.logLinhas[p] = (st.logLinhas[p] || []).concat(linhas.map((l) => l.linha)).slice(-LIMITE_LOG);
  const el = $("logs");
  const noFim = el.scrollTop + el.clientHeight >= el.scrollHeight - 20;
  el.textContent = st.logLinhas[p].join("\n");
  if (noFim) el.scrollTop = el.scrollHeight;
}

async function atualizar() {
  try {
    st.estado = await api("/api/estado");
    render();
    if (st.tela === "ativo") {
      await atualizarLogs();
      if (Date.now() - st.ultimoRelatorio > 5000) {
        st.ultimoRelatorio = Date.now();
        renderMembros();
        renderRelatorio();
      }
    }
  } catch (err) {
    $("lede").textContent = `Sem resposta da página local (${err.message}). O ./rede.sh ainda está rodando?`;
  }
}

(async () => {
  await atualizar();
  try { st.camadas = (await api("/api/sugestao")).camadas; } catch { /* mantém 28 */ }
  // http://127.0.0.1:8090/#/ip4/... abre já conectando
  const doHash = decodeURIComponent(location.hash.slice(1));
  if (ID_VALIDO.test(doHash) && st.tela === "escolha") { tela("formEntrar"); $("eId").value = doHash; conectar(); }
  setInterval(atualizar, 1500);
})();
