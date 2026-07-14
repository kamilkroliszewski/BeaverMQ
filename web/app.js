/*
 * app.js - BeaverMQ management dashboard (Vue 3 + Chart.js, dependency-light).
 *
 * Polls the broker JSON API (/api/overview, /api/queues, /api/exchanges,
 * /api/connections), derives publish/deliver/network rates and per-queue
 * in/out rates from successive snapshots, keeps short rolling histories for the
 * charts, and renders five views (Overview, Queues, Exchanges, Connections,
 * Performance) plus a slide-over detail drawer. No build step: served as static
 * files straight from the embedded HTTP server.
 */
const { createApp, markRaw } = Vue;

const HIST_N = 60;   // rolling chart history length
const SPARK_N = 24;  // per-queue depth sparkline length

/* Series colours (chosen to read on both light and dark themes). */
const C = {
  publish: '#f59e0b', deliver: '#2563eb', ready: '#e07a2b',
  netIn: '#0d9488', netOut: '#7c3aed', conns: '#2563eb', cons: '#16a34a',
};

/* ---- plain formatting helpers (shared by the chart component + app) ------- */
function compact(n) {
  n = +n || 0; const a = Math.abs(n);
  if (a >= 1e9) return (n / 1e9).toFixed(a >= 1e10 ? 0 : 1) + 'B';
  if (a >= 1e6) return (n / 1e6).toFixed(a >= 1e7 ? 0 : 1) + 'M';
  if (a >= 1e3) return (n / 1e3).toFixed(a >= 1e4 ? 0 : 1) + 'k';
  return String(Math.round(n));
}
function fmtInt(n) { return Math.round(+n || 0).toLocaleString(); }
function fmtBytes(n) {
  n = +n || 0;
  if (n < 1024) return n.toFixed(0) + ' B';
  if (n < 1048576) return (n / 1024).toFixed(1) + ' KB';
  if (n < 1073741824) return (n / 1048576).toFixed(1) + ' MB';
  return (n / 1073741824).toFixed(2) + ' GB';
}
function fmtUptime(s) {
  s = Math.floor(+s || 0);
  const d = Math.floor(s / 86400), h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60), sec = s % 60;
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${sec}s`;
  return `${sec}s`;
}
function cmp(a, b) {
  if (typeof a === 'number' && typeof b === 'number') return a - b;
  return String(a ?? '').localeCompare(String(b ?? ''), undefined, { numeric: true });
}

/* ---- inline icon set (clean line icons, 24x24) --------------------------- */
const ICONS = {
  dashboard: '<rect x="3" y="3" width="7" height="8" rx="1.5"/><rect x="14" y="3" width="7" height="5" rx="1.5"/><rect x="14" y="11" width="7" height="10" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/>',
  queue: '<path d="M4 13h3.5l1.5 2.5h6L16.5 13H20"/><rect x="3" y="5" width="18" height="14" rx="2.5"/>',
  exchange: '<path d="M3 8h14"/><path d="m14 5 3 3-3 3"/><path d="M21 16H7"/><path d="m10 13-3 3 3 3"/>',
  connection: '<path d="M9.5 14.5 14.5 9.5"/><path d="M8 12 5.5 14.5a3.5 3.5 0 0 0 5 5L13 17"/><path d="M16 12l2.5-2.5a3.5 3.5 0 0 0-5-5L11 7"/>',
  performance: '<polyline points="3 12 7 12 10 5 14 19 17 12 21 12"/>',
  search: '<circle cx="11" cy="11" r="7"/><line x1="21" y1="21" x2="16.7" y2="16.7"/>',
  'chevron-right': '<polyline points="9 6 15 12 9 18"/>',
  'chevron-left': '<polyline points="15 6 9 12 15 18"/>',
  x: '<line x1="6" y1="6" x2="18" y2="18"/><line x1="18" y1="6" x2="6" y2="18"/>',
  refresh: '<path d="M21 12a9 9 0 1 1-2.64-6.36"/><polyline points="21 3 21 9 15 9"/>',
  sun: '<circle cx="12" cy="12" r="4"/><line x1="12" y1="2" x2="12" y2="4"/><line x1="12" y1="20" x2="12" y2="22"/><line x1="2" y1="12" x2="4" y2="12"/><line x1="20" y1="12" x2="22" y2="12"/><line x1="4.9" y1="4.9" x2="6.3" y2="6.3"/><line x1="17.7" y1="17.7" x2="19.1" y2="19.1"/><line x1="4.9" y1="19.1" x2="6.3" y2="17.7"/><line x1="17.7" y1="6.3" x2="19.1" y2="4.9"/>',
  moon: '<path d="M21 12.8A8.5 8.5 0 1 1 11.2 3a6.5 6.5 0 0 0 9.8 9.8Z"/>',
  cpu: '<rect x="6" y="6" width="12" height="12" rx="2"/><rect x="9.5" y="9.5" width="5" height="5" rx="1"/><line x1="9" y1="3" x2="9" y2="5"/><line x1="15" y1="3" x2="15" y2="5"/><line x1="9" y1="19" x2="9" y2="21"/><line x1="15" y1="19" x2="15" y2="21"/><line x1="3" y1="9" x2="5" y2="9"/><line x1="3" y1="15" x2="5" y2="15"/><line x1="19" y1="9" x2="21" y2="9"/><line x1="19" y1="15" x2="21" y2="15"/>',
  clock: '<circle cx="12" cy="12" r="9"/><polyline points="12 7 12 12 15.5 14"/>',
  upload: '<line x1="12" y1="19" x2="12" y2="5"/><polyline points="6 11 12 5 18 11"/>',
  download: '<line x1="12" y1="5" x2="12" y2="19"/><polyline points="6 13 12 19 18 13"/>',
  users: '<circle cx="9" cy="8" r="3.2"/><path d="M3.5 20a5.5 5.5 0 0 1 11 0"/><path d="M16 5.2a3.2 3.2 0 0 1 0 6"/><path d="M20.5 20a5.5 5.5 0 0 0-4-5.3"/>',
  database: '<ellipse cx="12" cy="5.5" rx="8" ry="3"/><path d="M4 5.5v13c0 1.66 3.58 3 8 3s8-1.34 8-3v-13"/><path d="M4 12c0 1.66 3.58 3 8 3s8-1.34 8-3"/>',
  check: '<polyline points="5 12.5 10 17.5 19 7"/>',
  server: '<rect x="3" y="4" width="18" height="7" rx="2"/><rect x="3" y="13" width="18" height="7" rx="2"/><line x1="7" y1="7.5" x2="7.01" y2="7.5"/><line x1="7" y1="16.5" x2="7.01" y2="16.5"/>',
  network: '<line x1="7" y1="21" x2="7" y2="9"/><polyline points="3.5 12.5 7 9 10.5 12.5"/><line x1="17" y1="3" x2="17" y2="15"/><polyline points="13.5 11.5 17 15 20.5 11.5"/>',
  zap: '<polygon points="13 2 4 14 11 14 10 22 19 10 12 10 13 2"/>',
  menu: '<line x1="4" y1="7" x2="20" y2="7"/><line x1="4" y1="12" x2="20" y2="12"/><line x1="4" y1="17" x2="20" y2="17"/>',
  lock: '<rect x="4" y="10" width="16" height="10" rx="2"/><path d="M8 10V7a4 4 0 0 1 8 0v3"/>',
  trash: '<path d="M4 7h16"/><path d="M9 7V5a1.5 1.5 0 0 1 1.5-1.5h3A1.5 1.5 0 0 1 15 5v2"/><path d="M6.5 7l1 13h9l1-13"/>',
  hash: '<line x1="5" y1="9" x2="20" y2="9"/><line x1="4" y1="15" x2="19" y2="15"/><line x1="10" y1="3" x2="8" y2="21"/><line x1="16" y1="3" x2="14" y2="21"/>',
};

const VIEWS = ['overview', 'queues', 'exchanges', 'connections', 'cluster', 'performance'];
function initialView() {
  const h = (location.hash || '').replace(/^#/, '');
  if (VIEWS.includes(h)) return h;
  const v = localStorage.getItem('bmq.view');
  return VIEWS.includes(v) ? v : 'overview';
}

if (typeof Chart !== 'undefined') {
  Chart.defaults.font.family = '"Inter", -apple-system, sans-serif';
  Chart.defaults.font.size = 11;
}

const app = createApp({
  template: `
<div class="shell">
  <aside class="sidebar" :class="{ open: sidebarOpen }">
    <div class="brand">
      <span class="logo">🦫</span>
      <div>
        <div class="name">BeaverMQ</div>
        <div class="ver">v{{ ov.version || '—' }}</div>
      </div>
    </div>
    <nav class="nav">
      <div v-for="it in navItems" :key="it.id" class="nav-item"
           :class="{ active: view === it.id }" @click="nav(it.id)">
        <app-icon :name="it.icon" :size="18"></app-icon>
        <span>{{ it.label }}</span>
        <span v-if="it.n !== undefined" class="count num">{{ it.n }}</span>
      </div>
    </nav>
    <div class="spacer"></div>
    <div class="sidebar-foot">Multi-threaded AMQP 0-9-1<br>broker in C · {{ ov.workers || 0 }} workers</div>
  </aside>

  <div class="main">
    <header class="topbar">
      <button class="btn icon menu-btn" @click="sidebarOpen = !sidebarOpen"><app-icon name="menu"></app-icon></button>
      <h1>{{ currentTitle }}</h1>
      <span class="grow"></span>
      <span class="cluster" v-if="overview">cluster <b>{{ ov.broker }}</b></span>
      <span class="pill" :class="connected ? 'ok' : 'bad'"><span class="dot"></span>{{ connected ? 'Connected' : 'Offline' }}</span>
      <span class="pill" v-if="me" :class="me.level === 'open' ? 'bad' : 'ok'"
            :title="me.level === 'open'
                    ? 'No users configured - open bootstrap access from localhost'
                    : 'Signed in as ' + me.user + ' (' + me.level + ')'">
        <span class="dot"></span>{{ me.level === 'open' ? 'no auth (bootstrap)' : me.user }}
      </span>
      <button class="btn" v-if="me && me.level !== 'open'" @click="logout"
              title="Sign out (the browser forgets the Basic-auth credentials and prompts again)">Logout</button>
      <span class="muted" style="font-size:12px" v-if="lastUpdated">{{ lastUpdated }}</span>
      <select class="select" v-model.number="refreshMs">
        <option v-for="o in refreshOpts" :key="o.v" :value="o.v">{{ o.t === 'Paused' ? o.t : 'every ' + o.t }}</option>
      </select>
      <button class="btn icon" :class="{ spin: loading }" @click="fetchAll" title="Refresh now"><app-icon name="refresh"></app-icon></button>
      <button class="btn icon" @click="toggleTheme" :title="theme === 'dark' ? 'Light mode' : 'Dark mode'"><app-icon :name="theme === 'dark' ? 'sun' : 'moon'"></app-icon></button>
      <div class="loadbar" v-if="loading"><i></i></div>
    </header>

    <main class="content">
      <!-- ===== OVERVIEW ===== -->
      <section v-show="view === 'overview'" class="grid" style="gap:16px">
        <div class="grid cards">
          <stat-tile label="Messages ready" :value="fmtInt(qt.messages_ready)" icon="database" variant="c-amber"></stat-tile>
          <stat-tile label="Publish rate" :value="compact(publishRate)" suffix="/s" icon="upload" variant="c-green"></stat-tile>
          <stat-tile label="Deliver rate" :value="compact(deliverRate)" suffix="/s" icon="download" variant="c-blue"></stat-tile>
          <stat-tile label="Consumers" :value="fmtInt(totalConsumers)" icon="users" variant="c-violet"></stat-tile>
          <stat-tile label="Queues" :value="fmtInt(counts.queues)" icon="queue" variant="c-accent"></stat-tile>
          <stat-tile label="Exchanges" :value="fmtInt(counts.exchanges)" icon="exchange" variant="c-teal"></stat-tile>
          <stat-tile label="Connections" :value="fmtInt(counts.connections)" icon="connection" variant="c-blue"></stat-tile>
          <stat-tile label="Uptime" :value="fmtUptime(ov.uptime_seconds)" icon="clock" variant="c-slate"></stat-tile>
        </div>

        <div class="grid c2">
          <div class="card">
            <div class="card-head"><h2>Message throughput</h2><span class="grow"></span>
              <div class="legend">
                <span class="lg"><span class="sw" style="background:#f59e0b"></span>Published <span class="val num">{{ rateStr(publishRate) }}</span></span>
                <span class="lg"><span class="sw" style="background:#2563eb"></span>Delivered <span class="val num">{{ rateStr(deliverRate) }}</span></span>
              </div>
            </div>
            <div class="card-body"><chart-line :key="'tp'+chartKey" :labels="throughputChart.labels" :series="throughputChart.series" :area="true" :height="240"></chart-line></div>
          </div>
          <div class="card">
            <div class="card-head"><h2>Queued messages</h2><span class="grow"></span>
              <div class="legend"><span class="lg"><span class="sw" style="background:#e07a2b"></span>Ready <span class="val num">{{ fmtInt(qt.messages_ready) }}</span></span></div>
            </div>
            <div class="card-body"><chart-line :key="'qd'+chartKey" :labels="queuedChart.labels" :series="queuedChart.series" :area="true" :height="240"></chart-line></div>
          </div>
        </div>

        <div class="grid c2">
          <div class="card">
            <div class="card-head"><h2>Top queues by depth</h2><span class="grow"></span><span class="sub">{{ counts.queues }} total</span></div>
            <div class="card-body">
              <div v-if="topQueues.length === 0" class="muted" style="padding:8px 2px">No queues yet.</div>
              <div v-for="q in topQueues" :key="q.name" @click="openDetail('queue', q.name)" style="display:flex;align-items:center;gap:12px;padding:8px 2px;cursor:pointer">
                <div class="mono" style="width:38%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:600">{{ q.name }}</div>
                <div style="flex:1;height:8px;border-radius:99px;background:var(--surface-2);overflow:hidden">
                  <div :style="{ width: (q.messages / maxDepth * 100) + '%', height: '100%', background: 'var(--accent)', borderRadius: '99px' }"></div>
                </div>
                <div class="num" style="width:88px;text-align:right;font-weight:700">{{ fmtInt(q.messages) }}</div>
              </div>
            </div>
          </div>
          <div class="card">
            <div class="card-head"><h2>Broker</h2><span class="grow"></span><span class="sub">{{ ov.broker }}</span></div>
            <div class="card-body">
              <div class="kv">
                <div class="item"><div class="k">Workers</div><div class="v num">{{ ov.workers || 0 }}</div></div>
                <div class="item"><div class="k">Uptime</div><div class="v sm">{{ fmtUptime(ov.uptime_seconds) }}</div></div>
                <div class="item"><div class="k">Net in</div><div class="v sm num">{{ fmtBytes(net.bytes_received) }}</div></div>
                <div class="item"><div class="k">Net out</div><div class="v sm num">{{ fmtBytes(net.bytes_sent) }}</div></div>
                <div class="item"><div class="k">Total published</div><div class="v sm num">{{ fmtInt(ms.publish) }}</div></div>
                <div class="item"><div class="k">Total delivered</div><div class="v sm num">{{ fmtInt(qt.total_dequeued) }}</div></div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <!-- ===== QUEUES ===== -->
      <section v-show="view === 'queues'">
        <div class="card">
          <div class="card-head"><h2>Queues</h2><span class="sub">{{ queuesData.total }} total</span><span class="grow"></span>
            <div class="search" style="width:280px"><app-icon name="search" :size="16"></app-icon><input class="input" v-model="tables.queues.search" placeholder="Filter by name…"></div>
          </div>
          <div class="tablewrap">
            <table class="tbl">
              <thead><tr>
                <th class="sortable" @click="sortBy('queues','name')">Name <span class="arr" v-if="tables.queues.sortKey==='name'">{{ tables.queues.sortDir==='asc'?'▲':'▼' }}</span></th>
                <th class="sortable" @click="sortBy('queues','vhost')">Vhost <span class="arr" v-if="tables.queues.sortKey==='vhost'">{{ tables.queues.sortDir==='asc'?'▲':'▼' }}</span></th>
                <th class="sortable tr" @click="sortBy('queues','messages')">Ready <span class="arr" v-if="tables.queues.sortKey==='messages'">{{ tables.queues.sortDir==='asc'?'▲':'▼' }}</span></th>
                <th class="sortable tr" @click="sortBy('queues','consumers')">Consumers <span class="arr" v-if="tables.queues.sortKey==='consumers'">{{ tables.queues.sortDir==='asc'?'▲':'▼' }}</span></th>
                <th class="sortable tr" @click="sortBy('queues','inRate')">In/s <span class="arr" v-if="tables.queues.sortKey==='inRate'">{{ tables.queues.sortDir==='asc'?'▲':'▼' }}</span></th>
                <th class="sortable tr" @click="sortBy('queues','outRate')">Out/s <span class="arr" v-if="tables.queues.sortKey==='outRate'">{{ tables.queues.sortDir==='asc'?'▲':'▼' }}</span></th>
                <th class="tc">Trend</th>
                <th class="sortable tc" @click="sortBy('queues','durable')">Durable <span class="arr" v-if="tables.queues.sortKey==='durable'">{{ tables.queues.sortDir==='asc'?'▲':'▼' }}</span></th>
              </tr></thead>
              <tbody>
                <tr v-for="q in queuesData.items" :key="q.name" class="click" @click="openDetail('queue', q.name)">
                  <td class="name mono"><app-icon class="chev" name="chevron-right" :size="14" style="vertical-align:-2px"></app-icon>{{ q.name }}</td>
                  <td class="mono muted">{{ q.vhost }}</td>
                  <td class="tr"><span class="badge" :class="depthClass(q.messages)">{{ fmtInt(q.messages) }}</span></td>
                  <td class="tr num">{{ q.consumers }}</td>
                  <td class="tr num">{{ compact(q.inRate) }}</td>
                  <td class="tr num">{{ compact(q.outRate) }}</td>
                  <td class="tc">
                    <svg v-if="sparkPoints(q.name,84,26)" class="spark" width="84" height="26" viewBox="0 0 84 26" style="margin:0 auto"><polyline :points="sparkPoints(q.name,84,26)" fill="none" stroke="var(--accent)" stroke-width="1.6"></polyline></svg>
                    <span v-else class="muted">—</span>
                  </td>
                  <td class="tc"><app-icon v-if="q.durable" name="check" :size="16" style="color:var(--green)"></app-icon><span v-else class="muted">—</span></td>
                </tr>
                <tr v-if="queuesData.total === 0"><td colspan="8"><div class="empty"><app-icon class="ic" name="queue" :size="46"></app-icon><div class="t">{{ tables.queues.search ? 'No queues match your filter' : 'No queues declared yet' }}</div></div></td></tr>
              </tbody>
            </table>
          </div>
          <div class="pager" v-if="queuesData.total > 0">
            <span>Showing {{ queuesData.start + 1 }}–{{ queuesData.end }} of {{ queuesData.total }}</span><span class="grow"></span>
            <select class="select" v-model.number="tables.queues.pageSize"><option :value="25">25</option><option :value="50">50</option><option :value="100">100</option></select>
            <button class="pbtn" :disabled="queuesData.page<=1" @click="pageStep('queues',-1)"><app-icon name="chevron-left" :size="16"></app-icon></button>
            <span class="num">{{ queuesData.page }} / {{ queuesData.pages }}</span>
            <button class="pbtn" :disabled="queuesData.page>=queuesData.pages" @click="pageStep('queues',1)"><app-icon name="chevron-right" :size="16"></app-icon></button>
          </div>
        </div>
      </section>

      <!-- ===== EXCHANGES ===== -->
      <section v-show="view === 'exchanges'">
        <div class="card">
          <div class="card-head"><h2>Exchanges</h2><span class="sub">{{ exchangesData.total }} total</span><span class="grow"></span>
            <div class="search" style="width:280px"><app-icon name="search" :size="16"></app-icon><input class="input" v-model="tables.exchanges.search" placeholder="Filter by name / type…"></div>
          </div>
          <div class="tablewrap">
            <table class="tbl">
              <thead><tr>
                <th class="sortable" @click="sortBy('exchanges','name')">Name <span class="arr" v-if="tables.exchanges.sortKey==='name'">{{ tables.exchanges.sortDir==='asc'?'▲':'▼' }}</span></th>
                <th class="sortable" @click="sortBy('exchanges','type')">Type <span class="arr" v-if="tables.exchanges.sortKey==='type'">{{ tables.exchanges.sortDir==='asc'?'▲':'▼' }}</span></th>
                <th class="sortable tr" @click="sortBy('exchanges','bindings')">Bindings <span class="arr" v-if="tables.exchanges.sortKey==='bindings'">{{ tables.exchanges.sortDir==='asc'?'▲':'▼' }}</span></th>
              </tr></thead>
              <tbody>
                <tr v-for="x in exchangesData.items" :key="x.name" class="click" @click="openDetail('exchange', x.name)">
                  <td class="name mono"><app-icon class="chev" name="chevron-right" :size="14" style="vertical-align:-2px"></app-icon>{{ exName(x) }}</td>
                  <td><span class="tag" :style="exColors(x.type)">{{ x.type }}</span></td>
                  <td class="tr num">{{ x.bindings }}</td>
                </tr>
                <tr v-if="exchangesData.total === 0"><td colspan="3"><div class="empty"><app-icon class="ic" name="exchange" :size="46"></app-icon><div class="t">No exchanges</div></div></td></tr>
              </tbody>
            </table>
          </div>
          <div class="pager" v-if="exchangesData.total > 0">
            <span>Showing {{ exchangesData.start + 1 }}–{{ exchangesData.end }} of {{ exchangesData.total }}</span><span class="grow"></span>
            <select class="select" v-model.number="tables.exchanges.pageSize"><option :value="25">25</option><option :value="50">50</option><option :value="100">100</option></select>
            <button class="pbtn" :disabled="exchangesData.page<=1" @click="pageStep('exchanges',-1)"><app-icon name="chevron-left" :size="16"></app-icon></button>
            <span class="num">{{ exchangesData.page }} / {{ exchangesData.pages }}</span>
            <button class="pbtn" :disabled="exchangesData.page>=exchangesData.pages" @click="pageStep('exchanges',1)"><app-icon name="chevron-right" :size="16"></app-icon></button>
          </div>
        </div>
      </section>

      <!-- ===== CONNECTIONS ===== -->
      <section v-show="view === 'connections'">
        <div class="card">
          <div class="card-head"><h2>Connections</h2><span class="sub">{{ connectionsData.total }} total</span><span class="grow"></span>
            <div class="search" style="width:280px"><app-icon name="search" :size="16"></app-icon><input class="input" v-model="tables.connections.search" placeholder="Filter by peer / state…"></div>
          </div>
          <div class="tablewrap">
            <table class="tbl">
              <thead><tr>
                <th class="sortable" @click="sortBy('connections','id')">#<span class="arr" v-if="tables.connections.sortKey==='id'">{{ tables.connections.sortDir==='asc'?'▲':'▼' }}</span></th>
                <th class="sortable" @click="sortBy('connections','peer')">Peer <span class="arr" v-if="tables.connections.sortKey==='peer'">{{ tables.connections.sortDir==='asc'?'▲':'▼' }}</span></th>
                <th class="sortable" @click="sortBy('connections','state')">State <span class="arr" v-if="tables.connections.sortKey==='state'">{{ tables.connections.sortDir==='asc'?'▲':'▼' }}</span></th>
                <th class="sortable tr" @click="sortBy('connections','recv_bytes')">In <span class="arr" v-if="tables.connections.sortKey==='recv_bytes'">{{ tables.connections.sortDir==='asc'?'▲':'▼' }}</span></th>
                <th class="sortable tr" @click="sortBy('connections','sent_bytes')">Out <span class="arr" v-if="tables.connections.sortKey==='sent_bytes'">{{ tables.connections.sortDir==='asc'?'▲':'▼' }}</span></th>
              </tr></thead>
              <tbody>
                <tr v-for="c in connectionsData.items" :key="c.id" class="click" @click="openDetail('connection', c.id)">
                  <td class="muted num">{{ c.id }}</td>
                  <td class="name mono">{{ c.peer }}</td>
                  <td><span class="tag" :style="stateColors(c.state)">{{ c.state }}</span></td>
                  <td class="tr num">{{ fmtBytes(c.recv_bytes) }}</td>
                  <td class="tr num">{{ fmtBytes(c.sent_bytes) }}</td>
                </tr>
                <tr v-if="connectionsData.total === 0"><td colspan="5"><div class="empty"><app-icon class="ic" name="connection" :size="46"></app-icon><div class="t">No active connections</div></div></td></tr>
              </tbody>
            </table>
          </div>
          <div class="pager" v-if="connectionsData.total > 0">
            <span>Showing {{ connectionsData.start + 1 }}–{{ connectionsData.end }} of {{ connectionsData.total }}</span><span class="grow"></span>
            <select class="select" v-model.number="tables.connections.pageSize"><option :value="25">25</option><option :value="50">50</option><option :value="100">100</option></select>
            <button class="pbtn" :disabled="connectionsData.page<=1" @click="pageStep('connections',-1)"><app-icon name="chevron-left" :size="16"></app-icon></button>
            <span class="num">{{ connectionsData.page }} / {{ connectionsData.pages }}</span>
            <button class="pbtn" :disabled="connectionsData.page>=connectionsData.pages" @click="pageStep('connections',1)"><app-icon name="chevron-right" :size="16"></app-icon></button>
          </div>
        </div>
      </section>

      <!-- ===== CLUSTER ===== -->
      <section v-show="view === 'cluster'" class="grid" style="gap:16px">
        <!-- Prominent cluster-state banner (same on every node) -->
        <div class="card">
          <div class="card-body" style="display:flex;align-items:center;gap:14px;padding:14px 16px">
            <span class="tag" :style="stateColors(cl.state)" style="display:inline-flex;align-items:center;gap:6px;padding:6px 12px;font-size:14px">
              <app-icon :name="stateIcon(cl.state)" :size="16"></app-icon>{{ stateLabel(cl.state) }}
            </span>
            <div>
              <div style="font-weight:600">{{ stateDesc(cl.state) }}</div>
              <div class="muted" style="font-size:12px">
                Viewed from node #{{ cl.self_id }} ({{ cl.role }}) ·
                leader: {{ cl.leader_id < 0 ? 'none' : 'node #' + cl.leader_id }} ·
                quorum {{ cl.quorum }} of {{ cl.nodes }}
              </div>
            </div>
          </div>
        </div>

        <div class="grid cards">
          <stat-tile label="State" :value="stateLabel(cl.state)" icon="server" :variant="stateVariant(cl.state)"></stat-tile>
          <stat-tile label="This node" :value="'#' + cl.self_id + ' · ' + capitalize(cl.role)" icon="zap" :variant="roleVariant(cl.role)"></stat-tile>
          <stat-tile label="Leader" :value="cl.leader_id < 0 ? '—' : ('node #' + cl.leader_id)" icon="check" variant="c-green"></stat-tile>
          <stat-tile label="Term" :value="fmtInt(cl.term)" icon="hash" variant="c-violet"></stat-tile>
          <stat-tile label="Committed" :value="fmtInt(cl.commit_index)" icon="database" variant="c-accent"></stat-tile>
          <stat-tile label="Replication target" :value="fmtInt(cl.target_index)" icon="upload" variant="c-teal"></stat-tile>
        </div>

        <div class="card">
          <div class="card-head"><h2>Cluster members</h2>
            <span class="sub">replication progress (the leader's view, shown on every node)</span>
          </div>
          <div class="tablewrap">
            <table class="tbl">
              <thead><tr>
                <th>Node</th><th>Mesh address</th><th>Role</th><th>Link</th>
                <th class="tr">Replicated up to</th><th>Replication</th>
              </tr></thead>
              <tbody>
                <tr v-for="m in cl.members" :key="m.node_id">
                  <td class="name mono">node #{{ m.node_id }}<span v-if="m.self" class="muted"> · this node</span></td>
                  <td class="mono">{{ m.address }}</td>
                  <td><span class="tag" :style="roleColors(m.leader ? 'leader' : 'follower')">{{ m.leader ? 'leader' : 'follower' }}</span></td>
                  <td><span class="tag" :style="m.reachable ? upColors() : downColors()">{{ m.reachable ? 'up' : 'down' }}</span></td>
                  <td class="tr num">{{ fmtInt(m.replicated_index) }}</td>
                  <td>
                    <span v-if="!m.reachable" class="tag" :style="downColors()">unreachable</span>
                    <span v-else-if="m.behind > 0" class="tag" :style="stateColors('syncing')">catching up · {{ fmtInt(m.behind) }} behind</span>
                    <span v-else class="tag" :style="stateColors('healthy')">in sync</span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <!-- ===== ACCESS CONTROL ===== -->
      <section v-show="view === 'access'" class="grid" style="gap:16px">
        <div v-if="accessMsg" class="card access-msg-card" style="padding:10px 16px"><span class="access-msg">{{ accessMsg }}</span></div>

        <div class="card">
          <div class="card-head"><h2>Virtual hosts</h2><span class="sub">{{ vhosts.length }} total</span></div>
          <div class="card-body">
            <div class="tablewrap">
              <table class="tbl">
                <thead><tr><th>Name</th><th style="width:60px"></th></tr></thead>
                <tbody>
                  <tr v-for="v in vhosts" :key="v">
                    <td class="mono">{{ v }}</td>
                    <td><button v-if="v !== '/'" class="btn icon" title="Delete vhost" @click="delVhost(v)"><app-icon name="trash" :size="15"></app-icon></button></td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div style="display:flex;gap:8px;margin-top:12px;flex-wrap:wrap">
              <input class="input" style="max-width:240px" v-model.trim="newVhost" placeholder="new vhost name" @keyup.enter="addVhost">
              <button class="btn" @click="addVhost">Add vhost</button>
            </div>
          </div>
        </div>

        <div class="card">
          <div class="card-head"><h2>Users</h2><span class="sub">{{ users.length }} total</span></div>
          <div class="card-body">
            <div class="tablewrap">
              <table class="tbl">
                <thead><tr><th>Name</th><th>Tags</th><th style="width:60px"></th></tr></thead>
                <tbody>
                  <tr v-for="u in users" :key="u.name">
                    <td class="mono">{{ u.name }}</td>
                    <td><span v-for="t in u.tags" :key="t" class="badge" style="margin-right:6px">{{ t }}</span><span v-if="!u.tags.length" class="muted">—</span></td>
                    <td><button class="btn icon" title="Delete user" @click="delUser(u.name)"><app-icon name="trash" :size="15"></app-icon></button></td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div style="display:flex;gap:8px;margin-top:12px;flex-wrap:wrap;align-items:center">
              <input class="input" style="max-width:180px" v-model.trim="newUser.name" placeholder="username">
              <input class="input" style="max-width:180px" type="password" v-model="newUser.password" placeholder="password">
              <label style="display:flex;align-items:center;gap:6px;font-size:13px"><input type="checkbox" v-model="newUser.admin"> administrator</label>
              <button class="btn" @click="addUser">Add user</button>
            </div>
            <div class="muted" style="margin-top:8px;font-size:12px">A new user has no permissions until a row is added below. Re-adding an existing name resets its password/tags.</div>
          </div>
        </div>

        <div class="card">
          <div class="card-head"><h2>Permissions</h2><span class="sub">{{ perms.length }} total · POSIX regex per (user, vhost): configure / write / read</span></div>
          <div class="card-body">
            <div class="tablewrap">
              <table class="tbl">
                <thead><tr><th>User</th><th>Vhost</th><th>Configure</th><th>Write</th><th>Read</th><th style="width:60px"></th></tr></thead>
                <tbody>
                  <tr v-for="pm in perms" :key="pm.user + '@' + pm.vhost">
                    <td class="mono">{{ pm.user }}</td>
                    <td class="mono">{{ pm.vhost }}</td>
                    <td class="mono">{{ pm.configure || '∅' }}</td>
                    <td class="mono">{{ pm.write || '∅' }}</td>
                    <td class="mono">{{ pm.read || '∅' }}</td>
                    <td><button class="btn icon" title="Clear permission" @click="delPerm(pm)"><app-icon name="trash" :size="15"></app-icon></button></td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div style="display:flex;gap:8px;margin-top:12px;flex-wrap:wrap">
              <select class="select" v-model="newPerm.user"><option value="" disabled>user…</option><option v-for="u in users" :key="u.name" :value="u.name">{{ u.name }}</option></select>
              <select class="select" v-model="newPerm.vhost"><option value="" disabled>vhost…</option><option v-for="v in vhosts" :key="v" :value="v">{{ v }}</option></select>
              <input class="input" style="max-width:130px" v-model.trim="newPerm.configure" placeholder="configure">
              <input class="input" style="max-width:130px" v-model.trim="newPerm.write" placeholder="write">
              <input class="input" style="max-width:130px" v-model.trim="newPerm.read" placeholder="read">
              <button class="btn" @click="setPerm">Set permission</button>
            </div>
          </div>
        </div>
      </section>

      <!-- ===== PERFORMANCE ===== -->
      <section v-show="view === 'performance'" class="grid" style="gap:16px">
        <div class="grid cards">
          <stat-tile label="Total published" :value="compact(ms.publish)" icon="upload" variant="c-green"></stat-tile>
          <stat-tile label="Total delivered" :value="compact(qt.total_dequeued)" icon="download" variant="c-blue"></stat-tile>
          <stat-tile label="Net in" :value="fmtBytes(net.bytes_received)" icon="network" variant="c-teal"></stat-tile>
          <stat-tile label="Net out" :value="fmtBytes(net.bytes_sent)" icon="network" variant="c-violet"></stat-tile>
          <stat-tile label="Workers" :value="ov.workers || 0" icon="cpu" variant="c-slate"></stat-tile>
          <stat-tile label="Uptime" :value="fmtUptime(ov.uptime_seconds)" icon="clock" variant="c-accent"></stat-tile>
        </div>
        <div class="card">
          <div class="card-head"><h2>Message throughput</h2><span class="grow"></span>
            <div class="legend">
              <span class="lg"><span class="sw" style="background:#f59e0b"></span>Published <span class="val num">{{ rateStr(publishRate) }}</span></span>
              <span class="lg"><span class="sw" style="background:#2563eb"></span>Delivered <span class="val num">{{ rateStr(deliverRate) }}</span></span>
            </div>
          </div>
          <div class="card-body"><chart-line :key="'ptp'+chartKey" :labels="throughputChart.labels" :series="throughputChart.series" :area="true" :height="260"></chart-line></div>
        </div>
        <div class="grid c2">
          <div class="card">
            <div class="card-head"><h2>Network I/O</h2><span class="grow"></span>
              <div class="legend">
                <span class="lg"><span class="sw" style="background:#0d9488"></span>In <span class="val num">{{ bytesRate(netInRate) }}</span></span>
                <span class="lg"><span class="sw" style="background:#7c3aed"></span>Out <span class="val num">{{ bytesRate(netOutRate) }}</span></span>
              </div>
            </div>
            <div class="card-body"><chart-line :key="'net'+chartKey" unit="bytes" :labels="netChart.labels" :series="netChart.series" :area="true" :height="220"></chart-line></div>
          </div>
          <div class="card">
            <div class="card-head"><h2>Connections &amp; consumers</h2><span class="grow"></span>
              <div class="legend">
                <span class="lg"><span class="sw" style="background:#2563eb"></span>Connections <span class="val num">{{ counts.connections }}</span></span>
                <span class="lg"><span class="sw" style="background:#16a34a"></span>Consumers <span class="val num">{{ totalConsumers }}</span></span>
              </div>
            </div>
            <div class="card-body"><chart-line :key="'cc'+chartKey" unit="count" :labels="countsChart.labels" :series="countsChart.series" :height="220"></chart-line></div>
          </div>
        </div>
      </section>
    </main>
  </div>

  <!-- ===== Detail drawer ===== -->
  <transition name="fade"><div class="scrim" v-if="detail" @click="closeDetail"></div></transition>
  <transition name="slide">
    <aside class="drawer" v-if="detail">
      <div class="drawer-head">
        <div style="flex:1;min-width:0">
          <div class="kind">{{ detail.type }}</div>
          <div class="ttl mono">{{ detailTitle }}</div>
        </div>
        <button class="btn icon" @click="closeDetail"><app-icon name="x"></app-icon></button>
      </div>
      <div class="drawer-body">
        <template v-if="detail.type === 'queue' && detailData">
          <div class="kv">
            <div class="item"><div class="k">Ready</div><div class="v num">{{ fmtInt(detailData.messages) }}</div></div>
            <div class="item"><div class="k">Consumers</div><div class="v num">{{ detailData.consumers }}</div></div>
            <div class="item"><div class="k">Incoming</div><div class="v sm num">{{ rateStr(detailData.inRate) }}</div></div>
            <div class="item"><div class="k">Outgoing</div><div class="v sm num">{{ rateStr(detailData.outRate) }}</div></div>
            <div class="item"><div class="k">Total enqueued</div><div class="v sm num">{{ fmtInt(detailData.enqueued) }}</div></div>
            <div class="item"><div class="k">Total dequeued</div><div class="v sm num">{{ fmtInt(detailData.dequeued) }}</div></div>
            <div class="item full"><div class="k">Durability</div><div class="v sm">{{ detailData.durable ? 'Durable' : 'Transient' }}</div></div>
          </div>
          <div class="section-t">Depth (last {{ (detailData.depths || []).length }} samples)</div>
          <div class="card" style="box-shadow:none"><div class="card-body"><chart-line v-if="drawerSeries" :key="'dq'+detail.key+chartKey" :labels="drawerSeries.labels" :series="drawerSeries.series" :area="true" :height="150"></chart-line></div></div>
        </template>
        <template v-else-if="detail.type === 'exchange' && detailData">
          <div class="kv">
            <div class="item"><div class="k">Type</div><div class="v sm"><span class="tag" :style="exColors(detailData.type)">{{ detailData.type }}</span></div></div>
            <div class="item"><div class="k">Bindings</div><div class="v num">{{ detailData.bindings }}</div></div>
            <div class="item full"><div class="k">Name</div><div class="v sm mono">{{ exName(detailData) }}</div></div>
          </div>
        </template>
        <template v-else-if="detail.type === 'connection' && detailData">
          <div class="kv">
            <div class="item"><div class="k">Connection #</div><div class="v num">{{ detailData.id }}</div></div>
            <div class="item"><div class="k">State</div><div class="v sm"><span class="tag" :style="stateColors(detailData.state)">{{ detailData.state }}</span></div></div>
            <div class="item"><div class="k">Received</div><div class="v sm num">{{ fmtBytes(detailData.recv_bytes) }}</div></div>
            <div class="item"><div class="k">Sent</div><div class="v sm num">{{ fmtBytes(detailData.sent_bytes) }}</div></div>
            <div class="item full"><div class="k">Peer</div><div class="v sm mono">{{ detailData.peer }}</div></div>
          </div>
        </template>
        <div v-else class="empty"><div class="t">This item is no longer available.</div></div>
      </div>
    </aside>
  </transition>
</div>
`,
  data() {
    return {
      view: initialView(),
      theme: document.documentElement.getAttribute('data-theme') || 'light',
      chartKey: 0,
      sidebarOpen: false,

      refreshMs: +(localStorage.getItem('bmq.refresh') ?? 5000),
      refreshOpts: [
        { v: 0, t: 'Paused' }, { v: 2000, t: '2s' }, { v: 5000, t: '5s' },
        { v: 10000, t: '10s' }, { v: 30000, t: '30s' },
      ],

      loading: false,
      connected: false,
      error: '',
      lastUpdated: '',

      overview: null,
      queues: [],
      exchanges: [],
      connections: [],
      cluster: null,   // /api/cluster snapshot, or null (clustering disabled)

      queueHist: {},   // name -> { prevEnq, prevDeq, inRate, outRate, depths[] }
      _prev: null,     // { publish, deq, recv, sent, time }
      publishRate: 0, deliverRate: 0, netInRate: 0, netOutRate: 0,
      history: { t: [], publish: [], deliver: [], ready: [], netIn: [], netOut: [], conns: [], cons: [] },

      tables: {
        queues:      { search: '', sortKey: 'messages', sortDir: 'desc', page: 1, pageSize: 50 },
        exchanges:   { search: '', sortKey: 'name', sortDir: 'asc', page: 1, pageSize: 50 },
        connections: { search: '', sortKey: 'id', sortDir: 'asc', page: 1, pageSize: 50 },
      },

      detail: null,    // { type: 'queue'|'exchange'|'connection', key }
      _timer: null,

      /* Access-control page state. */
      vhosts: [], users: [], perms: [],
      accessMsg: '',
      me: null,        /* /api/whoami: { user, level } - login indicator */
      newVhost: '',
      newUser: { name: '', password: '', admin: false },
      newPerm: { user: '', vhost: '', configure: '.*', write: '.*', read: '.*' },
    };
  },

  computed: {
    ov()  { return this.overview || {}; },
    cl()  { return this.cluster || { enabled: false, members: [] }; },
    ot()  { return this.ov.object_totals || {}; },
    qt()  { return this.ov.queue_totals || {}; },
    ms()  { return this.ov.message_stats || {}; },
    net() { return this.ov.network || {}; },

    counts() {
      return { queues: this.queues.length, exchanges: this.exchanges.length,
               connections: this.connections.length };
    },
    totalConsumers() { return this.queues.reduce((s, q) => s + (q.consumers || 0), 0); },

    navItems() {
      const items = [
        { id: 'overview', label: 'Overview', icon: 'dashboard' },
        { id: 'queues', label: 'Queues', icon: 'queue', n: this.counts.queues },
        { id: 'exchanges', label: 'Exchanges', icon: 'exchange', n: this.counts.exchanges },
        { id: 'connections', label: 'Connections', icon: 'connection', n: this.counts.connections },
      ];
      if (this.cl.enabled)
        items.push({ id: 'cluster', label: 'Cluster', icon: 'server', n: this.cl.members.length });
      items.push({ id: 'access', label: 'Access control', icon: 'lock' });
      items.push({ id: 'performance', label: 'Performance', icon: 'performance' });
      return items;
    },
    currentTitle() { return (this.navItems.find((v) => v.id === this.view) || {}).label || ''; },

    /* Queues augmented with their derived in/out rates (for sort + display). */
    queuesAug() {
      return this.queues.map((q) => {
        const h = this.queueHist[q.name] || {};
        return { ...q, inRate: h.inRate || 0, outRate: h.outRate || 0 };
      });
    },
    topQueues() { return [...this.queuesAug].sort((a, b) => b.messages - a.messages).slice(0, 6); },
    maxDepth()  { return Math.max(1, ...this.topQueues.map((q) => q.messages)); },

    queuesData()      { return this.tableData('queues', this.queuesAug, ['name']); },
    exchangesData()   { return this.tableData('exchanges', this.exchanges, ['name', 'type']); },
    connectionsData() { return this.tableData('connections', this.connections, ['peer', 'state']); },

    throughputChart() {
      return { labels: this.history.t, series: [
        { label: 'Published/s', color: C.publish, data: this.history.publish },
        { label: 'Delivered/s', color: C.deliver, data: this.history.deliver },
      ] };
    },
    queuedChart() {
      return { labels: this.history.t,
               series: [{ label: 'Ready', color: C.ready, data: this.history.ready }] };
    },
    netChart() {
      return { labels: this.history.t, series: [
        { label: 'In/s', color: C.netIn, data: this.history.netIn },
        { label: 'Out/s', color: C.netOut, data: this.history.netOut },
      ] };
    },
    countsChart() {
      return { labels: this.history.t, series: [
        { label: 'Connections', color: C.conns, data: this.history.conns },
        { label: 'Consumers', color: C.cons, data: this.history.cons },
      ] };
    },

    detailData() {
      if (!this.detail) return null;
      const { type, key } = this.detail;
      if (type === 'queue') {
        const q = this.queuesAug.find((x) => x.name === key);
        if (!q) return null;
        return { ...q, depths: (this.queueHist[key] || {}).depths || [] };
      }
      if (type === 'exchange') return this.exchanges.find((x) => x.name === key) || null;
      if (type === 'connection') return this.connections.find((x) => x.id === key) || null;
      return null;
    },
    detailTitle() {
      const d = this.detailData;
      if (!d) return this.detail ? '(gone)' : '';
      if (this.detail.type === 'exchange') return this.exName(d);
      if (this.detail.type === 'connection') return d.peer || ('#' + d.id);
      return d.name;
    },
    drawerSeries() {
      if (this.detail && this.detail.type === 'queue' && this.detailData) {
        const d = this.detailData.depths || [];
        return { labels: d.map(() => ''), series: [{ label: 'Depth', color: C.ready, data: d }] };
      }
      return null;
    },
  },

  methods: {
    compact, fmtInt, fmtBytes, fmtUptime,
    rateStr(n) { return compact(n) + '/s'; },
    bytesRate(n) { return fmtBytes(n) + '/s'; },
    exName(x) { return x && x.name === '' ? '(AMQP default)' : (x ? x.name : ''); },

    capitalize(s) { s = String(s || ''); return s.charAt(0).toUpperCase() + s.slice(1); },
    roleVariant(r) { return r === 'leader' ? 'c-green' : (r === 'candidate' ? 'c-amber' : 'c-blue'); },
    roleColors(r) {
      const m = { leader: ['--green', '--green-weak'], candidate: ['--amber', '--amber-weak'],
                  follower: ['--blue', '--blue-weak'] };
      const [fg, bg] = m[r] || ['--slate', '--slate-weak'];
      return { color: `var(${fg})`, background: `var(${bg})` };
    },
    upColors()   { return { color: 'var(--green)', background: 'var(--green-weak)' }; },
    downColors() { return { color: 'var(--red)', background: 'var(--red-weak)' }; },

    stateLabel(s) {
      return { healthy: 'Healthy', syncing: 'Syncing', degraded: 'Degraded',
               no_quorum: 'No quorum' }[s] || s || '—';
    },
    stateDesc(s) {
      return {
        healthy:   'All nodes are up and fully replicated.',
        syncing:   'Replication in progress — a node is catching up.',
        degraded:  'A node is unreachable, but a majority is still up (writes continue).',
        no_quorum: 'Cannot reach a majority — writes are rejected (read-only).',
      }[s] || '';
    },
    stateColors(s) {
      const m = { healthy: ['--green', '--green-weak'], syncing: ['--amber', '--amber-weak'],
                  degraded: ['--amber', '--amber-weak'], no_quorum: ['--red', '--red-weak'] };
      const [fg, bg] = m[s] || ['--slate', '--slate-weak'];
      return { color: `var(${fg})`, background: `var(${bg})` };
    },
    stateVariant(s) {
      return { healthy: 'c-green', syncing: 'c-amber', degraded: 'c-amber', no_quorum: 'c-red' }[s] || 'c-slate';
    },
    stateIcon(s) { return s === 'healthy' ? 'check' : (s === 'no_quorum' ? 'x' : 'refresh'); },

    depthClass(m) { return m <= 0 ? 'zero' : (m > 100000 ? 'hot' : 'warm'); },
    exColors(t) {
      const m = { direct: ['--blue', '--blue-weak'], fanout: ['--violet', '--violet-weak'],
                  topic: ['--teal', '--teal-weak'], headers: ['--amber', '--amber-weak'] };
      const [fg, bg] = m[t] || ['--slate', '--slate-weak'];
      return { color: `var(${fg})`, background: `var(${bg})` };
    },
    stateColors(s) {
      const m = { ACTIVE: ['--green', '--green-weak'], CHANNEL_OPEN: ['--green', '--green-weak'],
                  HANDSHAKE: ['--amber', '--amber-weak'], CLOSING: ['--red', '--red-weak'] };
      const [fg, bg] = m[s] || ['--slate', '--slate-weak'];
      return { color: `var(${fg})`, background: `var(${bg})` };
    },

    sparkPoints(name, w, h) {
      const d = (this.queueHist[name] || {}).depths || [];
      if (d.length < 2) return '';
      const max = Math.max(1, ...d);
      const step = w / (SPARK_N - 1);
      const off = SPARK_N - d.length;
      return d.map((v, i) =>
        `${((off + i) * step).toFixed(1)},${(h - 2 - (v / max) * (h - 4)).toFixed(1)}`).join(' ');
    },

    /* generic filter + sort + paginate */
    tableData(kind, list, fields) {
      const c = this.tables[kind];
      const s = c.search.trim().toLowerCase();
      let rows = s ? list.filter((o) => fields.some((f) =>
        String(o[f] ?? '').toLowerCase().includes(s))) : list.slice();
      const total = rows.length;
      rows.sort((a, b) => { const r = cmp(a[c.sortKey], b[c.sortKey]); return c.sortDir === 'asc' ? r : -r; });
      const pages = Math.max(1, Math.ceil(total / c.pageSize));
      const page = Math.min(c.page, pages);
      const start = (page - 1) * c.pageSize;
      return { items: rows.slice(start, start + c.pageSize), total, pages, page, start,
               end: Math.min(start + c.pageSize, total) };
    },
    sortBy(kind, key) {
      const c = this.tables[kind];
      if (c.sortKey === key) { c.sortDir = c.sortDir === 'asc' ? 'desc' : 'asc'; }
      else { c.sortKey = key; c.sortDir = (key === 'name' || key === 'peer' || key === 'type' || key === 'state') ? 'asc' : 'desc'; }
    },
    pageStep(kind, dir) {
      const c = this.tables[kind];
      c.page = Math.min(this[kind + 'Data'].pages, Math.max(1, this[kind + 'Data'].page + dir));
    },

    openDetail(type, key) { this.detail = { type, key }; },
    closeDetail() { this.detail = null; },
    nav(v) { this.view = v; },

    applyTheme() { document.documentElement.setAttribute('data-theme', this.theme); },
    toggleTheme() { this.theme = this.theme === 'dark' ? 'light' : 'dark'; },

    /* HTTP Basic auth has no server-side session to destroy. The XHR below
     * supplies deliberately-wrong credentials through the browser's own auth
     * machinery, which overwrites the cached ones; the reload then gets a 401
     * and the browser shows its login prompt again. */
    logout() {
      const xhr = new XMLHttpRequest();
      xhr.open('GET', '/api/whoami', true, 'logout', String(Date.now()));
      xhr.onloadend = () => location.reload();
      try { xhr.send(); } catch (e) { location.reload(); }
    },

    restartTimer() {
      if (this._timer) { clearInterval(this._timer); this._timer = null; }
      if (this.refreshMs > 0) this._timer = setInterval(() => this.fetchAll(), this.refreshMs);
    },

    /* ---- access-control mutations (POST/DELETE the mgmt API) ---- */
    async accessCall(method, url, body) {
      this.accessMsg = '';
      try {
        const r = await fetch(url, {
          method,
          headers: body ? { 'Content-Type': 'application/json' } : undefined,
          body: body ? JSON.stringify(body) : undefined,
        });
        if (!r.ok) {
          const e = await r.json().catch(() => ({}));
          this.accessMsg = 'Error ' + r.status + ': ' + (e.error || r.statusText);
          return false;
        }
        await this.fetchAll();
        return true;
      } catch (e) {
        this.accessMsg = 'Request failed: ' + ((e && e.message) || e);
        return false;
      }
    },
    addVhost() {
      if (!this.newVhost) return;
      this.accessCall('POST', '/api/vhosts', { name: this.newVhost })
        .then((ok) => { if (ok) this.newVhost = ''; });
    },
    delVhost(v) {
      if (!confirm('Delete vhost "' + v + '"? Its permissions are removed.')) return;
      this.accessCall('DELETE', '/api/vhosts/' + encodeURIComponent(v));
    },
    addUser() {
      const u = this.newUser;
      if (!u.name || !u.password) { this.accessMsg = 'Username and password are required.'; return; }
      this.accessCall('POST', '/api/users',
                      { name: u.name, password: u.password, tags: u.admin ? 'administrator' : '' })
        .then((ok) => { if (ok) this.newUser = { name: '', password: '', admin: false }; });
    },
    delUser(name) {
      if (!confirm('Delete user "' + name + '"? Their permissions are removed.')) return;
      this.accessCall('DELETE', '/api/users/' + encodeURIComponent(name));
    },
    setPerm() {
      const pm = this.newPerm;
      if (!pm.user || !pm.vhost) { this.accessMsg = 'Pick a user and a vhost.'; return; }
      this.accessCall('POST', '/api/permissions', {
        user: pm.user, vhost: pm.vhost,
        configure: pm.configure, write: pm.write, read: pm.read,
      });
    },
    delPerm(pm) {
      if (!confirm('Clear permissions of "' + pm.user + '" on "' + pm.vhost + '"?')) return;
      this.accessCall('DELETE', '/api/permissions/' +
                      encodeURIComponent(pm.user) + '/' + encodeURIComponent(pm.vhost));
    },

    async fetchAll() {
      this.loading = true;
      try {
        /* allSettled: one slow/failed endpoint must not discard the others'
         * data nor falsely flip the connection state to offline. */
        const [ovR, qsR, xsR, csR, clR, vhR, usR, pmR, meR] = await Promise.allSettled([
          fetch('/api/overview').then((r) => r.json()),
          fetch('/api/queues').then((r) => r.json()),
          fetch('/api/exchanges').then((r) => r.json()),
          fetch('/api/connections').then((r) => r.json()),
          fetch('/api/cluster').then((r) => r.json()),
          fetch('/api/vhosts').then((r) => r.json()),
          fetch('/api/users').then((r) => r.json()),
          fetch('/api/permissions').then((r) => r.json()),
          fetch('/api/whoami').then((r) => r.json()),
        ]);
        const ov = ovR.status === 'fulfilled' ? ovR.value : null;
        const arr = (r) => (r.status === 'fulfilled' && Array.isArray(r.value)) ? r.value : null;

        this.connected = ov !== null;
        if (ov === null) { this.error = 'broker unreachable'; return; }
        this.error = '';
        this.cluster = (clR.status === 'fulfilled' && clR.value && clR.value.enabled)
          ? clR.value : null;
        this.vhosts = arr(vhR) || this.vhosts;
        this.users  = arr(usR) || this.users;
        this.perms  = arr(pmR) || this.perms;
        if (meR.status === 'fulfilled' && meR.value && meR.value.level)
          this.me = meR.value;
        this.process(ov, arr(qsR), arr(xsR), arr(csR));
      } catch (e) {
        this.connected = false;
        this.error = String((e && e.message) || e);
        console.error('BeaverMQ: failed to fetch API', e);
      } finally {
        this.loading = false;
      }
    },

    process(ov, qs, xs, cs) {
      const now = Date.now();
      const dt = this._prev ? (now - this._prev.time) / 1000 : 0;

      const pub  = (ov.message_stats && ov.message_stats.publish) || 0;
      const deq  = (ov.queue_totals && ov.queue_totals.total_dequeued) || 0;
      const recv = (ov.network && ov.network.bytes_received) || 0;
      const sent = (ov.network && ov.network.bytes_sent) || 0;
      if (dt > 0 && this._prev) {
        this.publishRate = Math.max(0, (pub - this._prev.publish) / dt);
        this.deliverRate = Math.max(0, (deq - this._prev.deq) / dt);
        this.netInRate   = Math.max(0, (recv - this._prev.recv) / dt);
        this.netOutRate  = Math.max(0, (sent - this._prev.sent) / dt);
      }
      this._prev = { publish: pub, deq, recv, sent, time: now };

      if (qs) {
        const seen = new Set();
        for (const q of qs) {
          seen.add(q.name);
          let h = this.queueHist[q.name];
          if (!h) { h = { prevEnq: q.enqueued, prevDeq: q.dequeued, inRate: 0, outRate: 0, depths: [] };
                    this.queueHist[q.name] = h; }
          if (dt > 0) {
            h.inRate = Math.max(0, (q.enqueued - h.prevEnq) / dt);
            h.outRate = Math.max(0, (q.dequeued - h.prevDeq) / dt);
          }
          h.prevEnq = q.enqueued; h.prevDeq = q.dequeued;
          h.depths.push(q.messages);
          if (h.depths.length > SPARK_N) h.depths.shift();
        }
        for (const name of Object.keys(this.queueHist)) if (!seen.has(name)) delete this.queueHist[name];
        this.queues = qs;
      }
      this.overview = ov;
      if (xs) this.exchanges = xs;
      if (cs) this.connections = cs;

      const H = this.history;
      H.t.push(new Date(now).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }));
      H.publish.push(Math.round(this.publishRate));
      H.deliver.push(Math.round(this.deliverRate));
      H.ready.push((ov.queue_totals && ov.queue_totals.messages_ready) || 0);
      H.netIn.push(this.netInRate);
      H.netOut.push(this.netOutRate);
      H.conns.push((ov.object_totals && ov.object_totals.connections) || 0);
      H.cons.push((qs || this.queues).reduce((s, q) => s + (q.consumers || 0), 0));
      for (const k of Object.keys(H)) if (H[k].length > HIST_N) H[k].shift();

      this.lastUpdated = new Date().toLocaleTimeString();
    },

    onKey(e) { if (e.key === 'Escape' && this.detail) this.closeDetail(); },
    onHash() { const v = (location.hash || '').replace(/^#/, ''); if (VIEWS.includes(v)) this.view = v; },
  },

  watch: {
    theme(v) { localStorage.setItem('bmq.theme', v); this.applyTheme(); this.chartKey++; },
    view(v) { localStorage.setItem('bmq.view', v); this.sidebarOpen = false;
              if (location.hash.replace(/^#/, '') !== v) location.hash = v; },
    refreshMs(v) { localStorage.setItem('bmq.refresh', v); this.restartTimer(); },
    'tables.queues.search'() { this.tables.queues.page = 1; },
    'tables.exchanges.search'() { this.tables.exchanges.page = 1; },
    'tables.connections.search'() { this.tables.connections.page = 1; },
  },

  mounted() {
    this.applyTheme();
    this.fetchAll();
    this.restartTimer();
    window.addEventListener('keydown', this.onKey);
    window.addEventListener('hashchange', this.onHash);
    if (VIEWS.includes(this.view) && location.hash.replace(/^#/, '') !== this.view) location.hash = this.view;
  },
  unmounted() {
    if (this._timer) clearInterval(this._timer);
    window.removeEventListener('keydown', this.onKey);
    window.removeEventListener('hashchange', this.onHash);
  },
});

/* ---- components ---------------------------------------------------------- */
app.component('app-icon', {
  props: { name: String, size: { type: Number, default: 18 } },
  computed: { inner() { return ICONS[this.name] || ''; } },
  template: `<svg :width="size" :height="size" viewBox="0 0 24 24" fill="none"
    stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"
    v-html="inner" aria-hidden="true"></svg>`,
});

app.component('stat-tile', {
  props: { label: String, value: [String, Number], suffix: String, icon: String,
           variant: { type: String, default: 'c-accent' } },
  template: `<div class="stat">
    <div class="ic" :class="variant"><app-icon :name="icon" :size="22"/></div>
    <div>
      <div class="v num">{{ value }}<span v-if="suffix" class="sfx muted"> {{ suffix }}</span></div>
      <div class="l">{{ label }}</div>
    </div>
  </div>`,
});

app.component('chart-line', {
  props: {
    series: { type: Array, default: () => [] },
    labels: { type: Array, default: () => [] },
    height: { type: Number, default: 240 },
    unit:   { type: String, default: 'msg' },   // 'msg' | 'bytes' | 'count'
    area:   { type: Boolean, default: false },
  },
  template: `<div class="chart" :style="{ height: height + 'px' }"><canvas ref="cv"></canvas></div>`,
  mounted() { this.build(); },
  beforeUnmount() { if (this._c) { this._c.destroy(); this._c = null; } },
  watch: {
    series: { deep: true, handler() { this.sync(); } },
    labels() { this.sync(); },
  },
  methods: {
    css(v) { return getComputedStyle(document.documentElement).getPropertyValue(v).trim(); },
    fmtY(v) { return this.unit === 'bytes' ? fmtBytes(v) : compact(v); },
    fmtTip(v) {
      if (this.unit === 'bytes') return fmtBytes(v) + '/s';
      if (this.unit === 'count') return compact(v);
      return compact(v) + '/s';
    },
    ds() {
      return this.series.map((s) => ({
        label: s.label, data: [...s.data], borderColor: s.color,
        backgroundColor: this.area ? s.color + '22' : 'transparent',
        fill: this.area, tension: 0.35, borderWidth: 2, pointRadius: 0, pointHoverRadius: 3,
      }));
    },
    build() {
      if (typeof Chart === 'undefined') return;
      const tick = this.css('--chart-tick'), grid = this.css('--grid-line');
      /* markRaw keeps the Chart.js instance OUT of Vue's reactive proxy. */
      this._c = markRaw(new Chart(this.$refs.cv, {
        type: 'line',
        data: { labels: [...this.labels], datasets: this.ds() },
        options: {
          responsive: true, maintainAspectRatio: false, animation: false,
          interaction: { intersect: false, mode: 'index' },
          plugins: {
            legend: { display: false },
            tooltip: { padding: 10, boxPadding: 4, usePointStyle: true,
              callbacks: { label: (c) => ` ${c.dataset.label}: ${this.fmtTip(c.parsed.y)}` } },
          },
          scales: {
            x: { grid: { display: false }, border: { display: false },
                 ticks: { color: tick, maxTicksLimit: 6, autoSkip: true, maxRotation: 0, font: { size: 10 } } },
            y: { beginAtZero: true, grid: { color: grid }, border: { display: false },
                 ticks: { color: tick, maxTicksLimit: 5, font: { size: 10 }, callback: (v) => this.fmtY(v) } },
          },
        },
      }));
    },
    sync() {
      if (!this._c) return;
      this._c.data.labels = [...this.labels];
      this._c.data.datasets = this.ds();
      this._c.update('none');
    },
  },
});

app.mount('#app');
