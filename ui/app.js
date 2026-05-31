// placeholder
/* ── app.js — Enterprise Data Platform UI ─────────────────────── */

// ── Simulated data ────────────────────────────────────────────────
const DATA = {
  kpi: { orders: 20, customers: 10, products: 20, anomalies: 3 },

  revenue: {
    labels: ['May 24','May 25','May 26','May 27','May 28','May 29','May 30'],
    actual: [1240, 980, 1560, 1320, 1780, 1450, 1690],
  },

  categories: {
    labels: ["men's clothing", "women's clothing", "electronics", "jewelery"],
    values: [3200, 2800, 4100, 1500],
  },

  topProducts: [
    { name: 'Fjallraven Backpack',      category: "men's clothing",   revenue: 820 },
    { name: 'Mens Casual Premium Slim', category: "men's clothing",   revenue: 640 },
    { name: 'WD 2TB Elements',          category: 'electronics',      revenue: 590 },
    { name: 'SanDisk 1TB Flash Drive',  category: 'electronics',      revenue: 510 },
    { name: 'White Gold Diamond Ring',  category: 'jewelery',         revenue: 480 },
  ],

  pipelineSteps: [
    { id: 'ingest_orders',    label: 'Ingest Orders',    icon: '📦', status: 'success' },
    { id: 'ingest_customers', label: 'Ingest Customers', icon: '👥', status: 'success' },
    { id: 'ingest_products',  label: 'Ingest Products',  icon: '🛍️', status: 'success' },
    { id: 'clean_data',       label: 'Clean (Silver)',   icon: '🧹', status: 'success' },
    { id: 'transform',        label: 'Transform (Gold)', icon: '⚙️', status: 'success' },
    { id: 'validate',         label: 'Validate',         icon: '✅', status: 'success' },
    { id: 'load_warehouse',   label: 'Load Warehouse',   icon: '🏛️', status: 'success' },
  ],

  qualitySteps: [
    { id: 'quality_checks',  label: 'Quality Checks',  icon: '🔍', status: 'success' },
    { id: 'detect_anomalies',label: 'Detect Anomalies',icon: '⚠️', status: 'success' },
    { id: 'run_forecasts',   label: 'Run Forecasts',   icon: '📈', status: 'success' },
    { id: 'notify_summary',  label: 'Notify Summary',  icon: '📣', status: 'success' },
  ],

  warehouseTables: [
    { name: 'dim_customer',     type: 'Dimension',   desc: 'Customer SCD Type 1, PII hashed',          status: 'loaded' },
    { name: 'dim_product',      type: 'Dimension',   desc: 'Product catalogue with value score',        status: 'loaded' },
    { name: 'dim_date',         type: 'Dimension',   desc: 'Date spine YYYYMMDD integer key',           status: 'loaded' },
    { name: 'fact_sales',       type: 'Fact',        desc: 'Order line items, grain = order × product', status: 'loaded' },
    { name: 'agg_daily_sales',  type: 'Aggregate',   desc: 'Daily revenue KPIs',                        status: 'loaded' },
    { name: 'agg_product_perf', type: 'Aggregate',   desc: 'Revenue & units sold per product',          status: 'loaded' },
    { name: 'agg_customer_ltv', type: 'Aggregate',   desc: 'Lifetime value tier per customer',          status: 'loaded' },
    { name: 'anomaly_log',      type: 'Operational', desc: 'Flagged anomalies with severity',           status: 'loaded' },
    { name: 'forecast_results', type: 'Operational', desc: '30-day revenue forecast with CI bands',     status: 'loaded' },
    { name: 'load_audit',       type: 'Operational', desc: 'Load audit trail per table per run',        status: 'loaded' },
    { name: 'v_daily_revenue',  type: 'View',        desc: 'Daily revenue joined with dim_date',        status: 'loaded' },
    { name: 'v_top_products',   type: 'View',        desc: 'Revenue-ranked products per category',      status: 'loaded' },
    { name: 'v_category_revenue',type:'View',        desc: 'Revenue by product category',               status: 'loaded' },
    { name: 'v_customer_overview',type:'View',       desc: 'Customer + LTV tier joined',                status: 'loaded' },
  ],

  anomalies: [
    { time: '2026-05-30 06:31', entity: 'fact_sales',       type: 'revenue_outlier',       metric: 'order_revenue', value: '$892.00', severity: 'high',     resolved: false },
    { time: '2026-05-30 06:31', entity: 'fact_sales',       type: 'quantity_outlier',      metric: 'quantity',      value: '18',      severity: 'medium',   resolved: false },
    { time: '2026-05-30 06:31', entity: 'dim_product',      type: 'price_discrepancy',     metric: 'price_pct_diff',value: '14.2%',   severity: 'medium',   resolved: false },
    { time: '2026-05-29 06:31', entity: 'agg_daily_sales',  type: 'revenue_drop',          metric: 'gross_revenue', value: '$980.00', severity: 'high',     resolved: true  },
    { time: '2026-05-28 06:31', entity: 'agg_customer_ltv', type: 'customer_spend_spike',  metric: 'total_spend',   value: '$1240.00',severity: 'critical', resolved: true  },
    { time: '2026-05-27 06:31', entity: 'fact_sales',       type: 'revenue_outlier',       metric: 'order_revenue', value: '$45.00',  severity: 'low',      resolved: true  },
  ],

  ltv: { labels: ['Low', 'Medium', 'High', 'VIP'], values: [4, 3, 2, 1] },

  forecast: (() => {
    const base = 1500, labels = [], pred = [], lower = [], upper = [];
    const today = new Date('2026-05-30');
    for (let i = 1; i <= 30; i++) {
      const d = new Date(today); d.setDate(d.getDate() + i);
      labels.push(d.toLocaleDateString('en-US', { month:'short', day:'numeric' }));
      const v = base + Math.sin(i * 0.9) * 200 + i * 12 + Math.random() * 80;
      pred.push(+v.toFixed(2));
      lower.push(+(v * 0.88).toFixed(2));
      upper.push(+(v * 1.12).toFixed(2));
    }
    return { labels, pred, lower, upper };
  })(),

  services: [
    { name: 'Airflow UI',     icon: '🌀', url: 'http://localhost:8080', user: 'admin',     pass: 'admin',      desc: 'DAG orchestration',       color: '#6366f1' },
    { name: 'MinIO Console',  icon: '🗄️', url: 'http://localhost:9001', user: 'minioadmin', pass: 'minioadmin', desc: 'Data Lake object storage', color: '#f59e0b' },
    { name: 'PostgreSQL',     icon: '🐘', url: 'localhost:5432',        user: 'warehouse',  pass: 'warehouse',  desc: 'Data Warehouse',           color: '#10b981' },
    { name: 'Grafana',        icon: '📊', url: 'http://localhost:3000', user: 'admin',      pass: 'admin',      desc: 'Operational dashboards',   color: '#ef4444' },
    { name: 'Redis',          icon: '⚡', url: 'localhost:6379',        user: '—',          pass: '—',          desc: 'Celery message broker',    color: '#f97316' },
    { name: 'FakeStore API',  icon: '🌐', url: 'https://fakestoreapi.com', user: '—',       pass: '—',          desc: 'Source REST API',          color: '#8b5cf6' },
  ],

  runSteps: [
    { step: 1, title: 'Install dependencies',    cmd: 'pip install -r requirements.txt' },
    { step: 2, title: 'Start all services',       cmd: 'docker-compose up -d' },
    { step: 3, title: 'Ingest Orders',            cmd: 'python api_ingestion/fetch_orders.py' },
    { step: 4, title: 'Ingest Customers',         cmd: 'python api_ingestion/fetch_customers.py' },
    { step: 5, title: 'Ingest Products',          cmd: 'python api_ingestion/fetch_products.py' },
    { step: 6, title: 'Clean data (Silver)',      cmd: 'python etl/clean_data.py' },
    { step: 7, title: 'Transform (Gold)',         cmd: 'python etl/transform.py' },
    { step: 8, title: 'Validate quality',         cmd: 'python etl/validate.py' },
    { step: 9, title: 'Load warehouse',           cmd: 'python etl/load_warehouse.py' },
    { step:10, title: 'Detect anomalies',         cmd: 'python anomaly_detection/anomaly.py' },
    { step:11, title: 'Generate forecast',        cmd: 'python forecasting/forecast.py' },
  ],
};

// ── Charts registry ───────────────────────────────────────────────
const charts = {};

function destroyChart(id) {
  if (charts[id]) { charts[id].destroy(); delete charts[id]; }
}

// ── Chart defaults ────────────────────────────────────────────────
Chart.defaults.color = '#8892a4';
Chart.defaults.borderColor = '#2a3347';
Chart.defaults.font.family = 'Inter, system-ui, sans-serif';

// ── Navigation ────────────────────────────────────────────────────
function navigate(page) {
  document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.page').forEach(el => el.classList.remove('active'));

  const navEl = document.querySelector(`[data-page="${page}"]`);
  const pageEl = document.getElementById(`page-${page}`);
  if (navEl)  navEl.classList.add('active');
  if (pageEl) pageEl.classList.add('active');

  const titles = {
    dashboard: 'Dashboard',   pipeline: 'ETL Pipeline',
    datalake:  'Data Lake',   warehouse: 'Warehouse',
    anomalies: 'Anomalies',   forecast: 'Forecast',
    services:  'Services',    run: 'Run Pipeline',
  };
  document.getElementById('pageTitle').textContent = titles[page] || page;
  document.getElementById('breadcrumb').textContent = titles[page] || page;

  renderPage(page);
}

// ── Render dispatcher ─────────────────────────────────────────────
function renderPage(page) {
  const fn = {
    dashboard: renderDashboard,
    pipeline:  renderPipeline,
    datalake:  renderDatalake,
    warehouse: renderWarehouse,
    anomalies: renderAnomalies,
    forecast:  renderForecast,
    services:  renderServices,
    run:       renderRun,
  }[page];
  if (fn) fn();
}

// ── DASHBOARD ─────────────────────────────────────────────────────
function renderDashboard() {
  const k = DATA.kpi;
  document.getElementById('kpi-orders').textContent    = k.orders;
  document.getElementById('kpi-customers').textContent = k.customers;
  document.getElementById('kpi-products').textContent  = k.products;
  document.getElementById('kpi-anomalies').textContent = k.anomalies;

  // Revenue chart
  destroyChart('revenue');
  charts.revenue = new Chart(document.getElementById('revenueChart'), {
    type: 'line',
    data: {
      labels: DATA.revenue.labels,
      datasets: [{
        label: 'Revenue ($)',
        data: DATA.revenue.actual,
        borderColor: '#6366f1',
        backgroundColor: 'rgba(99,102,241,0.12)',
        fill: true,
        tension: 0.4,
        pointBackgroundColor: '#6366f1',
        pointRadius: 4,
      }],
    },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { color: '#2a334740' } },
        y: { grid: { color: '#2a334740' }, ticks: { callback: v => '$' + v } },
      },
    },
  });

  // Category donut
  destroyChart('category');
  charts.category = new Chart(document.getElementById('categoryChart'), {
    type: 'doughnut',
    data: {
      labels: DATA.categories.labels,
      datasets: [{
        data: DATA.categories.values,
        backgroundColor: ['#6366f1','#10b981','#f59e0b','#ef4444'],
        borderWidth: 2,
        borderColor: '#161b27',
      }],
    },
    options: {
      responsive: true,
      plugins: { legend: { position: 'bottom', labels: { padding: 12, boxWidth: 12 } } },
      cutout: '65%',
    },
  });

  // Pipeline status
  const list = document.getElementById('pipelineStatusList');
  list.innerHTML = DATA.pipelineSteps.map(s => `
    <div class="pipeline-status-item">
      <span class="ps-icon">${s.icon}</span>
      <span class="ps-name">${s.label}</span>
      <span class="ps-status badge badge-green">✓ ${s.status}</span>
    </div>`).join('');

  // Top products table
  const tbody = document.querySelector('#topProductsTable tbody');
  tbody.innerHTML = DATA.topProducts.map((p, i) => `
    <tr>
      <td>${i + 1}</td>
      <td>${p.name}</td>
      <td><span class="badge badge-blue">${p.category}</span></td>
      <td>$${p.revenue}</td>
    </tr>`).join('');
}

// ── PIPELINE ──────────────────────────────────────────────────────
function renderPipeline() {
  function buildDag(steps, containerId) {
    const el = document.getElementById(containerId);
    // First 3 are parallel, rest sequential
    const parallel = steps.slice(0, 3);
    const sequential = steps.slice(3);

    let html = `<div class="dag-parallel">`;
    parallel.forEach(s => {
      html += `<div class="dag-node ${s.status}">
        <span class="node-icon">${s.icon}</span>
        <span>${s.label}</span>
        <span class="node-status">${s.status}</span>
      </div>`;
    });
    html += `</div>`;

    sequential.forEach(s => {
      html += `<span class="dag-arrow">→</span>
      <div class="dag-node ${s.status}">
        <span class="node-icon">${s.icon}</span>
        <span>${s.label}</span>
        <span class="node-status">${s.status}</span>
      </div>`;
    });
    el.innerHTML = html;
  }

  buildDag(DATA.pipelineSteps, 'dagFlow');
  buildDag(DATA.qualitySteps, 'dagFlow2');

  document.getElementById('pipelineLog').textContent = [
    '[06:00:01] etl_pipeline triggered',
    '[06:00:02] ingest_orders    ✓  20 records → raw/orders/date=2026-05-30/',
    '[06:00:03] ingest_customers ✓  10 records → raw/customers/date=2026-05-30/',
    '[06:00:03] ingest_products  ✓  20 records → raw/products/date=2026-05-30/',
    '[06:00:05] clean_data       ✓  Silver Parquet written',
    '[06:00:07] transform        ✓  7 Gold tables written',
    '[06:00:08] validate         ✓  12 passed, 0 warned, 0 failed',
    '[06:00:10] load_warehouse   ✓  247 rows upserted across 7 tables',
    '[06:30:01] quality_check triggered',
    '[06:30:02] run_quality_checks ✓  all checks passed',
    '[06:30:03] detect_anomalies   ✓  3 anomalies flagged',
    '[06:30:05] run_forecasts      ✓  30-day forecast written',
    '[06:30:06] notify_summary     ✓  audit row written',
  ].join('\n');
}

// ── DATA LAKE ─────────────────────────────────────────────────────
function renderDatalake() {
  document.getElementById('lakeLayers').innerHTML = `
    <div class="lake-layer">
      <div class="lake-layer-title" style="color:#f59e0b">🟤 Bronze — Raw</div>
      <div class="lake-layer-meta">
        Format: JSON<br>
        Prefix: raw/&lt;entity>/date=YYYY-MM-DD/<br>
        Contents: Raw API responses + _meta<br>
        Retention: Indefinite
      </div>
    </div>
    <div class="lake-layer">
      <div class="lake-layer-title" style="color:#8892a4">⚪ Silver — Processed</div>
      <div class="lake-layer-meta">
        Format: Parquet<br>
        Prefix: processed/<entity>/date=YYYY-MM-DD/<br>
        Contents: Cleaned, validated, typed<br>
        PII: SHA-256 hashed
      </div>
    </div>
    <div class="lake-layer">
      <div class="lake-layer-title" style="color:#f59e0b">🟡 Gold — Aggregated</div>
      <div class="lake-layer-meta">
        Format: Parquet<br>
        Prefix: processed/gold/<table>/date=YYYY-MM-DD/<br>
        Contents: Joined, aggregated business tables<br>
        Tables: 7 (dims + facts + aggs)
      </div>
    </div>
    <div class="lake-layer">
      <div class="lake-layer-title" style="color:#10b981">📋 Reports</div>
      <div class="lake-layer-meta">
        Format: JSON<br>
        Prefix: quality_reports/date=YYYY-MM-DD/<br>
        Contents: Validation audit reports<br>
        Also: processed/forecasts/*.parquet
      </div>
    </div>`;

  document.getElementById('fileTree').innerHTML = `
    <div class="folder">📁 enterprise-lake/</div>
    <div class="indent"><div class="folder">📁 raw/</div>
      <div class="indent">
        <div class="folder">📁 orders/date=2026-05-30/</div>
        <div class="indent"><div class="file">📄 orders_20260530T060001Z.json</div></div>
        <div class="folder">📁 customers/date=2026-05-30/</div>
        <div class="indent"><div class="file">📄 customers_20260530T060002Z.json</div></div>
        <div class="folder">📁 products/date=2026-05-30/</div>
        <div class="indent"><div class="file">📄 products_20260530T060003Z.json</div></div>
      </div>
      <div class="folder">📁 processed/</div>
      <div class="indent">
        <div class="folder">📁 orders/date=2026-05-30/</div>
        <div class="indent"><div class="file">📄 orders_20260530T060005Z.parquet</div></div>
        <div class="folder">📁 gold/fact_sales/date=2026-05-30/</div>
        <div class="indent"><div class="file">📄 fact_sales_20260530T060007Z.parquet</div></div>
        <div class="folder">📁 forecasts/</div>
        <div class="indent"><div class="file">📄 daily_revenue_20260530T063005Z.parquet</div></div>
      </div>
      <div class="folder">📁 quality_reports/date=2026-05-30/</div>
      <div class="indent"><div class="file">📄 report_20260530T060008Z.json</div></div>
    </div>`;
}

// ── WAREHOUSE ─────────────────────────────────────────────────────
function renderWarehouse() {
  const typeColors = { Dimension:'badge-blue', Fact:'badge-green', Aggregate:'badge-yellow', Operational:'badge-orange', View:'badge-red' };
  const tbody = document.querySelector('#warehouseTable tbody');
  tbody.innerHTML = DATA.warehouseTables.map(t => `
    <tr>
      <td><code>${t.name}</code></td>
      <td><span class="badge ${typeColors[t.type] || 'badge-blue'}">${t.type}</span></td>
      <td>${t.desc}</td>
      <td><span class="badge badge-green">✓ loaded</span></td>
    </tr>`).join('');

  destroyChart('ltv');
  charts.ltv = new Chart(document.getElementById('ltvChart'), {
    type: 'doughnut',
    data: {
      labels: DATA.ltv.labels,
      datasets: [{
        data: DATA.ltv.values,
        backgroundColor: ['#10b981','#6366f1','#f59e0b','#ef4444'],
        borderWidth: 2, borderColor: '#161b27',
      }],
    },
    options: {
      responsive: true,
      plugins: { legend: { position: 'bottom', labels: { padding: 12, boxWidth: 12 } } },
      cutout: '60%',
    },
  });

  destroyChart('dailySales');
  charts.dailySales = new Chart(document.getElementById('dailySalesChart'), {
    type: 'bar',
    data: {
      labels: DATA.revenue.labels,
      datasets: [{
        label: 'Gross Revenue',
        data: DATA.revenue.actual,
        backgroundColor: 'rgba(99,102,241,0.7)',
        borderRadius: 5,
      }],
    },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { display: false } },
        y: { grid: { color: '#2a334740' }, ticks: { callback: v => '$' + v } },
      },
    },
  });
}

// ── ANOMALIES ─────────────────────────────────────────────────────
function renderAnomalies() {
  const counts = { critical: 0, high: 0, medium: 0, low: 0 };
  DATA.anomalies.forEach(a => { if (!a.resolved) counts[a.severity] = (counts[a.severity] || 0) + 1; });
  document.getElementById('anom-critical').textContent = counts.critical;
  document.getElementById('anom-high').textContent     = counts.high;
  document.getElementById('anom-medium').textContent   = counts.medium;
  document.getElementById('anom-low').textContent      = counts.low;

  renderAnomalyTable();

  document.getElementById('anomSeverityFilter').onchange = renderAnomalyTable;
  document.getElementById('anomTypeFilter').onchange     = renderAnomalyTable;

  document.getElementById('methodsGrid').innerHTML = [
    { name: 'Z-Score',        desc: 'Flags records > 3σ from mean. Used for order revenue, customer spend.' },
    { name: 'IQR Fence',      desc: 'Flags records outside Q3 + 1.5×IQR. Robust to heavy-tailed distributions.' },
    { name: 'Day-over-Day',   desc: 'Flags daily revenue > 40% below 7-day rolling average.' },
    { name: 'Price Check',    desc: 'Flags products where avg sold price differs > 10% from catalogue.' },
    { name: 'Spend Spike',    desc: 'Flags customers with orders or spend > 3σ above platform average.' },
  ].map(m => `
    <div class="method-card">
      <div class="method-name">${m.name}</div>
      <div class="method-desc">${m.desc}</div>
    </div>`).join('');
}

function renderAnomalyTable() {
  const sevFilter  = document.getElementById('anomSeverityFilter').value;
  const typeFilter = document.getElementById('anomTypeFilter').value;
  const filtered   = DATA.anomalies.filter(a =>
    (!sevFilter  || a.severity === sevFilter) &&
    (!typeFilter || a.type === typeFilter)
  );
  const tbody = document.querySelector('#anomalyTable tbody');
  tbody.innerHTML = filtered.map(a => `
    <tr>
      <td>${a.time}</td>
      <td><code>${a.entity}</code></td>
      <td>${a.type.replace(/_/g,' ')}</td>
      <td>${a.metric}</td>
      <td>${a.value}</td>
      <td><span class="sev-${a.severity}">${a.severity}</span></td>
      <td>${a.resolved ? '<span class="badge badge-green">resolved</span>' : '<span class="badge badge-red">open</span>'}</td>
    </tr>`).join('');
}

// ── FORECAST ──────────────────────────────────────────────────────
function renderForecast() {
  const f = DATA.forecast;

  destroyChart('forecast');
  charts.forecast = new Chart(document.getElementById('forecastChart'), {
    type: 'line',
    data: {
      labels: f.labels,
      datasets: [
        {
          label: 'Predicted',
          data: f.pred,
          borderColor: '#6366f1',
          backgroundColor: 'rgba(99,102,241,0.08)',
          fill: false,
          tension: 0.4,
          pointRadius: 2,
        },
        {
          label: 'Upper 95%',
          data: f.upper,
          borderColor: 'rgba(16,185,129,0.4)',
          backgroundColor: 'rgba(16,185,129,0.08)',
          fill: '+1',
          tension: 0.4,
          pointRadius: 0,
          borderDash: [4, 4],
        },
        {
          label: 'Lower 95%',
          data: f.lower,
          borderColor: 'rgba(16,185,129,0.4)',
          backgroundColor: 'rgba(16,185,129,0.08)',
          fill: false,
          tension: 0.4,
          pointRadius: 0,
          borderDash: [4, 4],
        },
      ],
    },
    options: {
      responsive: true,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'top', labels: { boxWidth: 12, padding: 16 } } },
      scales: {
        x: { grid: { color: '#2a334740' }, ticks: { maxTicksLimit: 10 } },
        y: { grid: { color: '#2a334740' }, ticks: { callback: v => '$' + v } },
      },
    },
  });

  const tbody = document.querySelector('#forecastTable tbody');
  tbody.innerHTML = f.labels.slice(0, 10).map((lbl, i) => `
    <tr>
      <td>${i + 1}</td>
      <td>${lbl}</td>
      <td>$${f.pred[i]}</td>
      <td>$${f.lower[i]}</td>
      <td>$${f.upper[i]}</td>
    </tr>`).join('');
}

// ── SERVICES ──────────────────────────────────────────────────────
// ── SERVICES (continued) ──────────────────────────────────────────
function renderServices() {
  document.getElementById('servicesGrid').innerHTML = DATA.services.map(s => `
    <div class="service-card">
      <div class="service-card-header">
        <span class="service-icon">${s.icon}</span>
        <div>
          <div class="service-name">${s.name}</div>
          <div class="service-status">
            <span class="status-dot online"></span> running
          </div>
        </div>
      </div>
      <div style="font-size:.78rem;color:var(--text-muted);margin:.25rem 0">${s.desc}</div>
      ${s.url.startsWith('http') ? `<a href="${s.url}" target="_blank" class="service-url">${s.url}</a>` : `<span class="service-url">${s.url}</span>`}
      <div class="service-creds">user: ${s.user} &nbsp;|&nbsp; pass: ${s.pass}</div>
    </div>`).join('');

  document.getElementById('archDiagram').textContent = `
┌───────────────────────────────────────────────────────────────────────┐
│                          REST APIs                                    │
│           Orders (carts)  ·  Customers (users)  ·  Products          │
└────────────────────────────┬──────────────────────────────────────────┘
                             │ JSON
                             ▼
┌───────────────────────────────────────────────────────────────────────┐
│                   MinIO Data Lake  (S3-compatible)                    │
│                                                                       │
│  ┌──────────────┐   clean    ┌──────────────┐   agg    ┌──────────┐  │
│  │    Bronze    │  ────────► │    Silver    │ ───────► │   Gold   │  │
│  │  raw/ JSON   │            │  processed/  │          │  gold/   │  │
│  │  as-received │            │  Parquet     │          │  Parquet │  │
│  └──────────────┘            └──────────────┘          └──────────┘  │
│                                                                       │
│  quality_reports/   forecasts/                                        │
└──────────────────────────────────────┬────────────────────────────────┘
                                       │ upsert
                                       ▼
┌───────────────────────────────────────────────────────────────────────┐
│                    PostgreSQL Data Warehouse                          │
│                                                                       │
│  dim_customer  dim_product  dim_date   ◄── Dimensions                 │
│  fact_sales                            ◄── Fact                      │
│  agg_daily_sales  agg_product_perf  agg_customer_ltv  ◄── Aggregates │
│  anomaly_log  forecast_results  load_audit  ◄── Operational          │
│  v_daily_revenue  v_category_revenue  v_top_products  ◄── Views      │
└──────────────────────────────────────┬────────────────────────────────┘
                                       │ DirectQuery / Import
                                       ▼
                              ┌─────────────────┐
                              │    Power BI      │
                              │    Dashboard     │
                              └─────────────────┘

Anomaly Detection ─► anomaly_log (written to warehouse)
Forecasting       ─► forecast_results (warehouse + MinIO Parquet)
Orchestration     ─► Apache Airflow (2 DAGs)
`;
}

// ── RUN PIPELINE ──────────────────────────────────────────────────
function renderRun() {
  document.getElementById('runSteps').innerHTML = DATA.runSteps.map(s => `
    <div class="run-step">
      <div class="run-step-header">
        <span class="run-step-title">Step ${s.step}: ${s.title}</span>
      </div>
      <div class="run-step-cmd">
        <code>${s.cmd}</code>
        <button class="btn-copy" onclick="copyToClipboard('${s.cmd.replace(/'/g, "\\'")}')">Copy</button>
      </div>
    </div>`).join('');

  document.getElementById('dockerCmds').innerHTML = [
    { label: 'Start all services',    cmd: 'docker-compose up -d' },
    { label: 'Stop all services',     cmd: 'docker-compose down' },
    { label: 'View logs',             cmd: 'docker-compose logs -f' },
    { label: 'Restart services',      cmd: 'docker-compose restart' },
    { label: 'Check service status',  cmd: 'docker-compose ps' },
  ].map(d => `
    <div class="docker-cmd">
      <code>${d.cmd}</code>
      <button class="btn-copy" onclick="copyToClipboard('${d.cmd}')">Copy</button>
    </div>`).join('');
}

// ── Copy to clipboard ─────────────────────────────────────────────
function copyToClipboard(text) {
  navigator.clipboard.writeText(text).then(() => {
    showToast('Copied to clipboard!');
  }).catch(err => {
    console.error('Copy failed:', err);
  });
}

function showToast(message) {
  const toast = document.createElement('div');
  toast.className = 'toast';
  toast.textContent = message;
  document.body.appendChild(toast);
  setTimeout(() => toast.remove(), 2000);
}

// ── Sidebar toggle ────────────────────────────────────────────────
document.getElementById('sidebarToggle').addEventListener('click', () => {
  document.getElementById('sidebar').classList.toggle('collapsed');
});

// ── Navigation listeners ──────────────────────────────────────────
document.querySelectorAll('.nav-item').forEach(item => {
  item.addEventListener('click', (e) => {
    e.preventDefault();
    const page = item.getAttribute('data-page');
    navigate(page);
  });
});

// ── Trigger run button ────────────────────────────────────────────
document.getElementById('triggerRunBtn').addEventListener('click', () => {
  showToast('Pipeline triggered! Check Airflow UI at http://localhost:8080');
  document.getElementById('lastRunTime').textContent = new Date().toLocaleTimeString();
});

// ── Initialize ────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  navigate('dashboard');
  document.getElementById('lastRunTime').textContent = '06:00:10 UTC';
});
