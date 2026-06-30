/* =====================================================================
   CRCE FINANCE CLUB — MOCK TRADING TERMINAL
   ---------------------------------------------------------------------
   PERFORMANCE ARCHITECTURE (the whole point of this build):

     MockWebSocket (fires every ~100ms)
            │  raw ticks, very high frequency
            ▼
     dataBuffer (Map<symbol, latestTick>)   <-- "keep latest value only"
            │  +  dirtySymbols (Set<symbol>)
            ▼
     requestAnimationFrame loop, gated by a 300-500ms throttle window
            │  only fires the actual DOM patch when the window elapses
            ▼
     renderFrame() → patches ONLY the DOM nodes for symbols that
                     actually changed since the last paint. No full
                     re-render of the 50-card grid, ever.

   This decouples "how often data arrives" from "how often the DOM
   is touched" — the classic high-frequency-stream UI pattern used by
   real trading terminals (Kite, TradingView, Bloomberg).
   ===================================================================== */

'use strict';

/* =====================================================================
   1. CONFIG
   ===================================================================== */
const CONFIG = {
  TICK_INTERVAL_MS: 100,      // mock "WebSocket" message frequency
  RENDER_INTERVAL_MS: 400,    // throttle window for DOM paints (300-500ms range)
  TICKS_PER_MESSAGE: [4, 14], // how many random stocks move per tick
  MAX_PRICE_STEP_PCT: 0.45,   // max % a stock can move in a single tick
  WATCHLIST_SIZE: 12,
  CHART_HISTORY_POINTS: 60,
  NOTIFY_THRESHOLD_PCT: 2.4,  // |change%| beyond which we may toast
  NOTIFY_COOLDOWN_MS: 15000,  // per-symbol cooldown so toasts don't spam
};

/* =====================================================================
   2. MOCK STOCK UNIVERSE (50 NSE-style instruments)
   ===================================================================== */
const STOCK_UNIVERSE = [
  ['RELIANCE', 'Reliance Industries Ltd.', 'Energy', 2890],
  ['TCS', 'Tata Consultancy Services', 'IT', 3845],
  ['HDFCBANK', 'HDFC Bank Ltd.', 'Banking', 1652],
  ['INFY', 'Infosys Ltd.', 'IT', 1538],
  ['ICICIBANK', 'ICICI Bank Ltd.', 'Banking', 1198],
  ['HINDUNILVR', 'Hindustan Unilever Ltd.', 'FMCG', 2412],
  ['SBIN', 'State Bank of India', 'Banking', 812],
  ['BHARTIARTL', 'Bharti Airtel Ltd.', 'Telecom', 1574],
  ['ITC', 'ITC Ltd.', 'FMCG', 462],
  ['KOTAKBANK', 'Kotak Mahindra Bank', 'Banking', 1789],
  ['LT', 'Larsen & Toubro Ltd.', 'Infra', 3621],
  ['AXISBANK', 'Axis Bank Ltd.', 'Banking', 1142],
  ['BAJFINANCE', 'Bajaj Finance Ltd.', 'NBFC', 7104],
  ['ASIANPAINT', 'Asian Paints Ltd.', 'Consumer', 2865],
  ['MARUTI', 'Maruti Suzuki India', 'Auto', 12480],
  ['SUNPHARMA', 'Sun Pharma Industries', 'Pharma', 1734],
  ['TITAN', 'Titan Company Ltd.', 'Consumer', 3412],
  ['ULTRACEMCO', 'UltraTech Cement Ltd.', 'Cement', 11280],
  ['WIPRO', 'Wipro Ltd.', 'IT', 542],
  ['NESTLEIND', 'Nestle India Ltd.', 'FMCG', 2398],
  ['ONGC', 'Oil & Natural Gas Corp.', 'Energy', 262],
  ['NTPC', 'NTPC Ltd.', 'Power', 368],
  ['POWERGRID', 'Power Grid Corp.', 'Power', 318],
  ['TATAMOTORS', 'Tata Motors Ltd.', 'Auto', 968],
  ['TATASTEEL', 'Tata Steel Ltd.', 'Metals', 162],
  ['JSWSTEEL', 'JSW Steel Ltd.', 'Metals', 932],
  ['ADANIENT', 'Adani Enterprises Ltd.', 'Conglomerate', 3142],
  ['ADANIPORTS', 'Adani Ports & SEZ', 'Infra', 1428],
  ['COALINDIA', 'Coal India Ltd.', 'Mining', 442],
  ['HCLTECH', 'HCL Technologies Ltd.', 'IT', 1812],
  ['TECHM', 'Tech Mahindra Ltd.', 'IT', 1648],
  ['M&M', 'Mahindra & Mahindra', 'Auto', 2962],
  ['DRREDDY', "Dr. Reddy's Laboratories", 'Pharma', 1262],
  ['CIPLA', 'Cipla Ltd.', 'Pharma', 1512],
  ['DIVISLAB', "Divi's Laboratories", 'Pharma', 6124],
  ['GRASIM', 'Grasim Industries Ltd.', 'Cement', 2548,],
  ['BRITANNIA', 'Britannia Industries', 'FMCG', 5384],
  ['EICHERMOT', 'Eicher Motors Ltd.', 'Auto', 4862],
  ['HEROMOTOCO', 'Hero MotoCorp Ltd.', 'Auto', 5142],
  ['BAJAJ-AUTO', 'Bajaj Auto Ltd.', 'Auto', 9482],
  ['SHREECEM', 'Shree Cement Ltd.', 'Cement', 27340],
  ['UPL', 'UPL Ltd.', 'Chemicals', 612],
  ['INDUSINDBK', 'IndusInd Bank Ltd.', 'Banking', 1028],
  ['BAJAJFINSV', 'Bajaj Finserv Ltd.', 'NBFC', 1742],
  ['HDFCLIFE', 'HDFC Life Insurance', 'Insurance', 684],
  ['SBILIFE', 'SBI Life Insurance', 'Insurance', 1542],
  ['APOLLOHOSP', 'Apollo Hospitals', 'Healthcare', 6842,],
  ['TATACONSUM', 'Tata Consumer Products', 'FMCG', 1124],
  ['BPCL', 'Bharat Petroleum Corp.', 'Energy', 318],
  ['VEDANTA', 'Vedanta Ltd.', 'Metals', 462],
];

/* =====================================================================
   3. MOCK WEBSOCKET
   A tiny pub/sub that pretends to be a live exchange feed, emitting
   a batch of price ticks every TICK_INTERVAL_MS. This is the ONLY
   place "high frequency" data is produced.
   ===================================================================== */
class MockWebSocket {
  constructor(symbols) {
    this.symbols = symbols;
    this.listeners = [];
    this.timerId = null;
  }

  onMessage(callback) {
    this.listeners.push(callback);
  }

  connect() {
    this.timerId = setInterval(() => this._emitTick(), CONFIG.TICK_INTERVAL_MS);
  }

  disconnect() {
    clearInterval(this.timerId);
  }

  _emitTick() {
    const [min, max] = CONFIG.TICKS_PER_MESSAGE;
    const count = min + Math.floor(Math.random() * (max - min + 1));
    const movedSymbols = [];

    for (let i = 0; i < count; i++) {
      const symbol = this.symbols[Math.floor(Math.random() * this.symbols.length)];
      movedSymbols.push(symbol);
    }

    const message = { type: 'tick', timestamp: Date.now(), symbols: movedSymbols };
    this.listeners.forEach((cb) => cb(message));
  }
}

/* =====================================================================
   4. APPLICATION STATE
   ===================================================================== */

/** Master record per symbol: the "source of truth" market state. */
const stocks = new Map(); // symbol -> { name, sector, price, prevClose, open, high, low, volume, changePct }

/** Data buffer: latest tick already merged into `stocks`, but the DOM
 *  has not been told about it yet. This is what decouples feed-rate
 *  from paint-rate. */
const dirtySymbols = new Set();

/** DOM element cache so renderFrame() never has to re-query or rebuild
 *  the grid — created once in buildStockGrid(), reused forever. */
const domCache = new Map(); // symbol -> { card, priceEl, changeEl, volEl }
const watchlistDomCache = new Map();

/** Symbols permanently pinned to the watchlist panel. */
let watchlistSymbols = [];

/** Notification cooldown tracker to avoid toast spam. */
const lastNotifiedAt = new Map();

/** Currently featured symbol on the big chart panel. */
let featuredSymbol = 'RELIANCE';
const priceHistory = new Map(); // symbol -> array of recent prices (for sparklines/chart)

/** Mock portfolio holdings: symbol -> { qty, avgPrice } */
const portfolio = new Map([
  ['RELIANCE', { qty: 12, avgPrice: 2750 }],
  ['TCS', { qty: 6, avgPrice: 3700 }],
  ['HDFCBANK', { qty: 20, avgPrice: 1600 }],
  ['INFY', { qty: 15, avgPrice: 1480 }],
  ['ITC', { qty: 80, avgPrice: 440 }],
  ['TATAMOTORS', { qty: 25, avgPrice: 900 }],
  ['BAJFINANCE', { qty: 3, avgPrice: 7300 }],
  ['SBIN', { qty: 30, avgPrice: 780 }],
]);

/* =====================================================================
   5. SEEDING — build the initial market state
   ===================================================================== */
function seedStocks() {
  STOCK_UNIVERSE.forEach(([symbol, name, sector, basePrice]) => {
    const openDrift = (Math.random() - 0.5) * 0.02; // ±1% gap from prevClose
    const prevClose = basePrice;
    const open = +(prevClose * (1 + openDrift)).toFixed(2);

    stocks.set(symbol, {
      name,
      sector,
      price: open,
      prevClose,
      open,
      high: open,
      low: open,
      volume: Math.floor(50000 + Math.random() * 950000),
      changePct: +(((open - prevClose) / prevClose) * 100).toFixed(2),
    });

    priceHistory.set(symbol, Array.from({ length: CONFIG.CHART_HISTORY_POINTS }, () => open));
  });

  // Pick a watchlist: portfolio holdings + a few random extras, capped.
  const holders = [...portfolio.keys()];
  const others = STOCK_UNIVERSE.map((s) => s[0]).filter((s) => !holders.includes(s));
  shuffle(others);
  watchlistSymbols = [...holders, ...others].slice(0, CONFIG.WATCHLIST_SIZE);
}

function shuffle(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

/* =====================================================================
   6. TICK PROCESSING — merge a raw tick into the buffer (NOT the DOM)
   ===================================================================== */
function applyTick(symbol) {
  const s = stocks.get(symbol);
  if (!s) return;

  // Random walk: small % step, biased slightly by recent momentum.
  const stepPct = (Math.random() - 0.5) * 2 * CONFIG.MAX_PRICE_STEP_PCT;
  let newPrice = s.price * (1 + stepPct / 100);
  newPrice = Math.max(newPrice, 1); // floor guard
  newPrice = +newPrice.toFixed(2);

  s.price = newPrice;
  s.high = Math.max(s.high, newPrice);
  s.low = Math.min(s.low, newPrice);
  s.volume += Math.floor(Math.random() * 4000);
  s.changePct = +(((newPrice - s.prevClose) / s.prevClose) * 100).toFixed(2);

  // Track direction for the flash animation at paint time.
  s.lastDirection = stepPct >= 0 ? 'up' : 'down';

  // Maintain a short rolling history for the featured chart / sparklines.
  const hist = priceHistory.get(symbol);
  hist.push(newPrice);
  if (hist.length > CONFIG.CHART_HISTORY_POINTS) hist.shift();

  // Mark dirty — this is the ONLY thing that tells the render loop
  // "this symbol needs a DOM patch on the next paint."
  dirtySymbols.add(symbol);
}

/* =====================================================================
   7. THROTTLED requestAnimationFrame RENDER LOOP
   High-frequency ticks land in `dirtySymbols` continuously. We only
   ever touch the DOM once per RENDER_INTERVAL_MS, and even then we
   only touch nodes that are actually dirty.
   ===================================================================== */
let lastRenderTime = 0;

function rafLoop(timestamp) {
  if (timestamp - lastRenderTime >= CONFIG.RENDER_INTERVAL_MS) {
    renderFrame();
    lastRenderTime = timestamp;
  }
  requestAnimationFrame(rafLoop);
}

function renderFrame() {
  if (dirtySymbols.size === 0) return; // nothing changed — skip entirely

  // Snapshot + clear the dirty set immediately so new ticks arriving
  // mid-paint queue up for the *next* frame instead of being dropped.
  const changed = Array.from(dirtySymbols);
  dirtySymbols.clear();

  changed.forEach(patchStockCard);
  changed.forEach(patchWatchlistRow);

  // These are cheap aggregate recomputations (O(50) at worst) and are
  // intentionally only done once per throttle window, not per tick.
  updateTickerTape();
  updatePortfolioSummary();
  updateTopMovers();
  updateIndices();
  maybeNotify(changed);

  if (changed.includes(featuredSymbol)) {
    updateFeaturedChart();
  }
}

/* =====================================================================
   8. SELECTIVE DOM PATCHING
   Each function below mutates only the specific text nodes / classes
   that changed — never innerHTML of a whole list, never a full rebuild.
   ===================================================================== */
function patchStockCard(symbol) {
  const refs = domCache.get(symbol);
  if (!refs) return;
  const s = stocks.get(symbol);

  refs.priceEl.textContent = formatINR(s.price);
  refs.changeEl.textContent = formatChange(s.changePct);
  refs.changeEl.classList.toggle('is-up', s.changePct >= 0);
  refs.changeEl.classList.toggle('is-down', s.changePct < 0);
  refs.volEl.textContent = formatVolume(s.volume);

  // Flash animation: restart by forcing reflow on class removal.
  refs.card.classList.remove('flash-up', 'flash-down');
  void refs.card.offsetWidth; // force reflow so the animation re-triggers
  refs.card.classList.add(s.lastDirection === 'up' ? 'flash-up' : 'flash-down');
}

function patchWatchlistRow(symbol) {
  const refs = watchlistDomCache.get(symbol);
  if (!refs) return;
  const s = stocks.get(symbol);

  refs.priceEl.textContent = formatINR(s.price);
  refs.changeEl.textContent = formatChange(s.changePct);
  refs.changeEl.classList.toggle('is-up', s.changePct >= 0);
  refs.changeEl.classList.toggle('is-down', s.changePct < 0);

  refs.row.classList.remove('flash-up', 'flash-down');
  void refs.row.offsetWidth;
  refs.row.classList.add(s.lastDirection === 'up' ? 'flash-up' : 'flash-down');
}

/* ---- Ticker tape: cheap full rebuild of a small strip, throttled ---- */
function updateTickerTape() {
  const track = document.getElementById('tickerTrack');
  const subset = STOCK_UNIVERSE.slice(0, 22).map((s) => s[0]);

  const html = subset
    .map((symbol) => {
      const s = stocks.get(symbol);
      const dir = s.changePct >= 0 ? 'up' : 'down';
      const arrow = dir === 'up' ? '▲' : '▼';
      return `<span class="ticker-item ticker-item--${dir}">
        <span class="ticker-item__sym">${symbol}</span>
        <span>${formatINR(s.price)}</span>
        <span class="ticker-item__chg">${arrow} ${formatChange(s.changePct)}</span>
      </span>`;
    })
    .join('');

  // Duplicate content once so the CSS marquee (-50% translateX) loops seamlessly.
  track.innerHTML = html + html;
}

/* ---- Portfolio summary: aggregate compute + animated count-up ---- */
function updatePortfolioSummary() {
  let invested = 0;
  let currentValue = 0;
  let dayPnl = 0;

  portfolio.forEach((holding, symbol) => {
    const s = stocks.get(symbol);
    if (!s) return;
    invested += holding.avgPrice * holding.qty;
    currentValue += s.price * holding.qty;
    dayPnl += (s.price - s.prevClose) * holding.qty;
  });

  const totalPnl = currentValue - invested;
  const totalPnlPct = invested ? (totalPnl / invested) * 100 : 0;
  const dayPnlPct = (currentValue - dayPnl) ? (dayPnl / (currentValue - dayPnl)) * 100 : 0;

  setText('statPortfolioValue', formatINR(currentValue));
  setText('statPortfolioInvested', `Invested ${formatINR(invested)}`);

  const dayPnlEl = document.getElementById('statDayPnl');
  dayPnlEl.textContent = formatSignedINR(dayPnl);
  dayPnlEl.classList.toggle('is-up', dayPnl >= 0);
  dayPnlEl.classList.toggle('is-down', dayPnl < 0);
  const dayPnlPctEl = document.getElementById('statDayPnlPct');
  dayPnlPctEl.textContent = formatChange(dayPnlPct);
  dayPnlPctEl.classList.toggle('is-up', dayPnl >= 0);
  dayPnlPctEl.classList.toggle('is-down', dayPnl < 0);

  const totalPnlEl = document.getElementById('statTotalPnl');
  totalPnlEl.textContent = formatSignedINR(totalPnl);
  totalPnlEl.classList.toggle('is-up', totalPnl >= 0);
  totalPnlEl.classList.toggle('is-down', totalPnl < 0);
  const totalPnlPctEl = document.getElementById('statTotalPnlPct');
  totalPnlPctEl.textContent = formatChange(totalPnlPct);
  totalPnlPctEl.classList.toggle('is-up', totalPnl >= 0);
  totalPnlPctEl.classList.toggle('is-down', totalPnl < 0);

  setText('statPositions', String(portfolio.size));
}

/* ---- Top gainers / losers: small O(n log n) sort over 50 items ---- */
function updateTopMovers() {
  const all = [...stocks.entries()].sort((a, b) => b[1].changePct - a[1].changePct);
  const gainers = all.slice(0, 5);
  const losers = all.slice(-5).reverse();

  renderMoversList('topGainers', gainers);
  renderMoversList('topLosers', losers);
}

function renderMoversList(elId, list) {
  const el = document.getElementById(elId);
  el.innerHTML = list
    .map(([symbol, s]) => {
      const dir = s.changePct >= 0 ? 'up' : 'down';
      return `<li class="mover">
        <div class="mover__left">
          <span class="mover__sym">${symbol}</span>
          <span class="mover__sector">${s.sector}</span>
        </div>
        <div class="mover__right">
          <span class="mover__price">${formatINR(s.price)}</span>
          <span class="mover__change is-${dir}">${formatChange(s.changePct)}</span>
        </div>
      </li>`;
    })
    .join('');
}

/* ---- Market indices: synthetic indices derived from the universe ---- */
const INDEX_DEFS = [
  { id: 'NIFTY50', name: 'NIFTY 50', base: 24850, symbols: STOCK_UNIVERSE.slice(0, 50).map((s) => s[0]) },
  { id: 'SENSEX', name: 'SENSEX', base: 81600, symbols: STOCK_UNIVERSE.slice(0, 30).map((s) => s[0]) },
  { id: 'BANKNIFTY', name: 'BANK NIFTY', base: 53200, symbols: STOCK_UNIVERSE.filter((s) => s[2] === 'Banking').map((s) => s[0]) },
];
const indexHistory = new Map(INDEX_DEFS.map((d) => [d.id, []]));

function updateIndices() {
  INDEX_DEFS.forEach((def) => {
    const avgChangePct =
      def.symbols.reduce((sum, sym) => sum + (stocks.get(sym)?.changePct || 0), 0) / def.symbols.length;
    const value = def.base * (1 + avgChangePct / 100);

    const hist = indexHistory.get(def.id);
    hist.push(value);
    if (hist.length > 40) hist.shift();

    let card = document.getElementById(`index-${def.id}`);
    if (!card) {
      card = buildIndexCard(def.id, def.name);
      document.getElementById('indicesRow').appendChild(card);
    }
    const dir = avgChangePct >= 0 ? 'up' : 'down';
    card.querySelector('.index-card__value').textContent = value.toLocaleString('en-IN', { maximumFractionDigits: 2 });
    const chEl = card.querySelector('.index-card__change');
    chEl.textContent = formatChange(avgChangePct);
    chEl.classList.toggle('is-up', avgChangePct >= 0);
    chEl.classList.toggle('is-down', avgChangePct < 0);

    drawSparkline(card.querySelector('path'), hist, dir);
  });
}

function buildIndexCard(id, name) {
  const card = document.createElement('div');
  card.className = 'card index-card';
  card.id = `index-${id}`;
  card.innerHTML = `
    <div class="index-card__top">
      <span class="index-card__name">${name}</span>
      <span class="index-card__change mono">0.00%</span>
    </div>
    <span class="index-card__value mono">0</span>
    <svg class="index-card__spark" viewBox="0 0 200 40" preserveAspectRatio="none"><path /></svg>
  `;
  return card;
}

function drawSparkline(pathEl, values, dir) {
  if (!pathEl || values.length < 2) return;
  const w = 200, h = 40;
  const min = Math.min(...values), max = Math.max(...values);
  const range = max - min || 1;
  const step = w / (values.length - 1);

  const d = values
    .map((v, i) => `${i === 0 ? 'M' : 'L'}${(i * step).toFixed(1)},${(h - ((v - min) / range) * h).toFixed(1)}`)
    .join(' ');

  pathEl.setAttribute('d', d);
  pathEl.setAttribute('stroke', dir === 'up' ? 'var(--accent-bull)' : 'var(--accent-bear)');
}

/* ---- Featured chart (big animated SVG line chart) ---- */
function updateFeaturedChart() {
  const s = stocks.get(featuredSymbol);
  const hist = priceHistory.get(featuredSymbol);
  if (!s || !hist) return;

  setText('chartSymbol', featuredSymbol);
  setText('chartName', s.name);
  setText('chartLast', formatINR(s.price));

  const deltaEl = document.getElementById('chartDelta');
  const deltaAbs = s.price - s.prevClose;
  deltaEl.textContent = `${deltaAbs >= 0 ? '+' : ''}${deltaAbs.toFixed(2)} (${formatChange(s.changePct)})`;
  deltaEl.classList.toggle('is-up', deltaAbs >= 0);
  deltaEl.classList.toggle('is-down', deltaAbs < 0);

  const W = 1000, H = 280, PAD = 10;
  const min = Math.min(...hist), max = Math.max(...hist);
  const range = max - min || 1;
  const step = (W - PAD * 2) / (hist.length - 1);

  const points = hist.map((v, i) => [
    PAD + i * step,
    PAD + (H - PAD * 2) * (1 - (v - min) / range),
  ]);

  const linePath = points.map((p, i) => `${i === 0 ? 'M' : 'L'}${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(' ');
  const areaPath = `${linePath} L${points[points.length - 1][0].toFixed(1)},${H} L${points[0][0].toFixed(1)},${H} Z`;

  document.getElementById('chartLine').setAttribute('d', linePath);
  document.getElementById('chartArea').setAttribute('d', areaPath);

  const last = points[points.length - 1];
  const dot = document.getElementById('chartDot');
  dot.setAttribute('cx', last[0].toFixed(1));
  dot.setAttribute('cy', last[1].toFixed(1));

  // Faint horizontal gridlines, drawn once-ish (cheap to redo, static shape).
  const gridPts = [0.2, 0.4, 0.6, 0.8].map((f) => `M${PAD},${H * f} L${W - PAD},${H * f}`).join(' ');
  document.getElementById('chartGrid').setAttribute('d', gridPts);
}

/* ---- Floating notifications ---- */
function maybeNotify(changedSymbols) {
  const now = Date.now();
  for (const symbol of changedSymbols) {
    const s = stocks.get(symbol);
    if (Math.abs(s.changePct) < CONFIG.NOTIFY_THRESHOLD_PCT) continue;

    const last = lastNotifiedAt.get(symbol) || 0;
    if (now - last < CONFIG.NOTIFY_COOLDOWN_MS) continue;

    lastNotifiedAt.set(symbol, now);
    pushToast(symbol, s);
    break; // at most one toast per render frame keeps things calm
  }
}

function pushToast(symbol, s) {
  const stack = document.getElementById('toastStack');
  const dir = s.changePct >= 0 ? 'up' : 'down';
  const toast = document.createElement('div');
  toast.className = `toast toast--${dir}`;
  toast.innerHTML = `
    <span class="toast__icon">${dir === 'up' ? '▲' : '▼'}</span>
    <span class="toast__body">
      <span class="toast__title">${symbol} ${dir === 'up' ? 'surging' : 'sliding'} ${formatChange(s.changePct)}</span>
      <span class="toast__msg">Now trading at ${formatINR(s.price)}</span>
    </span>
  `;
  stack.appendChild(toast);

  if (stack.children.length > 4) stack.firstElementChild.remove();

  setTimeout(() => {
    toast.classList.add('toast--out');
    setTimeout(() => toast.remove(), 320);
  }, 4200);
}

/* =====================================================================
   9. ONE-TIME DOM CONSTRUCTION (built once, patched forever after)
   ===================================================================== */
function buildStockGrid() {
  const grid = document.getElementById('stockGrid');
  const frag = document.createDocumentFragment();

  STOCK_UNIVERSE.forEach(([symbol, name, sector]) => {
    const s = stocks.get(symbol);
    const card = document.createElement('article');
    card.className = 'stock-card';
    card.dataset.symbol = symbol;
    card.dataset.search = `${symbol} ${name}`.toLowerCase();
    card.innerHTML = `
      <div class="stock-card__top">
        <div>
          <span class="stock-card__sym">${symbol}</span>
          <span class="stock-card__name">${name}</span>
        </div>
        <span class="stock-card__badge">${sector}</span>
      </div>
      <span class="stock-card__price mono">${formatINR(s.price)}</span>
      <div class="stock-card__bottom">
        <span class="stock-card__change mono">${formatChange(s.changePct)}</span>
        <span class="stock-card__vol mono">Vol ${formatVolume(s.volume)}</span>
      </div>
    `;
    card.addEventListener('click', () => setFeaturedSymbol(symbol));
    frag.appendChild(card);

    domCache.set(symbol, {
      card,
      priceEl: card.querySelector('.stock-card__price'),
      changeEl: card.querySelector('.stock-card__change'),
      volEl: card.querySelector('.stock-card__vol'),
    });
  });

  grid.appendChild(frag);
}

function buildWatchlist() {
  const list = document.getElementById('watchlistList');
  const frag = document.createDocumentFragment();

  watchlistSymbols.forEach((symbol) => {
    const s = stocks.get(symbol);
    const row = document.createElement('div');
    row.className = 'watchlist-row';
    row.dataset.symbol = symbol;
    row.dataset.search = `${symbol} ${s.name}`.toLowerCase();
    row.innerHTML = `
      <div class="watchlist-row__left">
        <span class="watchlist-row__sym">${symbol}</span>
        <span class="watchlist-row__sector">${s.sector}</span>
      </div>
      <div class="watchlist-row__right">
        <span class="watchlist-row__price mono">${formatINR(s.price)}</span>
        <span class="watchlist-row__change mono">${formatChange(s.changePct)}</span>
      </div>
    `;
    row.addEventListener('click', () => setFeaturedSymbol(symbol));
    frag.appendChild(row);

    watchlistDomCache.set(symbol, {
      row,
      priceEl: row.querySelector('.watchlist-row__price'),
      changeEl: row.querySelector('.watchlist-row__change'),
    });
  });

  list.appendChild(frag);
  setText('watchlistCount', String(watchlistSymbols.length));
}

function setFeaturedSymbol(symbol) {
  featuredSymbol = symbol;
  updateFeaturedChart();
}

/* =====================================================================
   10. ANIMATED COUNT-UP (used once on initial load for portfolio value)
   ===================================================================== */
function animateCountUp(elId, targetValue, formatter, durationMs = 1200) {
  const el = document.getElementById(elId);
  const start = performance.now();

  function step(now) {
    const t = Math.min((now - start) / durationMs, 1);
    const eased = 1 - Math.pow(1 - t, 3); // ease-out cubic
    el.textContent = formatter(targetValue * eased);
    if (t < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
}

/* =====================================================================
   11. LIVE CLOCK + MARKET STATUS BADGE
   ===================================================================== */
function tickClock() {
  const now = new Date();
  const istString = now.toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false });
  setText('liveClock', istString);

  // Use formatToParts instead of splitting a formatted string — the
  // exact punctuation/order of toLocaleString output is locale- and
  // browser-dependent (e.g. "Wed, 21:34" vs "Wed 21:34"), so splitting
  // on ", " is unreliable. formatToParts gives structured data instead.
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Kolkata',
    hour12: false,
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
  }).formatToParts(now);
  const getPart = (type) => parts.find((p) => p.type === type)?.value;

  const weekday = getPart('weekday');
  const h = Number(getPart('hour')) % 24; // ICU sometimes returns "24" for midnight
  const m = Number(getPart('minute'));
  const minutesNow = h * 60 + m;
  const isWeekday = !['Sat', 'Sun'].includes(weekday);
  const isOpen = isWeekday && minutesNow >= 9 * 60 + 15 && minutesNow <= 15 * 60 + 30;

  const badge = document.getElementById('marketBadge');
  badge.classList.toggle('market-badge--closed', !isOpen);
  setText('marketBadgeText', isOpen ? 'Market Open' : 'Market Closed');
}

/* =====================================================================
   12. SEARCH (filters grid + watchlist without rebuilding either)
   ===================================================================== */
function setupSearch() {
  const input = document.getElementById('stockSearch');
  input.addEventListener('input', () => {
    const q = input.value.trim().toLowerCase();
    domCache.forEach((refs) => {
      const match = !q || refs.card.dataset.search.includes(q);
      refs.card.style.display = match ? '' : 'none';
    });
    watchlistDomCache.forEach((refs) => {
      const match = !q || refs.row.dataset.search.includes(q);
      refs.row.style.display = match ? '' : 'none';
    });
  });
}

/* =====================================================================
   13. SIDEBAR TOGGLE (mobile)
   ===================================================================== */
function setupNavToggle() {
  const btn = document.getElementById('navToggle');
  const sidebar = document.getElementById('sidebar');
  btn.addEventListener('click', () => sidebar.classList.toggle('is-collapsed'));
}

/* =====================================================================
   14. FORMATTERS
   ===================================================================== */
function formatINR(value) {
  return '₹' + value.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
function formatSignedINR(value) {
  const sign = value >= 0 ? '+' : '−';
  return `${sign}₹${Math.abs(value).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}
function formatChange(pct) {
  const sign = pct >= 0 ? '+' : '';
  return `${sign}${pct.toFixed(2)}%`;
}
function formatVolume(v) {
  if (v >= 1e7) return (v / 1e7).toFixed(2) + 'Cr';
  if (v >= 1e5) return (v / 1e5).toFixed(2) + 'L';
  if (v >= 1e3) return (v / 1e3).toFixed(1) + 'K';
  return String(v);
}
function setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

/* =====================================================================
   15. INIT
   ===================================================================== */
function init() {
  seedStocks();
  buildStockGrid();
  buildWatchlist();
  updateTickerTape();
  updateTopMovers();
  updateIndices();
  updateFeaturedChart();
  setupSearch();
  setupNavToggle();
  tickClock();
  setInterval(tickClock, 1000);

  // Animate the headline portfolio value once, from 0 → real value.
  let invested = 0, currentValue = 0;
  portfolio.forEach((h, symbol) => {
    const s = stocks.get(symbol);
    invested += h.avgPrice * h.qty;
    currentValue += s.price * h.qty;
  });
  animateCountUp('statPortfolioValue', currentValue, formatINR);
  setText('statPortfolioInvested', `Invested ${formatINR(invested)}`);

  // Spin up the mock feed.
  const symbols = STOCK_UNIVERSE.map((s) => s[0]);
  const socket = new MockWebSocket(symbols);
  socket.onMessage((msg) => msg.symbols.forEach(applyTick));
  socket.connect();

  // Kick off the throttled rAF render loop (decoupled from the feed).
  requestAnimationFrame(rafLoop);
}

document.addEventListener('DOMContentLoaded', init);