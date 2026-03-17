/**
 * static_app.js — 김프 리캡 대시보드 (GitHub Pages 등 정적 호스팅용)
 * 
 * 파이썬 서버 없이 브라우저 단에서 직접 JSON 파일들을 읽어들여
 * 일별, 기간별, 주말별 통계를 모두 스스로 계산합니다.
 */

const DATA_BASE = 'data';

// ─── State ───────────────────────────────────────────────────
let currentTab = 'archive';
let currentPeriod = '3d';
let currentCombo = 'bithumb-bybit';
let availableDates = [];
let cachedDataFiles = {}; // { 'YYYY-MM-DD': dataObj }

// ─── Init ────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  initTabs();
  initExchangeSelector();
  initPeriodSelector();
  loadDates();
});

function getCombo() {
  const dom = document.getElementById('domestic-exchange').value;
  const fgn = document.getElementById('foreign-exchange').value;
  return `${dom}-${fgn}`;
}

// ─── Tabs ────────────────────────────────────────────────────
function initTabs() {
  document.querySelectorAll('.tab').forEach(btn => {
    btn.addEventListener('click', () => {
      const tabId = btn.dataset.tab;
      currentTab = tabId;
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      btn.classList.add('active');
      document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
      document.getElementById(`panel-${tabId}`).classList.add('active');

      if (tabId === 'stats') loadStats(currentPeriod);
      if (tabId === 'weekend') loadWeekendStats();
    });
  });
}

// ─── Exchange Selector ───────────────────────────────────────
function initExchangeSelector() {
  const domesticEl = document.getElementById('domestic-exchange');
  const foreignEl = document.getElementById('foreign-exchange');
  const timeDisplay = document.getElementById('collection-time-display');

  function onChange() {
    currentCombo = getCombo();
    const time = domesticEl.value === 'bithumb' ? '00:00 ~ 00:15' : '09:00 ~ 09:15';
    timeDisplay.innerHTML = `수집 시간: <strong>${time} KST</strong>`;
    reloadCurrentTab();
  }

  domesticEl.addEventListener('change', onChange);
  foreignEl.addEventListener('change', onChange);
  onChange();
}

function reloadCurrentTab() {
  if (currentTab === 'archive') {
    const sel = document.getElementById('archive-date');
    if (sel.value) loadArchiveData(sel.value);
  } else if (currentTab === 'stats') {
    loadStats(currentPeriod);
  } else if (currentTab === 'weekend') {
    loadWeekendStats();
  }
}

// ─── Period Selector ─────────────────────────────────────────
function initPeriodSelector() {
  document.querySelectorAll('.period-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      currentPeriod = btn.dataset.period;
      document.querySelectorAll('.period-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      loadStats(currentPeriod);
    });
  });
}

// ─── Data Fetching Helper ────────────────────────────────────
async function fetchIndex() {
  try {
    const res = await fetch(`${DATA_BASE}/index.json`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err) {
    console.error(`Failed to load index.json:`, err);
    return [];
  }
}

async function fetchDateData(dateStr) {
  if (cachedDataFiles[dateStr]) return cachedDataFiles[dateStr];
  try {
    const res = await fetch(`${DATA_BASE}/${dateStr}.json`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    cachedDataFiles[dateStr] = data;
    return data;
  } catch (err) {
    console.warn(`Data not found for ${dateStr}`);
    return null;
  }
}

function getComboDataFromRaw(rawData, combo) {
  if (!rawData) return null;
  const c = rawData.combinations || {};
  if (c[combo]) {
    return {
      date: rawData.date,
      isWeekend: rawData.isWeekend,
      domesticExchange: combo.split('-')[0],
      foreignExchange: combo.split('-')[1],
      totalSnapshots: c[combo].totalSnapshots,
      tetherPremium: c[combo].tetherPremium,
      coins: c[combo].coins
    };
  }
  // 구버전 호환
  if (rawData.coins) {
    return rawData;
  }
  return null;
}

// ─── Load Dates ──────────────────────────────────────────────
async function loadDates() {
  availableDates = await fetchIndex();

  const select = document.getElementById('archive-date');
  select.innerHTML = '<option value="">날짜를 선택하세요</option>';

  const dayNames = ['일', '월', '화', '수', '목', '금', '토'];
  for (const d of availableDates) {
    const dt = new Date(d + 'T00:00:00');
    const dayName = dayNames[dt.getDay()];
    const isWeekend = dt.getDay() === 0 || dt.getDay() === 6;
    const opt = document.createElement('option');
    opt.value = d;
    opt.textContent = `${d} (${dayName})${isWeekend ? ' 🔵' : ''}`;
    select.appendChild(opt);
  }

  select.addEventListener('change', () => {
    if (select.value) loadArchiveData(select.value);
  });

  if (availableDates.length > 0) {
    select.value = availableDates[0];
    loadArchiveData(availableDates[0]);
  }
}

// ─── Archive Tab ─────────────────────────────────────────────
async function loadArchiveData(dateStr) {
  const container = document.getElementById('archive-content');
  container.innerHTML = '<div class="loading-state"><div class="spinner"></div><p>로딩 중...</p></div>';

  const rawData = await fetchDateData(dateStr);
  const data = getComboDataFromRaw(rawData, currentCombo);

  if (!data) {
    container.innerHTML = `<div class="empty-state"><span class="empty-icon">🚫</span><p>${dateStr} 데이터가 없습니다 (${currentCombo}).</p></div>`;
    return;
  }

  const coins = data.coins || [];
  const tetherAvg = data.tetherPremium?.avg ?? '—';
  const tetherMax = data.tetherPremium?.max ?? '—';

  let html = `
    <div class="info-cards">
      <div class="info-card">
        <div class="card-label">날짜</div>
        <div class="card-value date-val">${data.date} ${data.isWeekend ? '(주말)' : ''}</div>
      </div>
      <div class="info-card">
        <div class="card-label">거래소</div>
        <div class="card-value date-val">${comboLabel(currentCombo)}</div>
      </div>
      <div class="info-card">
        <div class="card-label">테더 김프 (평균/최대)</div>
        <div class="card-value premium">${tetherAvg}% / ${tetherMax}%</div>
      </div>
      <div class="info-card">
        <div class="card-label">필터 통과 코인</div>
        <div class="card-value count">${coins.length}개</div>
      </div>
      <div class="info-card">
        <div class="card-label">스냅샷 횟수</div>
        <div class="card-value count">${data.totalSnapshots ?? '—'}회</div>
      </div>
    </div>`;

  if (coins.length === 0) {
    html += '<div class="empty-state"><span class="empty-icon">✨</span><p>해당 기준을 만족하는 코인이 없었습니다.</p></div>';
  } else {
    html += `
    <div class="data-table-wrapper">
      <table class="data-table" id="archive-table">
        <thead>
          <tr>
            <th>코인</th>
            <th>평균 김프</th>
            <th>최대 김프</th>
            <th>입금</th>
            <th>출금</th>
            <th>네트워크</th>
            <th>스냅샷</th>
          </tr>
        </thead>
        <tbody>`;

    for (let i = 0; i < coins.length; i++) {
      const coin = coins[i];
      const pcls = getPremiumClass(coin.avgPremium);
      const networks = (coin.networks || []).map(n => `<span class="network-badge">${n}</span>`).join('') || '—';
      const hasSnaps = coin.snapshots && coin.snapshots.length > 0;

      html += `
          <tr>
            <td class="cell-symbol">${coin.symbol}</td>
            <td class="cell-premium ${pcls}">${coin.avgPremium.toFixed(2)}%</td>
            <td class="cell-premium ${pcls}">${coin.maxPremium.toFixed(2)}%</td>
            <td class="cell-status">${coin.depositEnabled ? '✅' : '❌'}</td>
            <td class="cell-status">${coin.withdrawalEnabled ? '✅' : '❌'}</td>
            <td class="cell-networks">${networks}</td>
            <td>${hasSnaps
          ? `<button class="stats-detail-toggle" onclick="toggleArchiveSnap(${i})">${coin.snapshots.length}개 보기 ▼</button>`
          : `${coin.appearances ?? '—'}회`}
            </td>
          </tr>`;

      if (hasSnaps) {
        html += `
          <tr class="daily-detail-row" id="archive-snap-${i}">
            <td colspan="7" class="daily-detail-cell">
              ${renderSnapshotTable(coin.snapshots)}
            </td>
          </tr>`;
      }
    }

    html += '</tbody></table></div>';
  }

  container.innerHTML = html;
}

function renderSnapshotTable(snapshots) {
  let html = `<table class="daily-detail-table">
    <thead><tr><th>시간</th><th>김프 (%)</th><th>국내가 (KRW)</th><th>해외가 (USDT)</th></tr></thead><tbody>`;
  for (const s of snapshots) {
    const pcls = getPremiumClass(s.premium);
    html += `<tr>
      <td>${s.time || '—'}</td>
      <td class="cell-premium ${pcls}">${s.premium.toFixed(2)}%</td>
      <td>${s.krwPrice ? s.krwPrice.toLocaleString() : '—'}</td>
      <td>${s.usdtPrice ? s.usdtPrice.toFixed(4) : '—'}</td>
    </tr>`;
  }
  html += '</tbody></table>';
  return html;
}

function toggleArchiveSnap(i) {
  const row = document.getElementById(`archive-snap-${i}`);
  if (row) {
    row.classList.toggle('expanded');
    const btn = row.previousElementSibling.querySelector('.stats-detail-toggle');
    if (btn) btn.textContent = row.classList.contains('expanded') ? '접기 ▲' : `보기 ▼`;
  }
}

// ─── Stats Engine (Client-Side) ──────────────────────────────
async function computeStatsClientSide(periodDays) {
  const cutoffTime = new Date().getTime() - (periodDays * 24 * 60 * 60 * 1000);
  const cutoffDate = new Date(cutoffTime).toISOString().split('T')[0];
  
  const targetDates = availableDates.filter(d => d >= cutoffDate);
  if (targetDates.length === 0) return { error: true };

  const coinStats = {};
  let daysCount = 0;

  for (const dateStr of targetDates) {
    const raw = await fetchDateData(dateStr);
    const data = getComboDataFromRaw(raw, currentCombo);
    if (!data) continue;
    
    daysCount++;
    const coins = data.coins || [];
    for (const coin of coins) {
      const sym = coin.symbol;
      if (!coinStats[sym]) {
        coinStats[sym] = {
          symbol: sym, frequency: 0, dailyData: [], allAvg: [], allMax: []
        };
      }
      coinStats[sym].frequency++;
      coinStats[sym].dailyData.push({
        date: dateStr,
        avgPremium: coin.avgPremium,
        maxPremium: coin.maxPremium,
        depositEnabled: coin.depositEnabled || false,
        withdrawalEnabled: coin.withdrawalEnabled || false,
      });
      coinStats[sym].allAvg.push(coin.avgPremium);
      coinStats[sym].allMax.push(coin.maxPremium);
    }
  }

  const resultCoins = Object.values(coinStats).map(s => {
    return {
      symbol: s.symbol,
      frequency: s.frequency,
      overallAvgPremium: s.allAvg.reduce((a,b)=>a+b, 0) / s.allAvg.length,
      overallMaxPremium: Math.max(...s.allMax),
      dailyData: s.dailyData
    };
  });
  resultCoins.sort((a,b) => b.frequency - a.frequency);

  return {
    totalDays: daysCount,
    dateRange: { from: targetDates[targetDates.length-1], to: targetDates[0] },
    coinStats: resultCoins
  };
}

// ─── Stats Tab ───────────────────────────────────────────────
async function loadStats(period) {
  const container = document.getElementById('stats-content');
  container.innerHTML = '<div class="loading-state"><div class="spinner"></div><p>통계 계산 중...</p></div>';

  const daysMap = { '3d': 3, '7d': 7, '14d': 14, '30d': 30 };
  const days = daysMap[period] || 7;

  const data = await computeStatsClientSide(days);
  if (data.error || data.totalDays === 0) {
    container.innerHTML = '<div class="empty-state"><span class="empty-icon">📭</span><p>데이터가 없습니다.</p></div>';
    return;
  }

  const coins = data.coinStats || [];
  const maxFreq = coins.length > 0 ? Math.max(...coins.map(c => c.frequency)) : 1;

  let html = `
    <div class="info-cards">
      <div class="info-card"><div class="card-label">조회 기간</div><div class="card-value date-val">${data.dateRange?.from || '—'} ~ ${data.dateRange?.to || '—'}</div></div>
      <div class="info-card"><div class="card-label">거래소</div><div class="card-value date-val">${comboLabel(currentCombo)}</div></div>
      <div class="info-card"><div class="card-label">데이터 일수</div><div class="card-value count">${data.totalDays}일</div></div>
      <div class="info-card"><div class="card-label">등장 코인 수</div><div class="card-value count">${coins.length}개</div></div>
    </div>`;

  if (coins.length === 0) {
    html += '<div class="empty-state"><span class="empty-icon">📊</span><p>기록된 코인이 없습니다.</p></div>';
  } else {
    html += `<div class="data-table-wrapper"><table class="data-table"><thead><tr>
      <th>코인</th><th>빈도</th><th>평균 김프</th><th>최대 김프</th><th>상세</th>
    </tr></thead><tbody>`;

    for (let i = 0; i < coins.length; i++) {
      const c = coins[i];
      const pcls = getPremiumClass(c.overallAvgPremium);
      const barW = Math.max(4, (c.frequency / maxFreq) * 120);
      html += `
        <tr>
          <td class="cell-symbol">${c.symbol}</td>
          <td class="cell-frequency"><div class="freq-bar"><div class="freq-bar-fill" style="width:${barW}px"></div><span>${c.frequency}일/${data.totalDays}일</span></div></td>
          <td class="cell-premium ${pcls}">${c.overallAvgPremium.toFixed(2)}%</td>
          <td class="cell-premium ${pcls}">${c.overallMaxPremium.toFixed(2)}%</td>
          <td><button class="stats-detail-toggle" onclick="toggleDetail('stats',${i})">▼</button></td>
        </tr>
        <tr class="daily-detail-row" id="stats-detail-${i}"><td colspan="5" class="daily-detail-cell">${renderDailyDetail(c.dailyData)}</td></tr>`;
    }
    html += '</tbody></table></div>';
  }
  container.innerHTML = html;
}

// ─── Weekend Stats Engine ────────────────────────────────────
async function computeWeekendStatsClientSide(weeks) {
  const cutoffTime = new Date().getTime() - (weeks * 7 * 24 * 60 * 60 * 1000);
  const cutoffDate = new Date(cutoffTime).toISOString().split('T')[0];
  
  const weekendDates = availableDates.filter(d => {
    if (d < cutoffDate) return false;
    const dt = new Date(d + 'T00:00:00');
    return dt.getDay() === 0 || dt.getDay() === 6;
  });

  if (weekendDates.length === 0) return { error: true };

  const coinStats = {};
  let totalDays = 0;

  for (const dateStr of weekendDates) {
    const raw = await fetchDateData(dateStr);
    const data = getComboDataFromRaw(raw, currentCombo);
    if (!data) continue;
    
    totalDays++;
    const coins = data.coins || [];
    for (const coin of coins) {
      const sym = coin.symbol;
      if (!coinStats[sym]) {
        coinStats[sym] = {
          symbol: sym, frequency: 0, dailyData: [], allAvg: [], allMax: []
        };
      }
      coinStats[sym].frequency++;
      coinStats[sym].dailyData.push({
        date: dateStr,
        avgPremium: coin.avgPremium,
        maxPremium: coin.maxPremium,
        depositEnabled: coin.depositEnabled || false,
        withdrawalEnabled: coin.withdrawalEnabled || false,
      });
      coinStats[sym].allAvg.push(coin.avgPremium);
      coinStats[sym].allMax.push(coin.maxPremium);
    }
  }

  const resultCoins = Object.values(coinStats).map(s => {
    return {
      symbol: s.symbol,
      frequency: s.frequency,
      overallAvgPremium: s.allAvg.reduce((a,b)=>a+b, 0) / s.allAvg.length,
      overallMaxPremium: Math.max(...s.allMax),
      dailyData: s.dailyData
    };
  });
  resultCoins.sort((a,b) => b.frequency - a.frequency);

  return {
    totalWeekendDays: totalDays,
    weeks: weeks,
    dateRange: { from: weekendDates[weekendDates.length-1], to: weekendDates[0] },
    coinStats: resultCoins
  };
}

// ─── Weekend Stats Tab ───────────────────────────────────────
async function loadWeekendStats() {
  const container = document.getElementById('weekend-content');
  container.innerHTML = '<div class="loading-state"><div class="spinner"></div><p>주말 통계 계산 중...</p></div>';

  const data = await computeWeekendStatsClientSide(4); // 4주 고정
  if (data.error || data.totalWeekendDays === 0) {
    container.innerHTML = '<div class="empty-state"><span class="empty-icon">🗓️</span><p>주말 데이터가 없습니다.</p></div>';
    return;
  }

  const coins = data.coinStats || [];
  const maxFreq = coins.length > 0 ? Math.max(...coins.map(c => c.frequency)) : 1;

  let html = `
    <div class="info-cards">
      <div class="info-card"><div class="card-label">조회 범위</div><div class="card-value date-val">최근 ${data.weeks}주 주말</div></div>
      <div class="info-card"><div class="card-label">거래소</div><div class="card-value date-val">${comboLabel(currentCombo)}</div></div>
      <div class="info-card"><div class="card-label">주말 일수</div><div class="card-value count">${data.totalWeekendDays}일</div></div>
      <div class="info-card"><div class="card-label">등장 코인 수</div><div class="card-value count">${coins.length}개</div></div>
    </div>`;

  if (coins.length === 0) {
    html += '<div class="empty-state"><span class="empty-icon">🌙</span><p>주말 데이터가 없습니다.</p></div>';
  } else {
    html += `<div class="data-table-wrapper"><table class="data-table"><thead><tr>
      <th>코인</th><th>빈도</th><th>평균 김프</th><th>최대 김프</th><th>상세</th>
    </tr></thead><tbody>`;

    for (let i = 0; i < coins.length; i++) {
      const c = coins[i];
      const pcls = getPremiumClass(c.overallAvgPremium);
      const barW = Math.max(4, (c.frequency / maxFreq) * 120);
      html += `
        <tr>
          <td class="cell-symbol">${c.symbol}</td>
          <td class="cell-frequency"><div class="freq-bar"><div class="freq-bar-fill" style="width:${barW}px"></div><span>${c.frequency}일/${data.totalWeekendDays}일</span></div></td>
          <td class="cell-premium ${pcls}">${c.overallAvgPremium.toFixed(2)}%</td>
          <td class="cell-premium ${pcls}">${c.overallMaxPremium.toFixed(2)}%</td>
          <td><button class="stats-detail-toggle" onclick="toggleDetail('wknd',${i})">▼</button></td>
        </tr>
        <tr class="daily-detail-row" id="wknd-detail-${i}"><td colspan="5" class="daily-detail-cell">${renderDailyDetail(c.dailyData)}</td></tr>`;
    }
    html += '</tbody></table></div>';
  }
  container.innerHTML = html;
}

// ─── Shared Renderers ────────────────────────────────────────
function renderDailyDetail(dailyData) {
  if (!dailyData || !dailyData.length) return '<p style="color:var(--text-muted)">상세 없음</p>';
  let html = '<table class="daily-detail-table"><thead><tr><th>날짜</th><th>평균 김프</th><th>최대 김프</th><th>입금</th><th>출금</th></tr></thead><tbody>';
  for (const d of dailyData) {
    html += `<tr>
      <td>${d.date}</td>
      <td class="cell-premium ${getPremiumClass(d.avgPremium)}">${d.avgPremium.toFixed(2)}%</td>
      <td class="cell-premium ${getPremiumClass(d.maxPremium)}">${d.maxPremium.toFixed(2)}%</td>
      <td class="cell-status">${d.depositEnabled ? '✅' : '❌'}</td>
      <td class="cell-status">${d.withdrawalEnabled ? '✅' : '❌'}</td>
    </tr>`;
  }
  return html + '</tbody></table>';
}

function toggleDetail(prefix, i) {
  const row = document.getElementById(`${prefix}-detail-${i}`);
  if (row) {
    row.classList.toggle('expanded');
    const btn = row.previousElementSibling.querySelector('.stats-detail-toggle');
    if (btn) btn.textContent = row.classList.contains('expanded') ? '▲' : '▼';
  }
}

// ─── Helpers ─────────────────────────────────────────────────
function getPremiumClass(p) {
  if (p >= 15) return 'high';
  if (p >= 8) return 'medium';
  return 'low';
}

function comboLabel(combo) {
  const labels = {
    'bithumb-bybit': '빗썸 vs 바이비트',
    'bithumb-binance': '빗썸 vs 바이낸스',
    'upbit-bybit': '업비트 vs 바이비트',
    'upbit-binance': '업비트 vs 바이낸스',
  };
  return labels[combo] || combo;
}
