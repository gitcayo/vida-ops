/**
 * ─────────────────────────────────────────────────────────────
 *  VIDA OPS — Google Drive / Sheets Integration  (v5)
 *
 *  HOW TO USE
 *  1. Add to your HTML <head>:
 *       <script src="https://apis.google.com/js/api.js"></script>
 *       <script src="https://accounts.google.com/gsi/client"></script>
 *  2. Add after your main app script tag:
 *       <script src="google_drive_integration.js"></script>
 *  3. In the app: Settings → Google Drive
 *       - Paste your OAuth Client ID → click Save ID
 *       - Click Connect to Google
 *       - Done.
 *
 *  CLIENT ID SOURCE
 *  Read from localStorage key 'vida_drive_client_id'
 *  Set by saveDriveClientId() in the Settings UI.
 *  Never hardcoded here.
 *
 *  TRIGGERS
 *  - Manual:    "Save to Drive" button on any delivered order
 *  - Automatic: After saveActuals() completes (wrapped below)
 *  - Manual:    "Save all delivered orders now" in Settings
 *  - Manual:    "Rebuild master sheet" in Settings
 *
 *  WHAT GETS SAVED
 *  Per order      → Drive/Shipment Orders/<season>/<orderId> — P&L
 *                   Tabs: Order summary | Items & sizes | Channel breakdown |
 *                         Actuals vs estimates | P&L | Customer sales
 *  Per collection → Drive/Shipment Orders/<season>/<name> — Collection Summary
 *                   Tabs: Collection summary | Orders | Items & margin |
 *                         Revenue by channel | P&L
 *  Per customer   → Drive/Shipment Orders/Customers/<company> — Purchase History
 *                   Tabs: Customer profile | Purchase history |
 *                         Products purchased | P&L summary
 *  Master sheet   → Drive/Shipment Orders/Master — All Orders
 *                   Tabs: All orders | All items | All customers
 * ─────────────────────────────────────────────────────────────
 */

const DRIVE_CONFIG = {
  ROOT_FOLDER_NAME: 'Shipment Orders',
  SCOPES: [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/userinfo.email',
  ].join(' '),
};

let _driveConnected = false;
let _accessToken    = null;
let _tokenClient    = null;
let _rootFolderId   = null;


// ─────────────────────────────────────────────────────────────
//  PUBLIC API
// ─────────────────────────────────────────────────────────────

async function driveConnect() {
  const clientId = localStorage.getItem('vida_drive_client_id');
  if (!clientId) {
    alert('Paste and save your Client ID first (Step 1).');
    return;
  }
  await _loadGoogleLibraries();
  return new Promise((resolve, reject) => {
    _tokenClient = google.accounts.oauth2.initTokenClient({
      client_id: clientId,
      scope: DRIVE_CONFIG.SCOPES,
      callback: async (response) => {
        if (response.error) {
          _setDriveStatus('error', 'Auth error: ' + response.error);
          reject(response.error);
          return;
        }
        _accessToken    = response.access_token;
        _driveConnected = true;
        _rootFolderId   = null;
        try {
          const profile = await _getGoogleProfile();
          _onConnectSuccess(profile.email || 'Connected');
          resolve(profile);
        } catch (e) {
          _onConnectSuccess('Connected');
          resolve({});
        }
      },
    });
    _tokenClient.requestAccessToken({ prompt: 'consent' });
  });
}

function driveDisconnect() {
  _accessToken    = null;
  _driveConnected = false;
  _rootFolderId   = null;
  localStorage.removeItem('vida_drive_email');
  const accountEl     = document.getElementById('drive-account');
  const connectBtn    = document.getElementById('drive-connect-btn');
  const disconnectBtn = document.getElementById('drive-disconnect-btn');
  if (accountEl)     accountEl.value            = '';
  if (connectBtn)    connectBtn.style.display    = 'inline-block';
  if (disconnectBtn) disconnectBtn.style.display = 'none';
  _setDriveStatus('disconnected', 'Disconnected. Reconnect to re-enable Drive sync.');
}

async function driveSaveOrder(orderId) {
  _requireAuth();
  const order = orders.find(o => o.id === orderId);
  if (!order) throw new Error('Order not found: ' + orderId);
  const calc         = calcOrder(order);
  const seasonFolder = await _getOrCreateSeasonFolder(order);
  const sheetId      = await _getOrCreateSheet(orderId + ' — P&L', seasonFolder);
  await _writeOrderSheet(sheetId, order, calc);
  _log('Saved order: ' + orderId);
  await _withFallback(() => _saveMasterSheet(),                          'master sheet');
  if (order.collectionId) {
    await _withFallback(() => driveSaveCollection(order.collectionId),   'collection');
  }
  const linked = customers.filter(c => c.orderIds.includes(orderId));
  for (const c of linked) {
    await _withFallback(() => driveSaveCustomer(c.id), 'customer: ' + c.company);
  }
  _updateLastSync('Order ' + orderId + ' saved');
  return sheetId;
}

async function driveSaveCollection(collectionId) {
  _requireAuth();
  const coll = collections.find(c => c.id === collectionId);
  if (!coll) return;
  const pl           = getCollPL(coll);
  const seasonFolder = await _getOrCreateFolder(coll.season || 'Unseasoned', await _getRootFolder());
  const sheetId      = await _getOrCreateSheet(coll.name + ' — Collection Summary', seasonFolder);
  await _writeCollectionSheet(sheetId, coll, pl);
  _log('Saved collection: ' + coll.name);
  return sheetId;
}

async function driveSaveCustomer(customerId) {
  _requireAuth();
  const cust = customers.find(c => c.id === customerId);
  if (!cust) return;
  const custFolder = await _getOrCreateFolder('Customers', await _getRootFolder());
  const sheetId    = await _getOrCreateSheet(cust.company + ' — Purchase History', custFolder);
  const stats      = calcCustomer(cust);
  await _writeCustomerSheet(sheetId, cust, stats);
  _log('Saved customer: ' + cust.company);
  return sheetId;
}

async function driveSaveMasterSheet() {
  _requireAuth();
  await _saveMasterSheet();
  _updateLastSync('Master sheet rebuilt');
}

async function driveSaveAllDelivered() {
  _requireAuth();
  const delivered = orders.filter(o => o.status === 'Delivered');
  if (!delivered.length) { alert('No delivered orders to save yet.'); return; }
  _updateLastSync('Saving ' + delivered.length + ' order(s)…');
  for (const o of delivered) {
    await _withFallback(() => driveSaveOrder(o.id), 'order ' + o.id);
  }
  _updateLastSync('All delivered orders saved');
}


// ─────────────────────────────────────────────────────────────
//  SHEET WRITERS
// ─────────────────────────────────────────────────────────────

async function _writeOrderSheet(sheetId, order, calc) {
  const tabs = [
    { title: 'Order summary',        rows: _rows_orderSummary(order, calc)  },
    { title: 'Items & sizes',        rows: _rows_itemSizes(order, calc)     },
    { title: 'Channel breakdown',    rows: _rows_channels(order, calc)      },
    { title: 'Actuals vs estimates', rows: _rows_actuals(order)             },
    { title: 'P&L',                  rows: _rows_orderPL(order, calc)       },
    { title: 'Customer sales',       rows: _rows_customerSales(order, calc) },
  ];
  await _ensureTabs(sheetId, tabs.map(t => t.title));
  for (const tab of tabs) await _writeTab(sheetId, tab.title, tab.rows);
  await _formatHeaderRows(sheetId, tabs.length);
}

async function _writeCollectionSheet(sheetId, coll, pl) {
  const tabs = [
    { title: 'Collection summary',  rows: _rows_collSummary(coll, pl)  },
    { title: 'Orders',              rows: _rows_collOrders(coll, pl)   },
    { title: 'Items & margin',      rows: _rows_collItems(coll, pl)    },
    { title: 'Revenue by channel',  rows: _rows_collChannels(coll, pl) },
    { title: 'P&L',                 rows: _rows_collPL(coll, pl)       },
  ];
  await _ensureTabs(sheetId, tabs.map(t => t.title));
  for (const tab of tabs) await _writeTab(sheetId, tab.title, tab.rows);
  await _formatHeaderRows(sheetId, tabs.length);
}

async function _writeCustomerSheet(sheetId, cust, stats) {
  const tabs = [
    { title: 'Customer profile',   rows: _rows_custProfile(cust, stats)  },
    { title: 'Purchase history',   rows: _rows_custHistory(cust, stats)  },
    { title: 'Products purchased', rows: _rows_custProducts(cust, stats) },
    { title: 'P&L summary',        rows: _rows_custPL(cust, stats)       },
  ];
  await _ensureTabs(sheetId, tabs.map(t => t.title));
  for (const tab of tabs) await _writeTab(sheetId, tab.title, tab.rows);
  await _formatHeaderRows(sheetId, tabs.length);
}

async function _saveMasterSheet() {
  const rootFolder = await _getRootFolder();
  const sheetId    = await _getOrCreateSheet('Master — All Orders', rootFolder);
  const tabs = [
    { title: 'All orders',    rows: _rows_masterOrders()    },
    { title: 'All items',     rows: _rows_masterItems()     },
    { title: 'All customers', rows: _rows_masterCustomers() },
  ];
  await _ensureTabs(sheetId, tabs.map(t => t.title));
  for (const tab of tabs) await _writeTab(sheetId, tab.title, tab.rows);
  await _formatHeaderRows(sheetId, tabs.length);
  _log('Master sheet rebuilt');
}


// ─────────────────────────────────────────────────────────────
//  ROW BUILDERS — Order
// ─────────────────────────────────────────────────────────────

function _rows_orderSummary(order, calc) {
  const sup  = suppliers.find(s => s.id === order.supplier) || {};
  const coll = collections.find(c => c.id === order.collectionId);
  return [
    ['Field', 'Value'],
    ['Order #',         order.id],
    ['Date',            order.date],
    ['Supplier',        sup.name || ''],
    ['Supplier country',sup.country || ''],
    ['Collection',      coll ? coll.name + (coll.season ? ' (' + coll.season + ')' : '') : '—'],
    ['Status',          order.status],
    ['Carrier',         order.carrier  || '—'],
    ['Tracking #',      order.tracking || '—'],
    ['ETA',             order.eta      || '—'],
    ['Actualized',      order.isActualized ? 'Yes' : 'No'],
    [],
    ['COSTS', ''],
    ['Shipping ($)',     _n(calc.ship)],
    ['Clearing ($)',     _n(calc.clear)],
    ['Extra costs ($)',  _n(calc.extraTotal)],
    ['Total landed ($)', _n(calc.tLanded)],
    [],
    ['REVENUE', ''],
    ['Retail revenue ($)',    _n(calc.rRev)],
    ['Wholesale revenue ($)', _n(calc.wRev)],
    ['Custom revenue ($)',    _n(calc.cRev)],
    ['Total revenue ($)',     _n(calc.tRev)],
    ['Gross profit ($)',      _n(calc.tRev - calc.tLanded)],
    ['Gross margin %',        _pct(calc.margin)],
    [],
    ['Total units', calc.units],
  ];
}

function _rows_itemSizes(order, calc) {
  const header = [
    'Item name', 'Product type', 'Group', 'Design(s)', 'Color(s)',
    'Unit price ($)', 'Markup %', 'Landed/unit ($)', 'Retail price ($)', 'Qty adj',
    ...SIZES, 'Total qty', 'Total landed ($)',
  ];
  const rows = [header];
  order.items.forEach(it => {
    const pt  = _findProduct(it.productId);
    const grp = pt ? (productGroups.find(g => g.id === pt.groupId) || {}).name || '' : '';
    rows.push([
      it.name, pt ? pt.name : '', grp,
      _joinNames(it.designs,   designs),
      _joinNames(it.colorways, colors),
      _n(it.unitPrice), it.markup + '%',
      _n(it._landedU || 0), _n(it._retail || 0),
      it.adj || 0,
      ...it.sizes,
      it._effQty || it.sizes.reduce((a, v) => a + (+v || 0), 0),
      _n(it._tLanded || 0),
    ]);
  });
  return rows;
}

function _rows_channels(order, calc) {
  const header = [
    'Item', 'Channel', 'Qty', 'Price/unit ($)', 'Revenue ($)',
    'Discount type', 'Discount detail', 'Custom note',
  ];
  const rows = [header];
  order.items.forEach(it => {
    it.channels.forEach(ch => {
      let discType = '', discDetail = '', note = '';
      if (ch.type === 'wholesale') {
        discType   = ch.wsType === 'pct' ? '% off retail' : 'Fixed price';
        discDetail = ch.wsType === 'pct' ? ch.wsPct + '%' : '$' + _n(ch.wsFixed);
      } else if (ch.type === 'custom') {
        note = ch.customNote || '';
      }
      rows.push([
        it.name,
        ch.type === 'retail' ? 'Retail' : ch.type === 'wholesale' ? 'Wholesale' : 'Custom',
        ch.qty || 0,
        _n(ch.price || 0),
        _n((ch.price || 0) * (ch.qty || 0)),
        discType, discDetail, note,
      ]);
    });
  });
  return rows;
}

function _rows_actuals(order) {
  const rows = [['Cost item', 'Estimated ($)', 'Actual ($)', 'Difference ($)']];
  const diff = (est, act) => act != null ? _n(act - est) : '—';
  rows.push(['Shipping', _n(order.estShip),  order.actShip  != null ? _n(order.actShip)  : '—', diff(order.estShip,  order.actShip)]);
  rows.push(['Clearing', _n(order.estClear), order.actClear != null ? _n(order.actClear) : '—', diff(order.estClear, order.actClear)]);
  const allLabels = [...new Set([
    ...(order.estExtra || []).map(e => e.label),
    ...(order.actExtra || []).map(e => e.label),
  ])];
  allLabels.forEach(lbl => {
    const est = (order.estExtra || []).find(e => e.label === lbl);
    const act = (order.actExtra || []).find(e => e.label === lbl);
    rows.push([lbl, est ? _n(est.amt) : '—', act ? _n(act.amt) : '—', (est && act) ? _n(act.amt - est.amt) : '—']);
  });
  return rows;
}

function _rows_orderPL(order, calc) {
  return [
    ['P&L — ' + order.id, ''], [],
    ['Total landed cost ($)', _n(calc.tLanded)], [],
    ['Retail revenue ($)',    _n(calc.rRev)],
    ['Wholesale revenue ($)', _n(calc.wRev)],
    ['Custom revenue ($)',    _n(calc.cRev)],
    ['Total revenue ($)',     _n(calc.tRev)], [],
    ['Gross profit ($)',      _n(calc.tRev - calc.tLanded)],
    ['Gross margin %',        _pct(calc.margin)], [],
    ['VAT-inclusive retail',  '= retail price × 1.10 per item'],
  ];
}

function _rows_customerSales(order, calc) {
  const header = ['Customer', 'Type', 'Item', 'Channel', 'Detail', 'Qty', 'Price/unit ($)', 'Revenue ($)'];
  const rows   = [header];
  const linked = customers.filter(c => c.orderIds.includes(order.id));
  if (!linked.length) { rows.push(['No customers linked to this order']); return rows; }
  linked.forEach(cust => {
    order.items.forEach(it => {
      it.channels.filter(ch => ch.type !== 'retail').forEach(ch => {
        const detail = ch.type === 'wholesale'
          ? (ch.wsType === 'pct' ? ch.wsPct + '% off retail' : 'Fixed $' + _n(ch.wsFixed))
          : (ch.customNote || '');
        rows.push([
          cust.company, cust.type, it.name,
          ch.type === 'wholesale' ? 'Wholesale' : 'Custom',
          detail, ch.qty || 0,
          _n(ch.price || 0), _n((ch.price || 0) * (ch.qty || 0)),
        ]);
      });
    });
  });
  return rows;
}


// ─────────────────────────────────────────────────────────────
//  ROW BUILDERS — Collection
// ─────────────────────────────────────────────────────────────

function _rows_collSummary(coll, pl) {
  return [
    ['Field', 'Value'],
    ['Collection name', coll.name],
    ['Season',          coll.season || ''],
    ['Description',     coll.desc   || ''],
    [], ['Orders', pl.orders], ['Total units', pl.units], [],
    ['Total landed ($)',     _n(pl.tLanded)],
    ['Retail revenue ($)',   _n(pl.rRev)],
    ['Wholesale revenue ($)',_n(pl.wRev)],
    ['Custom revenue ($)',   _n(pl.cRev)],
    ['Total revenue ($)',    _n(pl.tRev)],
    ['Gross profit ($)',     _n(pl.tRev - pl.tLanded)],
    ['Gross margin %',       _pct(pl.margin)],
    [], ['Units by size', ...SIZES], ['', ...pl.sizes],
  ];
}

function _rows_collOrders(coll, pl) {
  const header = ['Order #', 'Date', 'Supplier', 'Status', 'Actualized', 'Units', 'Landed ($)', 'Retail rev ($)', 'WS rev ($)', 'Custom rev ($)', 'Total rev ($)', 'Margin %'];
  const rows   = [header];
  pl.ords.forEach(o => {
    const c = calcOrder(o);
    rows.push([o.id, o.date, supName(o.supplier), o.status, o.isActualized ? 'Yes' : 'No', c.units, _n(c.tLanded), _n(c.rRev), _n(c.wRev), _n(c.cRev), _n(c.tRev), _pct(c.margin)]);
  });
  return rows;
}

function _rows_collItems(coll, pl) {
  const header = ['Item', 'Order #', 'Supplier', 'Product type', 'Group', 'Design(s)', 'Color(s)', 'Retail qty', 'WS qty', 'Custom qty', 'Retail rev ($)', 'WS rev ($)', 'Custom rev ($)', 'Total rev ($)', 'Margin %'];
  const rows   = [header];
  pl.ords.forEach(o => {
    calcOrder(o);
    o.items.forEach(it => {
      const pt  = _findProduct(it.productId);
      const grp = pt ? (productGroups.find(g => g.id === pt.groupId) || {}).name || '' : '';
      const rC  = it.channels.find(c => c.type === 'retail');
      const wC  = it.channels.find(c => c.type === 'wholesale');
      const cuC = it.channels.find(c => c.type === 'custom');
      const tR  = (rC ? rC.price * (rC.qty || 0) : 0) + (wC ? wC.price * (wC.qty || 0) : 0) + (cuC ? (cuC.price || 0) * (cuC.qty || 0) : 0);
      const m   = tR > 0 ? (tR - (it._tLanded || 0)) / tR * 100 : 0;
      rows.push([it.name, o.id, supName(o.supplier), pt ? pt.name : '', grp, _joinNames(it.designs, designs), _joinNames(it.colorways, colors), rC ? rC.qty : 0, wC ? wC.qty : 0, cuC ? cuC.qty : 0, _n(rC ? rC.price * (rC.qty || 0) : 0), _n(wC ? wC.price * (wC.qty || 0) : 0), _n(cuC ? (cuC.price || 0) * (cuC.qty || 0) : 0), _n(tR), _pct(m)]);
    });
  });
  return rows;
}

function _rows_collChannels(coll, pl) {
  return [
    ['Channel', 'Revenue ($)', '% of total'],
    ['Retail',    _n(pl.rRev), _pct(pl.tRev > 0 ? pl.rRev / pl.tRev * 100 : 0)],
    ['Wholesale', _n(pl.wRev), _pct(pl.tRev > 0 ? pl.wRev / pl.tRev * 100 : 0)],
    ['Custom',    _n(pl.cRev), _pct(pl.tRev > 0 ? pl.cRev / pl.tRev * 100 : 0)],
    ['Total',     _n(pl.tRev), '100%'],
  ];
}

function _rows_collPL(coll, pl) {
  return [
    ['P&L — ' + coll.name, ''], [],
    ['Total landed ($)',     _n(pl.tLanded)],
    ['Retail revenue ($)',   _n(pl.rRev)],
    ['Wholesale revenue ($)',_n(pl.wRev)],
    ['Custom revenue ($)',   _n(pl.cRev)],
    ['Total revenue ($)',    _n(pl.tRev)],
    ['Gross profit ($)',     _n(pl.tRev - pl.tLanded)],
    ['Gross margin %',       _pct(pl.margin)],
  ];
}


// ─────────────────────────────────────────────────────────────
//  ROW BUILDERS — Customer
// ─────────────────────────────────────────────────────────────

function _rows_custProfile(cust, stats) {
  const tier = stats.tRev > 10000 ? 'Gold' : stats.tRev > 3000 ? 'Silver' : 'New';
  return [
    ['Field', 'Value'],
    ['Company',           cust.company],
    ['Contact',           cust.contact  || ''],
    ['Email',             cust.email    || ''],
    ['Phone',             cust.phone    || ''],
    ['Type',              cust.type],
    ['Country',           cust.country  || ''],
    ['Address',           cust.address  || ''],
    ['Payment terms',     cust.terms    || ''],
    ['Default discount',  cust.discount ? cust.discount + '%' : '0%'],
    ['Notes',             cust.notes    || ''],
    [], ['SUMMARY', ''],
    ['Lifetime value ($)',   _n(stats.tRev)],
    ['Total orders',         stats.orderCount],
    ['Avg order value ($)',  _n(stats.avgOrder)],
    ['Products bought',      stats.productCount],
    ['Total units',          stats.units],
    ['Wholesale rev ($)',    _n(stats.wsRev)],
    ['Custom rev ($)',       _n(stats.custRev)],
    ['Gross margin %',       _pct(stats.margin)],
    ['Tier',                 tier],
  ];
}

function _rows_custHistory(cust, stats) {
  const header = ['Order #', 'Date', 'Collection', 'Status', 'WS revenue ($)', 'Custom revenue ($)', 'Total ($)', 'Margin %'];
  const rows   = [header];
  stats.ords.forEach(o => {
    const c   = calcOrder(o);
    const wR  = c.wRev, cuR = c.cRev, tot = wR + cuR;
    const lnd = tot > 0 ? (wR + cuR) / c.tRev * c.tLanded : 0;
    const m   = tot > 0 ? (tot - lnd) / tot * 100 : 0;
    rows.push([o.id, o.date, collName(o.collectionId) || '—', o.status, _n(wR), _n(cuR), _n(tot), _pct(m)]);
  });
  return rows;
}

function _rows_custProducts(cust, stats) {
  const header = ['Item', 'Product type', 'Design(s)', 'Color(s)', 'Order #', 'Channel', 'Detail', 'Qty', 'Price/unit ($)', 'Revenue ($)'];
  const rows   = [header];
  stats.ords.forEach(o => {
    calcOrder(o);
    o.items.forEach(it => {
      const pt = _findProduct(it.productId);
      it.channels.filter(ch => ch.type !== 'retail').forEach(ch => {
        const detail = ch.type === 'wholesale'
          ? (ch.wsType === 'pct' ? ch.wsPct + '% off retail' : 'Fixed $' + _n(ch.wsFixed))
          : (ch.customNote || '');
        rows.push([it.name, pt ? pt.name : '', _joinNames(it.designs, designs), _joinNames(it.colorways, colors), o.id, ch.type === 'wholesale' ? 'Wholesale' : 'Custom', detail, ch.qty || 0, _n(ch.price || 0), _n((ch.price || 0) * (ch.qty || 0))]);
      });
    });
  });
  return rows;
}

function _rows_custPL(cust, stats) {
  return [
    ['P&L — ' + cust.company, ''], [],
    ['Attributed landed cost ($)', _n(stats.tLanded)],
    ['Wholesale revenue ($)',      _n(stats.wsRev)],
    ['Custom orders revenue ($)',  _n(stats.custRev)],
    ['Total revenue ($)',          _n(stats.tRev)],
    ['Gross profit ($)',           _n(stats.tRev - stats.tLanded)],
    ['Gross margin %',             _pct(stats.margin)],
  ];
}


// ─────────────────────────────────────────────────────────────
//  ROW BUILDERS — Master
// ─────────────────────────────────────────────────────────────

function _rows_masterOrders() {
  const header = ['Order #', 'Date', 'Supplier', 'Country', 'Collection', 'Season', 'Status', 'Actualized', 'Units', 'Landed ($)', 'Retail rev ($)', 'WS rev ($)', 'Custom rev ($)', 'Total rev ($)', 'Margin %'];
  const rows   = [header];
  orders.forEach(o => {
    const c    = calcOrder(o);
    const coll = collections.find(x => x.id === o.collectionId);
    const sup  = suppliers.find(x => x.id === o.supplier) || {};
    rows.push([o.id, o.date, sup.name || '', sup.country || '', coll ? coll.name : '—', coll ? coll.season || '' : '', o.status, o.isActualized ? 'Yes' : 'No', c.units, _n(c.tLanded), _n(c.rRev), _n(c.wRev), _n(c.cRev), _n(c.tRev), _pct(c.margin)]);
  });
  return rows;
}

function _rows_masterItems() {
  const header = ['Order #', 'Item name', 'Product type', 'Group', 'Design(s)', 'Color(s)', 'Unit price ($)', 'Markup %', 'Landed/unit ($)', 'Retail price ($)', 'Total qty', 'Total rev ($)', 'Margin %'];
  const rows   = [header];
  orders.forEach(o => {
    calcOrder(o);
    o.items.forEach(it => {
      const pt  = _findProduct(it.productId);
      const grp = pt ? (productGroups.find(g => g.id === pt.groupId) || {}).name || '' : '';
      const tR  = it.channels.reduce((a, ch) => a + (ch.price || 0) * (ch.qty || 0), 0);
      const m   = tR > 0 ? (tR - (it._tLanded || 0)) / tR * 100 : 0;
      rows.push([o.id, it.name, pt ? pt.name : '', grp, _joinNames(it.designs, designs), _joinNames(it.colorways, colors), _n(it.unitPrice), it.markup + '%', _n(it._landedU || 0), _n(it._retail || 0), it._effQty || 0, _n(tR), _pct(m)]);
    });
  });
  return rows;
}

function _rows_masterCustomers() {
  const header = ['Company', 'Contact', 'Email', 'Phone', 'Type', 'Country', 'Terms', 'Discount', 'Orders', 'Lifetime value ($)', 'Avg order ($)', 'WS rev ($)', 'Custom rev ($)', 'Margin %', 'Tier'];
  const rows   = [header];
  customers.forEach(cust => {
    const s    = calcCustomer(cust);
    const tier = s.tRev > 10000 ? 'Gold' : s.tRev > 3000 ? 'Silver' : 'New';
    rows.push([cust.company, cust.contact || '', cust.email || '', cust.phone || '', cust.type, cust.country || '', cust.terms || '', cust.discount ? cust.discount + '%' : '0%', s.orderCount, _n(s.tRev), _n(s.avgOrder), _n(s.wsRev), _n(s.custRev), _pct(s.margin), tier]);
  });
  return rows;
}


// ─────────────────────────────────────────────────────────────
//  SHEETS API HELPERS
// ─────────────────────────────────────────────────────────────

async function _ensureTabs(sheetId, tabTitles) {
  const meta     = await _sheetsGet('spreadsheets/' + sheetId + '?fields=sheets.properties');
  const existing = meta.sheets.map(s => s.properties.title);
  const toAdd    = tabTitles.filter(t => !existing.includes(t));
  if (!toAdd.length) return;
  await _sheetsBatch(sheetId, toAdd.map(title => ({ addSheet: { properties: { title } } })));
}

async function _writeTab(sheetId, tabTitle, rows) {
  await _sheetsRequest('PUT',
    'spreadsheets/' + sheetId + '/values/' + encodeURIComponent(tabTitle + '!A1'),
    { valueInputOption: 'RAW' },
    { values: rows }
  );
}

async function _formatHeaderRows(sheetId, sheetCount) {
  const requests = [];
  for (let i = 0; i < sheetCount; i++) {
    requests.push(
      { repeatCell: { range: { sheetId: i, startRowIndex: 0, endRowIndex: 1 }, cell: { userEnteredFormat: { textFormat: { bold: true }, backgroundColor: { red: 0.88, green: 0.94, blue: 0.82 } } }, fields: 'userEnteredFormat(textFormat,backgroundColor)' } },
      { updateSheetProperties: { properties: { sheetId: i, gridProperties: { frozenRowCount: 1 } }, fields: 'gridProperties.frozenRowCount' } }
    );
  }
  if (requests.length) await _sheetsBatch(sheetId, requests);
}


// ─────────────────────────────────────────────────────────────
//  DRIVE FOLDER HELPERS
// ─────────────────────────────────────────────────────────────

async function _getRootFolder() {
  if (_rootFolderId) return _rootFolderId;
  const folderName = (document.getElementById('drive-root-folder') || {}).value || DRIVE_CONFIG.ROOT_FOLDER_NAME;
  _rootFolderId = await _getOrCreateFolder(folderName, null);
  return _rootFolderId;
}

async function _getOrCreateSeasonFolder(order) {
  const coll   = collections.find(c => c.id === order.collectionId);
  const season = coll ? (coll.season || 'Unseasoned') : 'Unseasoned';
  return _getOrCreateFolder(season, await _getRootFolder());
}

async function _getOrCreateFolder(name, parentId) {
  const q   = `name='${name}' and mimeType='application/vnd.google-apps.folder'${parentId ? ` and '${parentId}' in parents` : ''} and trashed=false`;
  const res = await _driveRequest('GET', 'files', { q, fields: 'files(id,name)' });
  if (res.files && res.files.length) return res.files[0].id;
  const created = await _driveRequest('POST', 'files', {}, { name, mimeType: 'application/vnd.google-apps.folder', ...(parentId ? { parents: [parentId] } : {}) });
  return created.id;
}

async function _getOrCreateSheet(name, parentFolderId) {
  const q   = `name='${name}' and mimeType='application/vnd.google-apps.spreadsheet' and '${parentFolderId}' in parents and trashed=false`;
  const res = await _driveRequest('GET', 'files', { q, fields: 'files(id,name)' });
  if (res.files && res.files.length) return res.files[0].id;
  const created = await _driveRequest('POST', 'files', {}, { name, mimeType: 'application/vnd.google-apps.spreadsheet', parents: [parentFolderId] });
  return created.id;
}


// ─────────────────────────────────────────────────────────────
//  HTTP HELPERS
// ─────────────────────────────────────────────────────────────

async function _driveRequest(method, path, params = {}, body = null) {
  const url = new URL('https://www.googleapis.com/drive/v3/' + path);
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  const res = await fetch(url.toString(), { method, headers: { Authorization: 'Bearer ' + _accessToken, 'Content-Type': 'application/json' }, ...(body ? { body: JSON.stringify(body) } : {}) });
  if (!res.ok) throw new Error('Drive API ' + res.status + ': ' + await res.text());
  return res.json();
}

async function _sheetsRequest(method, path, params = {}, body = null) {
  const url = new URL('https://sheets.googleapis.com/v4/' + path);
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  const res = await fetch(url.toString(), { method, headers: { Authorization: 'Bearer ' + _accessToken, 'Content-Type': 'application/json' }, ...(body ? { body: JSON.stringify(body) } : {}) });
  if (!res.ok) throw new Error('Sheets API ' + res.status + ': ' + await res.text());
  return res.json();
}

async function _sheetsGet(path)                { return _sheetsRequest('GET',  path); }
async function _sheetsBatch(sheetId, requests) { return _sheetsRequest('POST', 'spreadsheets/' + sheetId + ':batchUpdate', {}, { requests }); }
async function _getGoogleProfile() {
  const res = await fetch('https://www.googleapis.com/oauth2/v3/userinfo', { headers: { Authorization: 'Bearer ' + _accessToken } });
  return res.json();
}


// ─────────────────────────────────────────────────────────────
//  GOOGLE LIBRARY LOADER
// ─────────────────────────────────────────────────────────────

async function _loadGoogleLibraries() {
  if (window._vidaGoogleLoaded) return;
  await Promise.all([
    new Promise(r => { if (window.gapi) return r(); const s = document.createElement('script'); s.src = 'https://apis.google.com/js/api.js'; s.onload = r; document.head.appendChild(s); }),
    new Promise(r => { if (window.google && window.google.accounts) return r(); const s = document.createElement('script'); s.src = 'https://accounts.google.com/gsi/client'; s.onload = r; document.head.appendChild(s); }),
  ]);
  window._vidaGoogleLoaded = true;
}


// ─────────────────────────────────────────────────────────────
//  UI HELPERS
// ─────────────────────────────────────────────────────────────

function _onConnectSuccess(email) {
  const accountEl     = document.getElementById('drive-account');
  const connectBtn    = document.getElementById('drive-connect-btn');
  const disconnectBtn = document.getElementById('drive-disconnect-btn');
  if (accountEl)     accountEl.value            = email;
  if (connectBtn)    connectBtn.style.display    = 'none';
  if (disconnectBtn) disconnectBtn.style.display = 'inline-block';
  localStorage.setItem('vida_drive_email', email);
  _setDriveStatus('connected', 'Connected as ' + email);
}

function _setDriveStatus(type, msg) {
  const bar = document.getElementById('drive-status-bar');
  const txt = document.getElementById('drive-status-text');
  if (!bar || !txt) return;
  const s = { disconnected: 'background:#FAEEDA;color:#854F0B;border:.5px solid #FAC775', ready: 'background:#E6F1FB;color:#185FA5;border:.5px solid #B5D4F4', connected: 'background:#EAF3DE;color:#3B6D11;border:.5px solid #C0DD97', error: 'background:#FCEBEB;color:#A32D2D;border:.5px solid #F7C1C1' };
  bar.style.cssText = (s[type] || s.disconnected) + ';display:flex;align-items:center;gap:8px;padding:10px 14px;border-radius:var(--border-radius-md);font-size:13px;margin-bottom:1rem';
  txt.textContent   = msg;
}

function _updateLastSync(msg) {
  const el = document.getElementById('drive-last-sync');
  if (el) el.textContent = msg + ' — ' + new Date().toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

function _requireAuth() {
  if (!_driveConnected || !_accessToken) throw new Error('Not connected to Google Drive. Go to Settings → Google Drive → Connect first.');
}

async function _withFallback(fn, label) {
  try { await fn(); } catch (e) { console.warn('[Drive] ' + label + ' failed:', e.message); }
}

function _log(msg) { console.log('[Drive] ' + msg); }


// ─────────────────────────────────────────────────────────────
//  DATA HELPERS  (read from app globals)
// ─────────────────────────────────────────────────────────────

function _findProduct(id) {
  if (!id) return null;
  return (window.coreProducts || []).find(p => p.id === id) || (window.customProducts || []).find(p => p.id === id) || null;
}

function _joinNames(ids, list) {
  if (!ids || !ids.length) return '';
  return ids.map(id => (list.find(x => x.id === id) || {}).name).filter(Boolean).join(', ');
}

function _n(val)   { return parseFloat((+val || 0).toFixed(2)); }
function _pct(val) { return parseFloat((+val || 0).toFixed(1)) + '%'; }


// ─────────────────────────────────────────────────────────────
//  HOOK — wrap saveActuals for auto-save to Drive
// ─────────────────────────────────────────────────────────────

(function _wrapSaveActuals() {
  const _orig = window.saveActuals;
  if (typeof _orig !== 'function') return;
  window.saveActuals = async function (ordId) {
    _orig(ordId);
    const autoOn = document.getElementById('auto-on-actuals');
    if (_driveConnected && (!autoOn || autoOn.checked)) {
      try { await driveSaveOrder(ordId); } catch (e) { console.warn('[Drive] Auto-save failed:', e.message); }
    }
  };
})();


// ─────────────────────────────────────────────────────────────
//  EXPOSE TO GLOBAL SCOPE
// ─────────────────────────────────────────────────────────────

window.driveConnect          = driveConnect;
window.driveDisconnect       = driveDisconnect;
window.driveSaveOrder        = driveSaveOrder;
window.driveSaveCollection   = driveSaveCollection;
window.driveSaveCustomer     = driveSaveCustomer;
window.driveSaveMasterSheet  = driveSaveMasterSheet;
window.driveSaveAllDelivered = driveSaveAllDelivered;
