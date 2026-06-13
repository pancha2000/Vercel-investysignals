/**
 * ws-polyfill.js — InvestySignals
 * Replaces native WebSocket with HTTP polling for Vercel serverless.
 * Drop-in replacement: existing HTML code needs NO changes.
 *
 * How it works:
 *   - Intercepts  new WebSocket(url)  for same-origin connections
 *   - Polls  /api/events        every 2.5s  (signal/announcement events)
 *   - Polls  /api/market/prices every 5s    (live ticker prices)
 *   - Fires  onopen / onmessage / onclose   exactly like native WS
 */
(function () {
  'use strict';

  var _NativeWS  = window.WebSocket;
  var POLL_MS    = 2500;
  var PRICE_MS   = 5000;

  // ── Polyfill constructor ────────────────────────────────────
  function WSPoll(url) {
    var me       = this;
    me.url       = url;
    me.readyState = 0;          // CONNECTING
    me._closed   = false;
    me._lastTs   = Date.now() - 8000;
    me._token    = null;
    me._pollTmr  = null;
    me._priceTmr = null;

    // Extract Firebase token from WebSocket URL ?token=…
    try {
      var u = new URL(url.replace(/^wss?/, 'https'), location.href);
      me._token = u.searchParams.get('token') || null;
    } catch (_) {}

    // Fire onopen after a short delay (mimic async WS handshake)
    setTimeout(function () {
      if (me._closed) return;
      me.readyState = 1;        // OPEN
      if (typeof me.onopen === 'function') me.onopen({ type: 'open' });
      me._startPolling();
    }, 150);
  }

  // ── Helpers ─────────────────────────────────────────────────
  WSPoll.prototype._headers = function () {
    return this._token ? { Authorization: 'Bearer ' + this._token } : {};
  };

  WSPoll.prototype._fire = function (data) {
    if (typeof this.onmessage === 'function')
      this.onmessage({ data: JSON.stringify(data) });
  };

  // ── Polling ─────────────────────────────────────────────────
  WSPoll.prototype._startPolling = function () {
    var me = this;
    me._pollEvents();
    me._pollPrices();
    me._pollTmr  = setInterval(function () { me._pollEvents(); }, POLL_MS);
    me._priceTmr = setInterval(function () { me._pollPrices(); }, PRICE_MS);
  };

  WSPoll.prototype._pollEvents = async function () {
    if (this._closed) return;
    try {
      var r = await fetch('/api/events?since=' + this._lastTs, { headers: this._headers() });
      if (!r.ok) return;
      var json = await r.json();
      if (json.ts)                    this._lastTs = json.ts;
      if (Array.isArray(json.events)) json.events.forEach(ev => this._fire(ev));
    } catch (_) {}
  };

  WSPoll.prototype._pollPrices = async function () {
    if (this._closed) return;
    try {
      var r = await fetch('/api/market/prices');
      if (!r.ok) return;
      var json = await r.json();
      if (json.type === 'market_update') this._fire(json);
    } catch (_) {}
  };

  // ── Standard WS interface ────────────────────────────────────
  WSPoll.prototype.send = function () {
    // Auth is handled via the token in request headers.
    // No client→server messages needed for this app's WS usage.
  };

  WSPoll.prototype.close = function () {
    this._closed = true;
    clearInterval(this._pollTmr);
    clearInterval(this._priceTmr);
    this.readyState = 3; // CLOSED
    if (typeof this.onclose === 'function') this.onclose({ code: 1000, reason: '' });
  };

  WSPoll.CONNECTING = 0;
  WSPoll.OPEN       = 1;
  WSPoll.CLOSING    = 2;
  WSPoll.CLOSED     = 3;

  // ── Override window.WebSocket for same-origin connections ────
  window.WebSocket = function (url, protocols) {
    try {
      var u = new URL(url.replace(/^wss?/, 'https'), location.href);
      if (u.hostname === location.hostname) return new WSPoll(url);
    } catch (_) {}
    // External WS URLs → use native
    return new _NativeWS(url, protocols);
  };

  window.WebSocket.CONNECTING = 0;
  window.WebSocket.OPEN       = 1;
  window.WebSocket.CLOSING    = 2;
  window.WebSocket.CLOSED     = 3;

  console.log('[WS-Polyfill] Ready — real-time via polling (/api/events + /api/market/prices)');
})();
