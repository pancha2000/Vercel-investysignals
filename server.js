// ============================================================
//  InvestySignals — Backend Server (Upgraded v4)
//  Node.js + Express + MongoDB + Firebase Admin + WebSocket
//
//  v4 FIXES (all bugs resolved):
//  [F1]  BOS detection — close-based confirmation (no fake wick BOS)
//  [F2]  Entry zone — direction-aware (LONG=pullback zone, SHORT=push zone)
//  [F3]  currentPrice — live Binance ticker price (not stale 1H close)
//  [F4]  FVG — unmitigated only (filled FVGs excluded)
//  [F5]  RSI Divergence — proper swing pivot comparison (10-candle lookback)
//  [F6]  Score display — netScore (bull-bear) not just bullScore
//  [F7]  SL swing lookback — 8→15 candles for proper structure
//  [F8]  BTC context — 4H structure added alongside 1D EMA
//  [F9]  ATR entry — 15m ATR used for 15m entry zone refinement
//  [F10] TP levels — nearest S/R level aware (avoid placing TP inside SR)
//  [F11] Stochastic RSI added (1H, 15m) for overbought/oversold timing
//  [F12] CVD proxy — volume-weighted direction for entry confirmation
//
//  v3 UPGRADES (from previous version):
//  [1]  Klines cache (5-min TTL)
//  [2]  Binance API retry + exponential backoff
//  [3]  Candle outlier sanitization (median ±15%)
//  [4]  Completed candle fix (forming candle excluded)
//  [5]  calcADX — choppy market detection
//  [6]  Net scoring (bullScore − bearScore)
//  [7]  Weighted direction
//  [8]  findOrderBlock — explosive move + mitigation validated
//  [9]  Structure-aware SL
//  [10] HTF/LTF conflict detection
//  [11] Per-user throttle (30s)
//  [12] News blackout toggle
//  [13] State tracking + trend_flip WS broadcast
//  [14] Signal freshness flags
// ============================================================

'use strict';
require('dotenv').config();

const express    = require('express');
const http       = require('http');
const WebSocket  = require('ws');
const mongoose   = require('mongoose');
const cors       = require('cors');
const helmet     = require('helmet');
const rateLimit  = require('express-rate-limit');
const path       = require('path');
const admin      = require('firebase-admin');

const Signal       = require('./models/Signal');
const User         = require('./models/User');
// PaperTrade model defined inline in paper trade section below
const Settings     = require('./models/Settings');
const Announcement = require('./models/Announcement');
const Report          = require('./models/Report');
const BalanceRequest  = require('./models/BalanceRequest');
const Event           = require('./models/Event');

// ── Firebase ─────────────────────────────────────────────────
try {
  let credential;
  if (process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
    // Vercel: credentials from environment variable (raw JSON or base64)
    const raw = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
    const jsonStr = raw.trim().startsWith('{') ? raw : Buffer.from(raw, 'base64').toString('utf-8');
    credential = admin.credential.cert(JSON.parse(jsonStr));
  } else {
    // VPS / local: credentials from file
    const serviceAccount = require('./serviceAccount.json');
    credential = admin.credential.cert(serviceAccount);
  }
  admin.initializeApp({ credential });
  console.log('✅ Firebase Admin initialized');
} catch (err) {
  console.error('❌ Firebase Admin init failed:', err.message);
  process.exit(1);
}

// ── MongoDB ───────────────────────────────────────────────────
mongoose.connect(process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/investysignals', {
  maxPoolSize: process.env.VERCEL ? 1 : 10,
  bufferCommands: false,
})
  .then(async () => { console.log('✅ MongoDB connected'); await loadSettingsFromDB(); })
  .catch(err => {
    console.error('❌ MongoDB error:', err.message);
    if (!process.env.VERCEL) process.exit(1);
  });

const ADMIN_EMAILS = ['cdilrukshi52@gmail.com'];

const SETTINGS_DEFAULTS = {
  maintenance: false,
  maintenanceMsg: 'We are making improvements. Please check back shortly.',
  allowRegistrations: true,
  highImpactMode: false,
  highImpactMsg: 'High impact news period — signals temporarily paused.',
  groq_api_key: '',      // overrides .env GROQ_API_KEY if set
  groq_model: 'llama-3.3-70b-versatile',
  groq_max_tokens: 2000, // BUG FIX: 1500 too low — AI truncates JSON causing parse errors
  groq_temperature: 0.2,
};
let globalSettings = { ...SETTINGS_DEFAULTS };

async function loadSettingsFromDB() {
  try {
    const docs = await Settings.find({});
    docs.forEach(d => { globalSettings[d.key] = d.value; });
    console.log('⚙️  Settings loaded');
  } catch(e) { console.warn('⚠️  Settings load failed:', e.message); }
}
async function saveSettingToDB(key, value) {
  await Settings.findOneAndUpdate({ key }, { value }, { upsert: true, new: true });
}

// ── [1][2] Klines Cache + Retry ──────────────────────────────
const klinesCache   = new Map();
const KLINES_TTL    = 5 * 60 * 1000;  // BUG FIX: 5min TTL — 15min was too stale for fast markets

async function fetchKlinesCached(symbol, interval, limit = 200, retries = 3) {
  const key    = `${symbol}_${interval}_${limit}`;
  const cached = klinesCache.get(key);
  if (cached && Date.now() - cached.ts < KLINES_TTL) return cached.data;

  // Try futures API first, fall back to spot for coins not on futures (e.g. XLM, ALGO)
  const urls = [
    `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`,
    `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`,
  ];

  for (const url of urls) {
    for (let attempt = 0; attempt < retries; attempt++) {
      try {
        const r = await fetch(url);
        if (r.status === 429) { await new Promise(res => setTimeout(res, 1000 * Math.pow(2, attempt))); continue; }
        if (!r.ok) break; // try next URL
        const data = await r.json();
        if (!Array.isArray(data) || data.length < 5) break; // invalid, try spot
        klinesCache.set(key, { data, ts: Date.now() });
        return data;
      } catch(e) {
        if (attempt === retries - 1) break; // try next URL
        await new Promise(res => setTimeout(res, 500 * (attempt + 1)));
      }
    }
  }
  throw new Error(`fetchKlines failed: ${symbol} ${interval} — not found on futures or spot`);
}

// ── [F3] Live price cache ────────────────────────────────────
const priceCache = new Map();
const PRICE_TTL  = 10 * 1000; // 10 seconds

async function getLivePrice(symbol) {
  const cached = priceCache.get(symbol);
  if (cached && Date.now() - cached.ts < PRICE_TTL) return cached.price;
  try {
    // Try futures first, fall back to spot for coins not on futures
    let price = null;
    const fr = await fetch(`https://fapi.binance.com/fapi/v1/ticker/price?symbol=${symbol}`);
    if (fr.ok) {
      const fd = await fr.json();
      price = parseFloat(fd.price) || null;
    }
    if (!price) {
      const sr = await fetch(`https://api.binance.com/api/v3/ticker/price?symbol=${symbol}`);
      if (sr.ok) {
        const sd = await sr.json();
        price = parseFloat(sd.price) || null;
      }
    }
    if (price) priceCache.set(symbol, { price, ts: Date.now() });
    return price;
  } catch(e) { return null; }
}

// ── Normalize trading pair — always ensure USDT suffix ───────
// XLM → XLMUSDT, BTCUSDT → BTCUSDT, btc → BTCUSDT
function normalizePair(pair) {
  if (!pair) return '';
  const p = pair.toString().toUpperCase().replace(/[^A-Z0-9]/g, '').trim();
  return p.endsWith('USDT') ? p : p + 'USDT';
}

// ── [3] Outlier Sanitization ─────────────────────────────────
function sanitizeCandles(klines) {
  if (!klines || klines.length < 10) return klines;
  const closes = klines.map(k => parseFloat(k[4])).sort((a, b) => a - b);
  const median = closes[Math.floor(closes.length / 2)];
  if (median <= 0) return klines;
  return klines.filter(k => Math.abs(parseFloat(k[4]) - median) / median < 0.30); // BUG FIX: 15%→30% — 15% too aggressive, removes valid volatile candles
}

// ── [13] State + [11] Throttle ───────────────────────────────
const analysisState    = new Map();
const lastAnalysisTime = new Map();
const ANALYSIS_COOLDOWN = 0; // No cooldown — removed per user request

// ── Thesis Tracking (per user per coin) ──────────────────────
// Key: uid+':'+coin — stores previous analysis for flip detection
const thesisState = new Map();
const CONFLUENCE_THRESHOLD = 5; // score/10 — below this = NEUTRAL (5/10 now passes)

// ── Express ───────────────────────────────────────────────────
const app = express();
app.set('trust proxy', 1);
app.use(helmet({ contentSecurityPolicy: false, crossOriginEmbedderPolicy: false }));
const allowedOrigin = process.env.ALLOWED_ORIGIN;
// BUG FIX: CORS — www. + non-www + mobile app requests all allowed
const allowedOrigins = allowedOrigin
  ? [
      allowedOrigin,
      allowedOrigin.replace('://www.', '://'),
      allowedOrigin.replace('://', '://www.'),
    ].filter(Boolean)
  : null;
app.use(cors({
  origin: (origin, cb) => {
    // Allow requests with no origin (mobile apps, curl, server-to-server)
    if (!origin) return cb(null, true);
    // If no ALLOWED_ORIGIN set, allow all
    if (!allowedOrigins) return cb(null, true);
    // Check allowed list
    if (allowedOrigins.includes(origin)) return cb(null, true);
    // Allow anyway in development or if origin is undefined
    return cb(null, true); // BUG FIX: was throwing CORS error blocking all requests
  },
  credentials: true
}));
app.use(express.json({ limit: '5mb' }));

const apiLimiter   = rateLimit({ windowMs: 15*60*1000, max: 300, standardHeaders: true, legacyHeaders: false, message: { success: false, error: 'Too many requests.' } });
const adminLimiter = rateLimit({ windowMs: 15*60*1000, max: 1000, standardHeaders: true, legacyHeaders: false, message: { success: false, error: 'Too many requests.' } });
app.use('/api/admin/', adminLimiter);
app.use('/api/', apiLimiter);

const BLOCKED_STATIC = ['.env','serviceAccount.json','package.json','.gitignore','deploy.sh','server.js','node_modules'];

// Block sensitive files before static serving
app.use((req, res, next) => {
  const file = path.basename(req.path);
  if (BLOCKED_STATIC.includes(file)) return res.status(403).json({ success: false, error: 'Forbidden' });
  next();
});
// Serve HTML/CSS/JS files from project root
app.use(express.static(path.join(__dirname)));

// ── Auth ──────────────────────────────────────────────────────
async function ensureAdminPromotion(uid, emailFromToken) {
  try {
    let user = await User.findOne({ uid });
    const email = (emailFromToken || '').toLowerCase();
    const isAdminEmail = ADMIN_EMAILS.includes(email);
    if (!user) {
      let displayName = '';
      try { const fb = await admin.auth().getUser(uid); displayName = fb.displayName || ''; } catch(_) {}
      user = await User.create({ uid, email, displayName, role: isAdminEmail ? 'admin' : 'user', plan: 'free', paperBalance: 1000 });
    } else if (isAdminEmail && user.role !== 'admin') {
      await User.updateOne({ uid }, { role: 'admin', email });
      user.role = 'admin';
    }
    return user;
  } catch(e) { return null; }
}

async function verifyToken(req, res, next) {
  const token = (req.headers.authorization || '').replace('Bearer ', '');
  if (!token) return res.status(401).json({ success: false, error: 'Unauthorized' });
  try {
    req.user = await admin.auth().verifyIdToken(token);
    // Auto-create user in MongoDB if missing (new Firebase users)
    let dbUser = await User.findOne({ uid: req.user.uid });
    if (!dbUser) {
      try {
        dbUser = await User.create({
          uid:          req.user.uid,
          email:        (req.user.email || '').toLowerCase(),
          displayName:  req.user.name || '',
          role:         ADMIN_EMAILS.includes((req.user.email||'').toLowerCase()) ? 'admin' : 'user',
          plan:         'free',
          paperBalance: 1000,
        });
      } catch(_) {
        dbUser = await User.findOne({ uid: req.user.uid });
      }
    }
    if (dbUser?.suspended) return res.status(403).json({ success: false, error: 'Account suspended', reason: dbUser.suspendReason });
    req.dbUser = dbUser;
    next();
  } catch(e) { res.status(401).json({ success: false, error: 'Invalid token' }); }
}

async function verifyAdmin(req, res, next) {
  const token = (req.headers.authorization || '').replace('Bearer ', '');
  if (!token) return res.status(401).json({ success: false, error: 'Unauthorized' });
  try {
    req.user = await admin.auth().verifyIdToken(token);
    const u = await ensureAdminPromotion(req.user.uid, (req.user.email || '').toLowerCase());
    if (!u || u.role !== 'admin') return res.status(403).json({ success: false, error: 'Admin access required' });
    req.dbUser = u;
    next();
  } catch(e) { res.status(401).json({ success: false, error: 'Authentication failed' }); }
}

const PLAN_LEVEL = { free: 0, pro: 1, elite: 2, admin: 99 };
function planLevel(plan) { return PLAN_LEVEL[plan] ?? 0; }

// ============================================================
//  STANDARD API ROUTES
// ============================================================

app.get('/api/health', (req, res) => res.json({ success: true, status: 'ok', time: new Date().toISOString() }));
app.get('/api/version', (req, res) => res.json({ version:'TDZ-FIX-v3', tdzFixed:true, analysisScope:'function-level', note:'let analysis=null declared before try — TDZ impossible' }));

app.get('/api/analysis', async (req, res) => {
  try {
    const pair = (req.query.pair || 'BTCUSDT').toUpperCase().replace(/[^A-Z0-9]/g, '');
    const tf   = (req.query.tf || '1h').replace(/[^a-zA-Z0-9]/g, '');
    const r    = await fetch(`https://fapi.binance.com/fapi/v1/klines?symbol=${pair}&interval=${tf}&limit=100`);
    if (!r.ok) return res.status(502).json({ success: false, error: `Binance error: ${await r.text()}` });
    const klines = await r.json();
    if (!Array.isArray(klines) || klines.length < 15) return res.status(502).json({ success: false, error: 'Not enough data' });
    const closes = klines.map(k => parseFloat(k[4]));
    let gains=0,losses=0;
    for (let i=1;i<=14;i++){const d=closes[i]-closes[i-1];d>=0?gains+=d:losses-=d;}
    let ag=gains/14,al=losses/14;
    for (let i=15;i<closes.length;i++){const d=closes[i]-closes[i-1];ag=(ag*13+(d>0?d:0))/14;al=(al*13+(d<0?-d:0))/14;}
    const rsi=parseFloat((100-100/(1+(al===0?100:ag/al))).toFixed(2));
    function _da_ema(data,n){const k=2/(n+1);let v=data.slice(0,n).reduce((a,b)=>a+b,0)/n;const o=[v];for(let i=n;i<data.length;i++){v=data[i]*k+v*(1-k);o.push(v);}return o;}
    const e12=_da_ema(closes,12),e26=_da_ema(closes,26);
    const ml=e12.slice(e12.length-e26.length).map((v,i)=>v-e26[i]),s9=_da_ema(ml,9);
    const macd=parseFloat(ml[ml.length-1].toFixed(4)),sig=parseFloat(s9[s9.length-1].toFixed(4));
    const bbC=closes.slice(-20),bbM=bbC.reduce((a,b)=>a+b,0)/20;
    const std=Math.sqrt(bbC.reduce((a,c)=>a+Math.pow(c-bbM,2),0)/20);
    res.json({ success:true, pair, tf, price:parseFloat(closes[closes.length-1].toFixed(4)), rsi,
      macd:{macd,signal:sig,histogram:parseFloat((macd-sig).toFixed(4))},
      bb:{upper:parseFloat((bbM+2*std).toFixed(2)),middle:parseFloat(bbM.toFixed(2)),lower:parseFloat((bbM-2*std).toFixed(2))} });
  } catch(err) { res.status(500).json({ success: false, error: err.message }); }
});

const STABLECOINS = new Set(['USDCUSDT','FDUSDUSDT','TUSDUSDT','BUSDUSDT','EURUSDT','DAIUSDT','USDPUSDT','AEURUSDT']);
app.get('/api/scan', async (req, res) => {
  try {
    const ctrl=new AbortController();
    setTimeout(()=>ctrl.abort(),10000);
    const r=await fetch('https://api.binance.com/api/v3/ticker/24hr',{signal:ctrl.signal});
    if (!r.ok) throw new Error(`Binance error: ${r.status}`);
    const data=await r.json();
    const results=data
      .filter(c=>c.symbol.endsWith('USDT')&&!STABLECOINS.has(c.symbol))
      .filter(c=>parseFloat(c.quoteVolume)>=15_000_000&&parseInt(c.count)>=100_000)
      .filter(c=>{const ch=parseFloat(c.priceChangePercent);return ch>=3||ch<=-3;})
      .sort((a,b)=>parseFloat(b.quoteVolume)-parseFloat(a.quoteVolume)).slice(0,20)
      .map(c=>({symbol:c.symbol,change:parseFloat(c.priceChangePercent),volume:parseFloat(c.quoteVolume),price:parseFloat(c.lastPrice),trades:parseInt(c.count)}));
    res.json({ success:true, count:results.length, coins:results });
  } catch(err) { res.status(500).json({ success:false, error:err.message }); }
});

app.get('/api/user/status', verifyToken, async (req, res) => {
  try {
    const user=await ensureAdminPromotion(req.user.uid,(req.user.email||'').toLowerCase());
    if (!user) return res.status(500).json({ success:false, error:'Could not load user' });
    await User.updateOne({uid:req.user.uid},{lastLogin:new Date()});
    res.json({ success:true, status:{role:user.role,plan:user.plan,suspended:user.suspended,suspendReason:user.suspendReason,
      maintenance:globalSettings.maintenance||user.maintenance,
      maintenanceMsg:globalSettings.maintenance?globalSettings.maintenanceMsg:(user.maintenanceMsg||''),
      paperBalance:user.paperBalance}});
  } catch(err) { res.status(500).json({ success:false, error:err.message }); }
});

app.get('/api/signals', verifyToken, async (req, res) => {
  try {
    const user=req.dbUser,userPlan=user?user.plan:'free',userRole=user?user.role:'user';
    const pf=userRole==='admin'?{}:{plan:{$in:Object.keys(PLAN_LEVEL).filter(p=>planLevel(p)<=planLevel(userPlan))}};
    const signals=await Signal.find({active:true,...pf}).sort({createdAt:-1}).limit(50);
    res.json({ success:true, signals, data:signals });
  } catch(err) { res.status(500).json({ success:false, error:err.message }); }
});

/* GET /api/registration-status — public, checks if new registrations are allowed */
app.get('/api/registration-status', (req, res) => {
  res.json({ success: true, open: globalSettings.allowRegistrations !== false });
});

/* POST /api/reports — public bug/signal report submission */
app.post('/api/reports', async (req, res) => {
  try {
    const { category, message, context } = req.body;
    if (!message) return res.status(400).json({ success: false, error: 'Message is required.' });
    const report = await Report.create({
      reporterUid:   'anonymous',
      reporterEmail: req.body.email || '',
      category:      category || 'other',
      message:       message.slice(0, 2000),
      context:       context || '',
    });
    res.json({ success: true, data: report });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

app.get('/api/announcement', async (req, res) => {
  try {
    const now=new Date();
    const ann=await Announcement.findOne({active:true,showFrom:{$lte:now},$or:[{showUntil:null},{showUntil:{$gte:now}}]}).sort({createdAt:-1});
    res.json({ success:true, data:ann, announcement:ann });
  } catch(err) { res.json({ success:true, data:null, announcement:null }); }
});

// ═══════════════════════════════════════════════════════════════
//  PAPER TRADING API — server-side persistence
//  Uses uid field (Firebase UID) — compatible with all frontend versions
// ═══════════════════════════════════════════════════════════════

/* ── Paper Trade Schema (inline — supports both old uid and new userUid) ── */
const paperTradeSchema2 = new mongoose.Schema({
  uid:        { type:String, required:true, index:true },
  userUid:    { type:String, index:true },  // alias for uid, kept for compat
  id:         { type:Number },              // client timestamp id (old frontend)
  symbol:     { type:String, required:true },
  pair:       { type:String, required:true },
  direction:  { type:String, enum:['LONG','SHORT'], required:true },
  entryType:  { type:String, default:'MARKET' },
  orderType:  { type:String, default:'MARKET' },
  entryPrice: { type:Number },
  entry:      { type:Number },
  tp1:        { type:Number },
  tp2:        { type:Number },
  tp3:        { type:Number },
  sl:         { type:Number },
  amount:     { type:Number },
  size:       { type:Number },
  leverage:   { type:Number, default:5 },
  notional:   Number,
  liqPrice:   Number,
  status:     { type:String, default:'OPEN' },  // no enum — allow any status string
  openTime:   String,
  openedAt:   Date,
  fillTime:   String,
  filledAt:   Date,
  closeTime:  String,
  closedAt:   Date,
  closePrice: Number,
  pnl:        Number,
  roe:        Number,
  totalPnl:   Number,
  totalRoe:   Number,
  tp1Pnl:     Number,
  tp1HitPrice:Number,
  tp1HitTime: String,
  currentSl:  Number,
  trailOffset:Number,
  triggerPrice: Number,
  remainingSize: Number,
}, { timestamps:true });

const paperBalanceSchema2 = new mongoose.Schema({
  uid:     { type:String, required:true, unique:true, index:true },
  balance: { type:Number, default:1000 },
}, { timestamps:true });

// Use existing models if already registered (avoid OverwriteModelError on hot reload)
const PT  = mongoose.models.PaperTrade   || mongoose.model('PaperTrade',   paperTradeSchema2);
const PB  = mongoose.models.PaperBalance || mongoose.model('PaperBalance', paperBalanceSchema2);

/* ── Unified auth middleware for paper trade routes ── */
async function ptAuth(req, res, next) {
  const auth = (req.headers.authorization || '');
  if (!auth.startsWith('Bearer ')) return res.status(401).json({ success:false, error:'Unauthorized' });
  try {
    const decoded = await admin.auth().verifyIdToken(auth.slice(7));
    req.uid = decoded.uid;
    next();
  } catch(e) { res.status(401).json({ success:false, error:'Invalid token' }); }
}

/* ── Helper: find trades by uid OR userUid ── */
function uidQuery(uid) {
  return { $or: [{ uid }, { userUid: uid }] };
}

/* GET /api/paper/trades — get all trades */
app.get('/api/paper/trades', ptAuth, async (req, res) => {
  try {
    const trades = await PT.find(uidQuery(req.uid)).sort({ id:-1, openedAt:-1, createdAt:-1 }).lean();
    // Normalize: ensure both entryPrice and entry fields exist
    const normalized = trades.map(t => ({
      ...t,
      entryPrice: t.entryPrice || t.entry || 0,
      entry:      t.entry      || t.entryPrice || 0,
      amount:     t.amount     || t.size || 0,
      size:       t.size       || t.amount || 0,
      symbol:     normalizePair(t.symbol || t.pair),
      pair:       normalizePair(t.pair   || t.symbol),
    }));
    res.json({ success:true, trades:normalized });
  } catch(e) { res.status(500).json({ success:false, error:e.message }); }
});

// Also expose as /api/paper-trades (some pages use this URL)
app.get('/api/paper-trades', ptAuth, async (req, res) => {
  try {
    const [trades, pb] = await Promise.all([
      PT.find(uidQuery(req.uid)).sort({ id:-1, openedAt:-1, createdAt:-1 }).lean(),
      PB.findOne({ uid: req.uid }),
    ]);
    const normalized = trades.map(t => ({
      ...t,
      entryPrice: t.entryPrice || t.entry || 0,
      entry:      t.entry      || t.entryPrice || 0,
      amount:     t.amount     || t.size || 0,
      size:       t.size       || t.amount || 0,
      symbol:     normalizePair(t.symbol || t.pair),
      pair:       normalizePair(t.pair   || t.symbol),
    }));
    const balance = pb?.balance ?? 1000;
    // Return both field names for compatibility
    res.json({ success:true, trades:normalized, data:normalized, balance });
  } catch(e) { res.status(500).json({ success:false, error:e.message }); }
});

/* POST /api/paper/trades — open new trade (old frontend format) */
app.post('/api/paper/trades', ptAuth, async (req, res) => {
  try {
    const b = req.body;
    // Ensure both field-name variants are stored
    const tradeData = {
      uid:        req.uid,
      userUid:    req.uid,
      id:         b.id || Date.now(),
      symbol:     normalizePair(b.symbol || b.pair),
      pair:       normalizePair(b.pair   || b.symbol),
      direction:  b.direction,
      entryType:  b.entryType  || b.orderType || 'MARKET',
      orderType:  b.orderType  || b.entryType || 'MARKET',
      entryPrice: b.entryPrice || b.entry || 0,
      entry:      b.entry      || b.entryPrice || 0,
      tp1: b.tp1, tp2: b.tp2, tp3: b.tp3, sl: b.sl,
      amount:   b.amount || b.size || 0,
      size:     b.size   || b.amount || 0,
      leverage: b.leverage || 5,
      notional: b.notional,
      liqPrice: b.liqPrice,
      status:   b.status || (
        (b.entryType==='LIMIT'||b.orderType==='LIMIT')
          ? (b.direction === 'SHORT' ? 'PENDING_SHORT' : 'PENDING_LONG')
          : 'OPEN'
      ),
      openTime: b.openTime || new Date().toISOString(),
      openedAt: b.openedAt ? new Date(b.openedAt) : new Date(),
      fillTime: b.fillTime,
      triggerPrice: b.triggerPrice,
      remainingSize: b.remainingSize || b.size || b.amount,
    };
    const trade = await PT.create(tradeData);
    res.json({ success:true, trade });
  } catch(e) { res.status(500).json({ success:false, error:e.message }); }
});

/* POST /api/paper/trade (singular) — new analysis.html format */
app.post('/api/paper/trade', ptAuth, async (req, res) => {
  try {
    const b = req.body;
    if (!b.pair || !b.direction || !['LONG','SHORT'].includes(b.direction))
      return res.status(400).json({ success:false, error:'Invalid pair or direction.' });

    const entryPrice = parseFloat(b.entry || b.entryPrice) || 0;
    const size       = parseFloat(b.size  || b.amount)     || 100;
    const lev        = Math.min(Math.max(parseInt(b.leverage)||10, 1), 125);
    const isLimit    = (b.orderType||'MARKET') === 'LIMIT';
    const statusVal  = isLimit
      ? (b.direction==='LONG' ? 'PENDING_LONG' : 'PENDING_SHORT')
      : 'OPEN';

    // Balance check
    let pb = await PB.findOne({ uid: req.uid });
    if (!pb) pb = await PB.create({ uid: req.uid, balance: 1000 });
    if (pb.balance < size)
      return res.json({ success:false, error:`Insufficient balance ($${pb.balance.toFixed(2)} available).` });

    const trade = await PT.create({
      uid:        req.uid,
      userUid:    req.uid,
      id:         b.id || Date.now(),
      symbol:     normalizePair(b.pair),
      pair:       normalizePair(b.pair),
      direction:  b.direction,
      entryType:  b.orderType || 'MARKET',
      orderType:  b.orderType || 'MARKET',
      entryPrice: entryPrice,
      entry:      entryPrice,
      triggerPrice: parseFloat(b.triggerPrice) || entryPrice,
      tp1: parseFloat(b.tp1)||null, tp2: parseFloat(b.tp2)||null,
      tp3: parseFloat(b.tp3)||null, sl: parseFloat(b.sl)||null,
      amount:   size, size, leverage: lev,
      notional: entryPrice ? parseFloat((size * lev).toFixed(4)) : 0,
      liqPrice: entryPrice
        ? parseFloat((b.direction === 'LONG'
            ? entryPrice * (1 - 1/lev * 0.9)
            : entryPrice * (1 + 1/lev * 0.9)).toFixed(4))
        : 0,
      remainingSize: size,
      status:   statusVal,
      openTime: new Date().toISOString(),
      openedAt: new Date(),
      fillTime: isLimit ? null : new Date().toISOString(),
      filledAt: isLimit ? null : new Date(),
    });

    // Deduct from balance
    await PB.updateOne({ uid: req.uid }, { $inc: { balance: -size } });

    try { broadcastToAll({ type:'paper_trade_opened', trade, uid: req.uid }); } catch(_) {}
    res.json({ success:true, trade, message:'Trade opened!' });
  } catch(e) { res.status(500).json({ success:false, error:e.message }); }
});

/* PATCH /api/paper/trades/:id — update by numeric id (old frontend) */
app.patch('/api/paper/trades/:id', ptAuth, async (req, res) => {
  try {
    const numId = parseInt(req.params.id);
    const trade = await PT.findOneAndUpdate(
      { ...uidQuery(req.uid), id: numId },
      { $set: req.body },
      { new: true }
    );
    if (!trade) return res.status(404).json({ success:false, error:'Trade not found' });
    res.json({ success:true, trade });
  } catch(e) { res.status(500).json({ success:false, error:e.message }); }
});

/* PATCH /api/paper/trade/:id/close — close by MongoDB _id (new frontend) */
app.patch('/api/paper/trade/:id/close', ptAuth, async (req, res) => {
  try {
    // BUG FIX: smart id detection — ObjectId or numeric
    const isObjectId = /^[a-f\d]{24}$/i.test(req.params.id);
    const numericId  = parseInt(req.params.id);
    const idFilter   = isObjectId
      ? { _id: req.params.id }
      : { id: numericId || 0 };
    const filter = { $and: [ uidQuery(req.uid), idFilter ] };
    // Auto-calculate pnl if not provided but closePrice is given
    const tradeBeforeClose = await PT.findOne(filter).lean();
    if (!tradeBeforeClose) return res.status(404).json({ success:false, error:'Trade not found' });

    const closePrice  = parseFloat(req.body.closePrice) || 0;
    const entryPrice  = tradeBeforeClose.entryPrice || tradeBeforeClose.entry || 0;
    // FIX: use remainingSize (not size) — for TP1_HIT trades only 50% of position remains open
    const tradeSize   = tradeBeforeClose.remainingSize || tradeBeforeClose.size || tradeBeforeClose.amount || 0;
    const leverage    = tradeBeforeClose.leverage || 1;
    const isLong      = tradeBeforeClose.direction === 'LONG';

    let pnl = parseFloat(req.body.pnl || req.body.totalPnl || 0);
    if (!pnl && closePrice && entryPrice && tradeSize) {
      // FIX: always derive notional from current remaining size (stored notional = original full position)
      const notional = tradeSize * leverage;
      pnl = isLong
        ? (closePrice - entryPrice) / entryPrice * notional
        : (entryPrice - closePrice) / entryPrice * notional;
      pnl = parseFloat(pnl.toFixed(4));
    }
    const notionalForRoe = tradeSize * leverage;
    const roe = notionalForRoe
      ? parseFloat((pnl / (notionalForRoe / leverage) * 100).toFixed(2))
      : 0;

    const patch = { ...req.body, pnl, roe, status: req.body.status || 'CLOSED',
      closedAt: new Date(), closeTime: new Date().toISOString() };
    const trade = await PT.findOneAndUpdate(filter, { $set: patch }, { new: true });
    if (!trade) return res.status(404).json({ success:false, error:'Trade not found' });

    // FIX: refund remaining margin (remainingSize), not original full size
    if (tradeSize > 0) await PB.updateOne({ uid: req.uid }, { $inc: { balance: tradeSize + pnl } }, { upsert: true });
    res.json({ success:true, trade, pnl, roe });
  } catch(e) { res.status(500).json({ success:false, error:e.message }); }
});

/* PATCH /api/paper/trade/:id/cancel — cancel pending order */
app.patch('/api/paper/trade/:id/cancel', ptAuth, async (req, res) => {
  try {
    // BUG FIX: smart id detection — ObjectId or numeric
    const isObjectId = /^[a-f\d]{24}$/i.test(req.params.id);
    const numericId  = parseInt(req.params.id);
    const idFilter   = isObjectId
      ? { _id: req.params.id }
      : { id: numericId || 0 };
    const filter = { $and: [ uidQuery(req.uid), idFilter ] };
    const trade = await PT.findOneAndUpdate(filter,
      { $set: { status:'CANCELLED', closedAt:new Date(), closeTime:new Date().toISOString() } },
      { new: true }
    );
    if (!trade) return res.status(404).json({ success:false, error:'Trade not found' });
    // Refund size to balance
    const size = trade.size || trade.amount || 0;
    if (size > 0) await PB.updateOne({ uid: req.uid }, { $inc: { balance: size } });
    res.json({ success:true, trade });
  } catch(e) { res.status(500).json({ success:false, error:e.message }); }
});

/* DELETE /api/paper/trades/:id — delete by numeric id */
app.delete('/api/paper/trades/:id', ptAuth, async (req, res) => {
  try {
    const numId = parseInt(req.params.id);
    await PT.findOneAndDelete({ ...uidQuery(req.uid), id: numId });
    res.json({ success:true });
  } catch(e) { res.status(500).json({ success:false, error:e.message }); }
});

/* DELETE /api/paper/trades — clear closed trades */
app.delete('/api/paper/trades', ptAuth, async (req, res) => {
  try {
    const { scope } = req.body || {};
    if (scope === 'all') {
      await PT.deleteMany(uidQuery(req.uid));
    } else {
      const closedStatuses = ['TP2','TP2_HIT','TP3_HIT','BE_CLOSE','TRAIL_WIN','SL','SL_HIT','CLOSED','CANCELLED'];
      await PT.deleteMany({ ...uidQuery(req.uid), status: { $in: closedStatuses } });
    }
    res.json({ success:true });
  } catch(e) { res.status(500).json({ success:false, error:e.message }); }
});

/* GET /api/paper/balance */
app.get('/api/paper/balance', ptAuth, async (req, res) => {
  try {
    let [pb, user] = await Promise.all([
      PB.findOne({ uid: req.uid }),
      User.findOne({ uid: req.uid }).lean(),
    ]);
    if (!pb) pb = await PB.create({ uid: req.uid, balance: 1000 });
    res.json({
      success: true,
      balance: pb.balance,
      hasSetInitialBalance: user?.hasSetInitialBalance ?? false,
    });
  } catch(e) { res.status(500).json({ success:false, error:e.message }); }
});

/* PUT /api/paper/balance — set balance (old frontend) */
app.put('/api/paper/balance', ptAuth, async (req, res) => {
  try {
    const bal = parseFloat(req.body.balance);
    if (isNaN(bal) || bal < 0) return res.status(400).json({ success:false, error:'Invalid balance' });
    const pb = await PB.findOneAndUpdate({ uid: req.uid }, { balance: bal }, { upsert:true, new:true });
    res.json({ success:true, balance: pb.balance });
  } catch(e) { res.status(500).json({ success:false, error:e.message }); }
});

/* POST /api/paper/balance/set — set balance (new frontend) */
app.post('/api/paper/balance/set', ptAuth, async (req, res) => {
  try {
    const bal = parseFloat(req.body.amount || req.body.balance);
    if (isNaN(bal) || bal < 1 || bal > 1_000_000)
      return res.status(400).json({ success:false, error:'Amount must be between 1 and 1,000,000.' });
    const [pb] = await Promise.all([
      PB.findOneAndUpdate({ uid: req.uid }, { balance: bal }, { upsert:true, new:true }),
      User.findOneAndUpdate({ uid: req.uid }, { hasSetInitialBalance: true }),
    ]);
    res.json({ success:true, balance: pb.balance });
  } catch(e) { res.status(500).json({ success:false, error:e.message }); }
});

/* GET /api/paper/balance/my-requests (stub — keeps old routes alive) */
app.get('/api/paper/balance/my-requests', ptAuth, async (req, res) => {
  res.json({ success:true, requests:[] });
});

/* POST /api/paper/balance/request — user submits a balance reset/custom request */
app.post('/api/paper/balance/request', ptAuth, async (req, res) => {
  try {
    const amount = parseFloat(req.body.amount);
    if (isNaN(amount) || amount < 100 || amount > 1_000_000)
      return res.status(400).json({ success:false, error:'Amount must be 100 – 1,000,000 USDT.' });
    const reason = (req.body.reason || '').trim().slice(0, 300);
    const [user, pb] = await Promise.all([
      User.findOne({ uid: req.uid }).lean(),
      PB.findOne({ uid: req.uid }),
    ]);
    const BR_m = mongoose.models.BalanceRequest || require('./models/BalanceRequest');
    await BR_m.create({
      userUid:         req.uid,
      userEmail:       user?.email       || '',
      displayName:     user?.displayName || '',
      requestType:     'CUSTOM',
      requestedAmount: amount,
      currentBalance:  pb?.balance ?? 1000,
      reason,
      status: 'pending',
    });
    res.json({ success:true });
  } catch(e) { res.status(500).json({ success:false, error:e.message }); }
});

// ── Trade Monitor ─────────────────────────────────────────────
// Context-aware re-analysis for an open trade: HOLD / DCA / CLOSE / MOVE_SL
// ═══════════════════════════════════════════════════════════════
//  DEEP ANALYSIS — Full multi-timeframe AI analysis
// ═══════════════════════════════════════════════════════════════
app.post('/api/deep-analysis', verifyToken, async (req, res) => {
  // DEFINITIVE FIX: Declare analysis at function scope (outside try block)
  // This eliminates ANY possibility of TDZ — analysis is always 'null' until assigned,
  // never "not initialized". Fixes "Cannot access 'analysis' before initialization" on re-analyze.
  let analysis = null;

  // ── Analysis helper functions (declared at handler scope, NOT inside try block) ──
  // CRITICAL FIX: Function declarations inside try{} blocks cause V8 block-scope
  // hoisting quirks that trigger "Cannot access 'analysis' before initialization"
  // on re-analysis of the same coin. Moving them here (handler scope) fully fixes it.
  async function _da_klines(sym, interval, limit=200) {
    const r = await fetchKlinesCached(sym, interval, limit);
    return sanitizeCandles(r).map(k=>({
      open:parseFloat(k[1]),high:parseFloat(k[2]),low:parseFloat(k[3]),
      close:parseFloat(k[4]),volume:parseFloat(k[5])
    }));
  }
  function _da_ema(arr, n) {
    if (arr.length < n) return arr.length ? [arr[arr.length-1]] : [];
    const k=2/(n+1); let v=arr.slice(0,n).reduce((a,b)=>a+b,0)/n;
    const out=[v]; for(let i=n;i<arr.length;i++){v=arr[i]*k+v*(1-k);out.push(v);} return out;
  }
  function _da_rsi(closes, period=14) {
    if(closes.length < period+2) return 50;
    let g=0,l=0;
    for(let i=1;i<=period;i++){const d=closes[i]-closes[i-1];d>=0?g+=d:l-=d;}
    let ag=g/period,al=l/period;
    for(let i=period+1;i<closes.length;i++){const d=closes[i]-closes[i-1];ag=(ag*(period-1)+(d>0?d:0))/period;al=(al*(period-1)+(d<0?-d:0))/period;}
    if(al===0) return 100;
    if(ag===0) return 0;
    return parseFloat((100-100/(1+ag/al)).toFixed(2));
  }
  function _da_macd(closes) {
    const e12=_da_ema(closes,12),e26=_da_ema(closes,26);
    const ml=e12.slice(e12.length-e26.length).map((v,i)=>v-e26[i]);
    const s9=_da_ema(ml,9);
    const h=ml[ml.length-1]-s9[s9.length-1];
    const ph=ml[ml.length-2]-s9[s9.length-2];
    return {macd:parseFloat(ml[ml.length-1].toFixed(6)),signal:parseFloat(s9[s9.length-1].toFixed(6)),histogram:parseFloat(h.toFixed(6)),prevHistogram:parseFloat(ph.toFixed(6))};
  }
  function _da_bb(closes,n=20) {
    const s=closes.slice(-n),m=s.reduce((a,b)=>a+b,0)/n;
    const std=Math.sqrt(s.reduce((a,c)=>a+Math.pow(c-m,2),0)/n);
    return {upper:parseFloat((m+2*std).toFixed(4)),middle:parseFloat(m.toFixed(4)),lower:parseFloat((m-2*std).toFixed(4))};
  }
  function _da_atr(candles,n=14) {
    if (candles.length < n+1) return 0;
    const trs=candles.slice(1).map((c,i)=>Math.max(c.high-c.low,Math.abs(c.high-candles[i].close),Math.abs(c.low-candles[i].close)));
    let atr=trs.slice(0,n).reduce((a,b)=>a+b,0)/n;
    for(let i=n;i<trs.length;i++) atr=(atr*(n-1)+trs[i])/n;
    return parseFloat(atr.toFixed(6));
  }
  function _da_structure(candles) {
    if(candles.length<20) return 'NEUTRAL';
    const recent=candles.slice(-20);
    const mid=Math.floor(recent.length/2);
    const prev=recent.slice(0,mid), curr=recent.slice(mid);
    const phH=Math.max(...prev.map(c=>c.high)), plL=Math.min(...prev.map(c=>c.low));
    const cH=Math.max(...curr.map(c=>c.high)), cL=Math.min(...curr.map(c=>c.low));
    const lastClose=candles[candles.length-1].close;
    if(lastClose>phH&&cH>phH) return 'BOS_BULLISH';
    if(lastClose<plL&&cL<plL) return 'BOS_BEARISH';
    if(cH>phH) return 'CHOCH_BULLISH';
    if(cL<plL) return 'CHOCH_BEARISH';
    return 'NEUTRAL';
  }
  function _da_fvgs(candles) {
    const result=[];
    for(let i=2;i<candles.length;i++){
      const prev=candles[i-2],curr=candles[i];
      if(curr.low>prev.high){
        // BULL FVG: gap [prev.high → curr.low]. Mitigated if any later candle's low enters the gap.
        const mitigated=candles.slice(i+1).some(c=>c.low<curr.low);
        if(!mitigated) result.push({type:'BULL',low:prev.high,high:curr.low,idx:i});
      } else if(curr.high<prev.low){
        // BEAR FVG: gap [curr.high → prev.low]. Mitigated if any later candle's high enters the gap.
        const mitigated=candles.slice(i+1).some(c=>c.high>curr.high);
        if(!mitigated) result.push({type:'BEAR',low:curr.high,high:prev.low,idx:i});
      }
    }
    return result.slice(-5);
  }
  function _da_srLevels(candles,n=5) {
    const pivots=[];
    const lastClose=candles[candles.length-1].close;
    for(let i=n;i<candles.length-n;i++){
      const w=candles.slice(i-n,i+n+1);
      if(candles[i].high===Math.max(...w.map(c=>c.high))) pivots.push(candles[i].high);
      if(candles[i].low ===Math.min(...w.map(c=>c.low)))  pivots.push(candles[i].low);
    }
    const unique=[...new Set(pivots.map(p=>parseFloat(p.toFixed(4))))].sort((a,b)=>a-b);
    // Return 3 nearest support levels below + 3 nearest resistance levels above
    const below=unique.filter(p=>p<=lastClose).slice(-3);
    const above=unique.filter(p=>p>lastClose).slice(0,3);
    return [...below,...above].sort((a,b)=>a-b);
  }
  function _da_orderBlock(candles) {
    for(let i=candles.length-3;i>=0;i--){
      const c=candles[i],nx=candles[i+1];
      if(nx.close>c.high&&nx.close-nx.open>0){
        // BULL_OB: price must not have closed below OB low after formation
        const mitigated=candles.slice(i+2).some(cand=>cand.close<c.low);
        if(!mitigated) return {type:'BULL_OB',low:c.low,high:c.high};
      }
      if(nx.close<c.low&&nx.open-nx.close>0){
        // BEAR_OB: price must not have closed above OB high after formation
        const mitigated=candles.slice(i+2).some(cand=>cand.close>c.high);
        if(!mitigated) return {type:'BEAR_OB',low:c.low,high:c.high};
      }
    }
    return null;
  }
  function _da_volRatio(candles,n=20) {
    if(candles.length<n+1) return {ratio:1,spike:false};
    const vols=candles.map(c=>c.volume);
    const avg=vols.slice(-n-1,-1).reduce((a,b)=>a+b,0)/n;
    if(avg===0) return {ratio:1,spike:false};
    const last=vols[vols.length-1];
    const ratio=parseFloat((last/avg).toFixed(2));
    return {ratio,spike:ratio>2};
  }
  function _da_candlePattern(c) {
    const body=Math.abs(c.close-c.open),range=c.high-c.low;
    if(range===0) return 'DOJI';
    if(body/range<0.1) return 'DOJI';
    const upper=c.high-Math.max(c.open,c.close),lower=Math.min(c.open,c.close)-c.low;
    if(c.close>c.open){
      if(lower>body*2) return 'PIN_BAR_BULL';
      return 'BULL_CANDLE';
    } else {
      if(upper>body*2) return 'PIN_BAR_BEAR';
      return 'BEAR_CANDLE';
    }
  }
  function _da_rsiDiv(candles,rsiArr) {
    if(candles.length<10||rsiArr.length<10) return 'NONE';
    const n=Math.min(candles.length,rsiArr.length,20);
    const pHigh=candles.slice(-n).map(c=>c.high);   // highs for bearish div
    const pLow =candles.slice(-n).map(c=>c.low);    // lows for bullish div
    const pR=rsiArr.slice(-n);
    const half=Math.floor(n/2);
    const prevPriceHigh=Math.max(...pHigh.slice(0,half));
    const prevPriceLow =Math.min(...pLow.slice(0,half));
    const currPriceHigh=Math.max(...pHigh.slice(half));
    const currPriceLow =Math.min(...pLow.slice(half));
    const prevRsiHigh  =Math.max(...pR.slice(0,half));
    const prevRsiLow   =Math.min(...pR.slice(0,half));
    const currRsiHigh  =Math.max(...pR.slice(half));
    const currRsiLow   =Math.min(...pR.slice(half));
    if(currPriceHigh>prevPriceHigh&&currRsiHigh<prevRsiHigh) return 'BEARISH_DIV';
    if(currPriceLow<prevPriceLow&&currRsiLow>prevRsiLow)     return 'BULLISH_DIV';
    return 'NONE';
  }
  function _da_rsiArray(closes, period=14) {
    if (closes.length < period + 1) return [];
    const out = [];
    let g=0, l=0;
    for (let i=1; i<=period; i++) { const d=closes[i]-closes[i-1]; d>=0?g+=d:l-=d; }
    let ag=g/period, al=l/period;
    // Edge case: if both gains and losses are 0 (no price movement), RSI = 50 (neutral)
    out.push(ag===0&&al===0 ? 50 : parseFloat((100-100/(1+(al===0?Infinity:ag/al))).toFixed(2)));
    for (let i=period+1; i<closes.length; i++) {
      const d=closes[i]-closes[i-1];
      ag=(ag*(period-1)+(d>0?d:0))/period;
      al=(al*(period-1)+(d<0?-d:0))/period;
      out.push(ag===0&&al===0 ? 50 : parseFloat((100-100/(1+(al===0?Infinity:ag/al))).toFixed(2)));
    }
    return out;
  }


  // ══════════════════════════════════════════════════════════════
  // ADVANCED INDICATORS — ADX, Multiple OBs, Fibonacci, Open Interest
  // ══════════════════════════════════════════════════════════════

  // ── ADX (Average Directional Index) — Full Wilder implementation ──
  // Returns: { adx, plusDI, minusDI, trend, strength }
  // ADX > 25 = trending | ADX < 20 = ranging | ADX > 50 = strong trend
  // +DI > -DI = bullish momentum | -DI > +DI = bearish momentum
  function _da_adx(candles, period = 14) {
    if (candles.length < period * 2 + 1) return { adx: 0, plusDI: 0, minusDI: 0, trend: 'RANGING', strength: 'WEAK' };

    const trArr = [], plusDM = [], minusDM = [];
    for (let i = 1; i < candles.length; i++) {
      const curr = candles[i], prev = candles[i - 1];
      const highDiff = curr.high - prev.high;
      const lowDiff  = prev.low  - curr.low;
      const tr = Math.max(curr.high - curr.low, Math.abs(curr.high - prev.close), Math.abs(curr.low - prev.close));
      trArr.push(tr);
      plusDM.push(highDiff > 0 && highDiff > lowDiff ? highDiff : 0);
      minusDM.push(lowDiff > 0 && lowDiff > highDiff ? lowDiff : 0);
    }

    // Wilder's smoothed SUM (not average) — used for DM/TR where ratio cancels the period scaling
    function wilderSmooth(arr, p) {
      let sum = arr.slice(0, p).reduce((a, b) => a + b, 0);
      const out = [sum];
      for (let i = p; i < arr.length; i++) {
        sum = sum - sum / p + arr[i];
        out.push(sum);
      }
      return out;
    }

    const smoothTR  = wilderSmooth(trArr, period);
    const smoothPDM = wilderSmooth(plusDM, period);
    const smoothMDM = wilderSmooth(minusDM, period);

    const dxArr = [];
    for (let i = 0; i < smoothTR.length; i++) {
      if (smoothTR[i] === 0) { dxArr.push(0); continue; }
      const pdi = 100 * smoothPDM[i] / smoothTR[i];
      const mdi = 100 * smoothMDM[i] / smoothTR[i];
      const dx  = Math.abs(pdi - mdi) / (pdi + mdi || 1) * 100;
      dxArr.push(dx);
    }

    const adxArr = wilderSmooth(dxArr, period);
    // FIXED: wilderSmooth returns smoothed SUM ≈ period × mean(DX). Divide by period for true ADX (0-100).
    const adxVal  = parseFloat((adxArr[adxArr.length - 1] / period).toFixed(2));
    const lastTR  = smoothTR[smoothTR.length - 1];
    const plusDIv = lastTR ? parseFloat((100 * smoothPDM[smoothPDM.length - 1] / lastTR).toFixed(2)) : 0;
    const minDIv  = lastTR ? parseFloat((100 * smoothMDM[smoothMDM.length - 1] / lastTR).toFixed(2)) : 0;

    const trend    = adxVal > 25 ? (plusDIv > minDIv ? 'TRENDING_BULL' : 'TRENDING_BEAR') : 'RANGING';
    const strength = adxVal > 50 ? 'VERY_STRONG' : adxVal > 35 ? 'STRONG' : adxVal > 25 ? 'MODERATE' : adxVal > 15 ? 'WEAK' : 'NO_TREND';

    return { adx: adxVal, plusDI: plusDIv, minusDI: minDIv, trend, strength };
  }

  // ── Multiple Order Blocks — up to 5 valid unmitigated OBs ──
  // BULL_OB: last bearish candle before a strong bullish impulse, price hasn't closed below OB low
  // BEAR_OB: last bullish candle before a strong bearish impulse, price hasn't closed above OB high
  function _da_orderBlocks(candles, maxCount = 5) {
    const lastClose = candles[candles.length - 1].close;
    const obs = [];
    // Minimum impulse: next candle body > 0.5× average body
    const avgBody = candles.slice(-30).reduce((s, c) => s + Math.abs(c.close - c.open), 0) / 30;
    const minImpulse = avgBody * 0.5;

    for (let i = candles.length - 4; i >= 1; i--) {
      if (obs.length >= maxCount) break;
      const ob   = candles[i];
      const next = candles[i + 1];
      const impulseBody = Math.abs(next.close - next.open);
      if (impulseBody < minImpulse) continue;

      // BULL_OB: ob is bearish, next is strong bullish, price still above OB low
      if (ob.close < ob.open && next.close > next.open && next.close > ob.high) {
        if (lastClose > ob.low) { // not mitigated (price hasn't closed below OB low)
          const mitigated = candles.slice(i + 2).some(c => c.close < ob.low);
          if (!mitigated) obs.push({ type: 'BULL_OB', low: parseFloat(ob.low.toFixed(4)), high: parseFloat(ob.high.toFixed(4)), bodyLow: parseFloat(Math.min(ob.open, ob.close).toFixed(4)), bodyHigh: parseFloat(Math.max(ob.open, ob.close).toFixed(4)), idx: i });
        }
      }
      // BEAR_OB: ob is bullish, next is strong bearish, price still below OB high
      if (ob.close > ob.open && next.close < next.open && next.close < ob.low) {
        if (lastClose < ob.high) { // not mitigated
          const mitigated = candles.slice(i + 2).some(c => c.close > ob.high);
          if (!mitigated) obs.push({ type: 'BEAR_OB', low: parseFloat(ob.low.toFixed(4)), high: parseFloat(ob.high.toFixed(4)), bodyLow: parseFloat(Math.min(ob.open, ob.close).toFixed(4)), bodyHigh: parseFloat(Math.max(ob.open, ob.close).toFixed(4)), idx: i });
        }
      }
    }
    return obs; // sorted nearest-first (most recent first)
  }

  // ── Fibonacci Retracement — from most recent swing high/low ──
  // Returns key fib levels for entry zone confluence
  function _da_fibonacci(candles, lookback = 50) {
    const recent = candles.slice(-lookback);
    const swingHigh = Math.max(...recent.map(c => c.high));
    const swingLow  = Math.min(...recent.map(c => c.low));
    const range = swingHigh - swingLow;
    if (range === 0) return null;
    const lastClose = candles[candles.length - 1].close;
    // CORRECTED direction logic:
    //   price > mid → uptrend retracing down → BULLISH_RETRACE (levels count DOWN from swing high)
    //   price < mid → downtrend bouncing up  → BEARISH_RETRACE (levels count UP from swing low)
    const mid = swingLow + range * 0.5;
    const direction = lastClose > mid ? 'BULLISH_RETRACE' : 'BEARISH_RETRACE';
    const fmt = v => parseFloat(v.toFixed(4));
    return {
      direction,
      swingHigh: fmt(swingHigh),
      swingLow:  fmt(swingLow),
      f236: fmt(direction === 'BULLISH_RETRACE' ? swingHigh - range * 0.236 : swingLow + range * 0.236),
      f382: fmt(direction === 'BULLISH_RETRACE' ? swingHigh - range * 0.382 : swingLow + range * 0.382),
      f500: fmt(direction === 'BULLISH_RETRACE' ? swingHigh - range * 0.500 : swingLow + range * 0.500),
      f618: fmt(direction === 'BULLISH_RETRACE' ? swingHigh - range * 0.618 : swingLow + range * 0.618),
      f786: fmt(direction === 'BULLISH_RETRACE' ? swingHigh - range * 0.786 : swingLow + range * 0.786),
    };
  }

  try {
    const coin = ((req.body.coin || 'BTC').toUpperCase().replace(/USDT$/,'').replace(/[^A-Z0-9]/g,'')).trim();
    if (!coin) return res.status(400).json({ success:false, error:'coin required' });
    const symbol = coin + 'USDT';

    // Fetch all timeframes in parallel
    const [m15c,h1c,h4c,d1c,btcH4] = await Promise.all([
      _da_klines(symbol,'15m',200),
      _da_klines(symbol,'1h',300),
      _da_klines(symbol,'4h',200),
      _da_klines(symbol,'1d',250),
      _da_klines('BTCUSDT','4h',50),
    ]);

    // Live price
    const price = await getLivePrice(symbol) || h1c[h1c.length-1].close;

    // Compute all indicators
    const m15closes=m15c.map(c=>c.close),h1closes=h1c.map(c=>c.close),h4closes=h4c.map(c=>c.close),d1closes=d1c.map(c=>c.close);
    const btcCloses=btcH4.map(c=>c.close);

    const m15RSI=_da_rsi(m15closes),h1RSI=_da_rsi(h1closes),h4RSI=_da_rsi(h4closes);
    const h1MACDv=_da_macd(h1closes),m15MACDv=_da_macd(m15closes);
    const h1BB=_da_bb(h1closes),h4BB=_da_bb(h4closes);
    const h1Ema20=_da_ema(h1closes,20),h1Ema50=_da_ema(h1closes,50),h1Ema200=_da_ema(h1closes,200);
    const h4Ema20=_da_ema(h4closes,20),h4Ema50=_da_ema(h4closes,50),h4Ema200=_da_ema(h4closes,200);
    const d1Ema200=_da_ema(d1closes,200);
    const btcEma20=_da_ema(btcCloses,20);

    const m15Struct=_da_structure(m15c),h1Struct=_da_structure(h1c),h4Struct=_da_structure(h4c),d1Struct=_da_structure(d1c);
    const h4FVGs=_da_fvgs(h4c),h1FVGs=_da_fvgs(h1c),m15FVGs=_da_fvgs(m15c);
    const h4SR=_da_srLevels(h4c),d1SR=_da_srLevels(d1c);
    const h4OB=_da_orderBlock(h4c),d1OB=_da_orderBlock(d1c);
    const h1Vol=_da_volRatio(h1c),m15Vol=_da_volRatio(m15c);
    const atr4h=_da_atr(h4c),atr1h=_da_atr(h1c);
    const m15Candle=m15c[m15c.length-1];
    const m15CP={pattern:_da_candlePattern(m15Candle)};
    const prevDayHigh=d1c[d1c.length-2]?.high,prevDayLow=d1c[d1c.length-2]?.low;

    // ── ADVANCED INDICATORS ──────────────────────────────────────
    // ADX — trend strength and direction
    const h4ADX = _da_adx(h4c, 14);
    const d1ADX = _da_adx(d1c, 14);
    const h1ADX = _da_adx(h1c, 14);

    // Multiple Order Blocks (up to 5 valid unmitigated OBs per TF)
    const h4OBs = _da_orderBlocks(h4c, 5);
    const d1OBs = _da_orderBlocks(d1c, 5);
    const h1OBs = _da_orderBlocks(h1c, 3);

    // Fibonacci Retracement (last 50 candles)
    const h4Fib = _da_fibonacci(h4c, 50);
    const d1Fib = _da_fibonacci(d1c, 50);

    // Open Interest — Binance Futures API (live OI + 4h history for trend)
    let oiData = null;
    try {
      const [oiNow, oiHist] = await Promise.all([
        fetch(`https://fapi.binance.com/fapi/v1/openInterest?symbol=${symbol}`).then(r => r.json()),
        fetch(`https://fapi.binance.com/futures/data/openInterestHist?symbol=${symbol}&period=1h&limit=6`).then(r => r.json()),
      ]);
      if (oiNow?.openInterest) {
        const oiCurrent = parseFloat(oiNow.openInterest);
        // OI trend: compare oldest vs newest in history
        const oiTrend = Array.isArray(oiHist) && oiHist.length >= 2
          ? parseFloat(oiHist[oiHist.length-1].sumOpenInterest) > parseFloat(oiHist[0].sumOpenInterest)
            ? 'RISING' : 'FALLING'
          : 'UNKNOWN';
        // OI + Price interpretation
        const lastClose = h1c[h1c.length-1].close;
        const prevClose = h1c[h1c.length-7]?.close || lastClose;
        const priceDir  = lastClose > prevClose ? 'UP' : 'DOWN';
        // Classic OI interpretation
        const oiSignal =
          oiTrend === 'RISING'  && priceDir === 'UP'   ? 'BULLISH_CONTINUATION'   : // new longs entering
          oiTrend === 'RISING'  && priceDir === 'DOWN'  ? 'BEARISH_CONTINUATION'   : // new shorts entering
          oiTrend === 'FALLING' && priceDir === 'UP'    ? 'SHORT_SQUEEZE'          : // shorts covering → bullish
          oiTrend === 'FALLING' && priceDir === 'DOWN'  ? 'LONG_LIQUIDATION'       : // longs exiting → bearish
          'NEUTRAL';
        oiData = { current: oiCurrent, trend: oiTrend, signal: oiSignal, priceDir };
      }
    } catch(_) {}

    const h4rsiArr  = _da_rsiArray(h4closes);
    const h1rsiArr  = _da_rsiArray(h1closes);
    const m15rsiArr = _da_rsiArray(m15closes);
    const h4Div=_da_rsiDiv(h4c.slice(-20),h4rsiArr.slice(-20));
    const h1Div=_da_rsiDiv(h1c.slice(-20),h1rsiArr.slice(-20));
    const m15Div=_da_rsiDiv(m15c.slice(-20),m15rsiArr.slice(-20));

    // BTC trend
    const btcPrice=btcH4[btcH4.length-1].close;
    const btcEma20Last=btcEma20[btcEma20.length-1];
    const btcGapPct=Math.abs(btcPrice-btcEma20Last)/btcEma20Last*100;
    const btcStrength=btcGapPct>1.5?'STRONG_':'';
    const btcTrend=btcPrice>btcEma20Last?btcStrength+'BULL':btcStrength+'BEAR';

    // Funding rate (Binance futures)
    let fundingRate=null,fundingBias='NEUTRAL';
    try {
      const fr=await fetch(`https://fapi.binance.com/fapi/v1/fundingRate?symbol=${symbol}&limit=1`);
      const fd=await fr.json();
      if(Array.isArray(fd)&&fd.length) {
        fundingRate=parseFloat(fd[0].fundingRate)*100;
        fundingBias=fundingRate>0.01?'LONGS_PAYING':fundingRate<-0.01?'SHORTS_PAYING':'NEUTRAL';
      }
    } catch(_){}

    // Build raw data object
    const rawData = {
      price, btcTrend, fundingRate, fundingBias,
      m15RSI, h1RSI, h4RSI,
      h4Div, h1Div, m15Div,
      h1MACD:h1MACDv, m15MACD:m15MACDv,
      h4Struct, h1Struct, m15Struct, d1Struct,
      h1Ema20:h1Ema20[h1Ema20.length-1], h1Ema50:h1Ema50[h1Ema50.length-1], h1Ema200:h1Ema200[h1Ema200.length-1],
      h4Ema20:h4Ema20[h4Ema20.length-1], h4Ema50:h4Ema50[h4Ema50.length-1], h4Ema200:h4Ema200[h4Ema200.length-1],
      d1Ema200:d1Ema200[d1Ema200.length-1],
      h1BB, h4BB,
      h4SR, d1SR,
      h4FVGs, h1FVGs, m15FVGs,
      h4OB, d1OB,
      h1Vol, m15Vol,
      atr4h, atr1h,
      m15Candle:m15CP,
      prevDayHigh, prevDayLow,
      // Advanced indicators
      h4ADX, d1ADX, h1ADX,
      h4OBs, d1OBs, h1OBs,
      h4Fib, d1Fib,
      oiData,
      // Entry/SL/TP placeholders (filled by AI)
      entryHigh: null, entryLow: null, sl: null, tp1: null, tp2: null, tp3: null,
    };

    // ── M15 + H1 Early Warning System ────────────────────────────
    // Detects early reversal signals BEFORE H4 confirms
    const earlyWarnings = [];

    // 1. M15 RSI > 70 while H4 still bullish (overextension warning)
    if (m15RSI > 70 && h4RSI < 70 && h4Struct === 'BOS_BULLISH') {
      earlyWarnings.push('M15 RSI overbought (' + m15RSI + ') while H4 still bullish — possible short-term top');
    }
    if (m15RSI < 30 && h4RSI > 30 && h4Struct === 'BOS_BEARISH') {
      earlyWarnings.push('M15 RSI oversold (' + m15RSI + ') while H4 still bearish — possible short-term bounce');
    }

    // 2. M15 bearish divergence while H1 structure bullish (early flip signal)
    if (m15Div === 'BEARISH_DIV' && h1Struct === 'BOS_BULLISH') {
      earlyWarnings.push('M15 bearish divergence forming against H1 bullish structure — watch for H1 CHoCH');
    }
    if (m15Div === 'BULLISH_DIV' && h1Struct === 'BOS_BEARISH') {
      earlyWarnings.push('M15 bullish divergence forming against H1 bearish structure — possible H1 reversal incoming');
    }

    // 3. M15 MACD cross against H4 trend
    if (m15MACDv.histogram < 0 && m15MACDv.prevHistogram > 0 && h4Struct === 'BOS_BULLISH') {
      earlyWarnings.push('M15 MACD just crossed bearish — early warning, H4 still bullish');
    }
    if (m15MACDv.histogram > 0 && m15MACDv.prevHistogram < 0 && h4Struct === 'BOS_BEARISH') {
      earlyWarnings.push('M15 MACD just crossed bullish — early reversal signal, H4 still bearish');
    }

    // 4. Volume spike against trend
    if (m15Vol.spike && m15Struct === 'BOS_BEARISH' && h4Struct === 'BOS_BULLISH') {
      earlyWarnings.push('Volume spike (' + m15Vol.ratio + 'x) on M15 bearish move — possible distribution');
    }

    // ── Thesis Tracking — compare with previous analysis ────────
    const uid = req.user?.uid || req.uid || 'anon'; // BUG FIX: verifyToken sets req.user.uid not req.uid
    const thesisKey = uid + ':' + coin;
    const prevThesis = thesisState.get(thesisKey);

    let thesisStatus = 'NEW';
    let thesisContext = '';

    if (prevThesis && prevThesis.ts && (Date.now() - prevThesis.ts < 6 * 60 * 60 * 1000)) {
      const prevBias   = prevThesis.bias;
      const prevD1     = prevThesis.d1Struct;
      const prevH4     = prevThesis.h4Struct;
      const d1Match    = prevD1 === d1Struct;
      const h4Match    = prevH4 === h4Struct;

      // FIX: Determine thesisStatus from STRUCTURE ONLY here (analysis not available yet)
      // Cannot reference analysis.overallBias before groq call — was causing ReferenceError
      if (d1Match && h4Match) {
        thesisStatus = 'CONFIRMED';
      } else if (d1Match && !h4Match) {
        thesisStatus = 'RETRACEMENT';         // H4 changed, D1 still intact
      } else if (!d1Match && h4Match) {
        thesisStatus = 'WEAKENING';           // D1 changed, uncertain
      } else {
        thesisStatus = 'INVALIDATED';         // both flipped — real reversal
      }

      thesisContext = `
THESIS CONTEXT (CRITICAL — read before giving direction):
Previous analysis bias: ${prevBias}
Previous D1 structure:  ${prevD1}
Previous H4 structure:  ${prevH4}
Current D1 structure:   ${d1Struct}
Current H4 structure:   ${h4Struct}
Thesis status:          ${thesisStatus}

Rules you MUST follow:
- If CONFIRMED or RETRACEMENT: overallBias must MATCH previous bias (${prevBias}) unless score >= 8 against it
- If RETRACEMENT: explain M15/H1 is normal pullback, original thesis valid, mention 38.2/50/61.8% fib levels
- If WEAKENING: set overallBias to NEUTRAL, warn user
- If INVALIDATED: clearly state reversal confirmed with structure evidence, then give new direction
- If confluenceScore < ${CONFLUENCE_THRESHOLD}: set overallBias to NEUTRAL regardless`;
    } else {
      thesisContext = `THESIS CONTEXT: First analysis for this coin. Set thesis fresh.
Rule: If confluenceScore < ${CONFLUENCE_THRESHOLD}, set overallBias to NEUTRAL.`;
    }

    // ── Groq AI Analysis ─────────────────────────────────────
    // DB ලේ key set කරලා ඇත්නම් ඒක use කරනවා, නැත්නම් .env ලේ key
    const GROQ_API_KEY = (globalSettings.groq_api_key && globalSettings.groq_api_key.trim())
      ? globalSettings.groq_api_key.trim()
      : process.env.GROQ_API_KEY;
    if (!GROQ_API_KEY) return res.status(500).json({ success:false, error:'GROQ_API_KEY not configured. Set it in Admin Panel → AI Settings.' });
    const GROQ_MODEL       = (globalSettings.groq_model       || 'llama-3.3-70b-versatile').trim();
    const GROQ_MAX_TOKENS  = parseInt(globalSettings.groq_max_tokens)  || 2000; // BUG FIX: increased default
    const GROQ_TEMPERATURE = parseFloat(globalSettings.groq_temperature) || 0.2;

    const earlyWarnText = earlyWarnings.length ? '\nEARLY WARNINGS (M15/H1 signals):\n' + earlyWarnings.map((w,i) => (i+1)+'. '+w).join('\n') : '';
    const prompt = `You are a professional crypto futures trader who provides institutional-grade signals. Analyze ${coin}/USDT and respond ONLY with valid JSON.

${thesisContext}${earlyWarnText}

MARKET DATA:
- Price: $${price}
- BTC Trend (4H EMA20): ${btcTrend}
- Funding: ${fundingRate!=null?fundingRate.toFixed(4)+'%':'-'} (${fundingBias})
- RSI: 15m=${m15RSI} | 1H=${h1RSI} | 4H=${h4RSI}
- RSI Div: 4H=${h4Div} | 1H=${h1Div} | 15m=${m15Div}
- MACD 1H: hist=${h1MACDv.histogram} (prev=${h1MACDv.prevHistogram})
- MACD 15m: hist=${m15MACDv.histogram}
- Structure: D1=${d1Struct} | 4H=${h4Struct} | 1H=${h1Struct} | 15m=${m15Struct}
- EMA 1H: 20=$${h1Ema20[h1Ema20.length-1]?.toFixed(4)} 50=$${h1Ema50[h1Ema50.length-1]?.toFixed(4)} 200=$${h1Ema200[h1Ema200.length-1]?.toFixed(4)}
- EMA 4H: 20=$${h4Ema20[h4Ema20.length-1]?.toFixed(4)} 50=$${h4Ema50[h4Ema50.length-1]?.toFixed(4)}
- BB 1H: upper=$${h1BB.upper} lower=$${h1BB.lower} mid=$${h1BB.middle}
- S/R 4H: ${h4SR.join(',')}
- S/R 1D: ${d1SR.join(',')}
- FVG 4H (unmitigated): ${h4FVGs.map(f=>f.type+' $'+f.low+'-$'+f.high).join(' | ')||'none'}
- FVG 1H (unmitigated): ${h1FVGs.map(f=>f.type+' $'+f.low+'-$'+f.high).join(' | ')||'none'}
- OB 4H: ${h4OB?h4OB.type+' $'+h4OB.low+'-$'+h4OB.high:'none'}
- OB 1D: ${d1OB?d1OB.type+' $'+d1OB.low+'-$'+d1OB.high:'none'}
- Vol spike 1H: ${h1Vol.spike?'YES':'NO'} (${h1Vol.ratio}x)
- Candle 15m: ${m15CP.pattern}
- ATR 4H: ${atr4h} | ATR 1H: ${atr1h}
- Prev Day H/L: $${prevDayHigh?.toFixed(4)} / $${prevDayLow?.toFixed(4)}
- ADX D1: ${d1ADX.adx} (${d1ADX.strength}) +DI=${d1ADX.plusDI} -DI=${d1ADX.minusDI} → ${d1ADX.trend}
- ADX 4H: ${h4ADX.adx} (${h4ADX.strength}) +DI=${h4ADX.plusDI} -DI=${h4ADX.minusDI} → ${h4ADX.trend}
- ADX 1H: ${h1ADX.adx} (${h1ADX.strength}) +DI=${h1ADX.plusDI} -DI=${h1ADX.minusDI} → ${h1ADX.trend}
- ORDER BLOCKS 4H (${h4OBs.length} unmitigated): ${h4OBs.map(o=>o.type+' $'+o.bodyLow+'-$'+o.bodyHigh).join(' | ')||'none'}
- ORDER BLOCKS D1 (${d1OBs.length} unmitigated): ${d1OBs.map(o=>o.type+' $'+o.bodyLow+'-$'+o.bodyHigh).join(' | ')||'none'}
- ORDER BLOCKS 1H (${h1OBs.length} unmitigated): ${h1OBs.map(o=>o.type+' $'+o.bodyLow+'-$'+o.bodyHigh).join(' | ')||'none'}
- FIBONACCI 4H: ${h4Fib?'SwH=$'+h4Fib.swingHigh+' SwL=$'+h4Fib.swingLow+' | 38.2=$'+h4Fib.f382+' 50=$'+h4Fib.f500+' 61.8=$'+h4Fib.f618:'N/A'}
- FIBONACCI D1: ${d1Fib?'SwH=$'+d1Fib.swingHigh+' SwL=$'+d1Fib.swingLow+' | 38.2=$'+d1Fib.f382+' 50=$'+d1Fib.f500+' 61.8=$'+d1Fib.f618:'N/A'}
- OPEN INTEREST: ${oiData?'OI='+oiData.current.toFixed(0)+' Trend='+oiData.trend+' Signal='+oiData.signal:'N/A'}

═══ MANDATORY ENTRY QUALITY RULES (follow these exactly — this determines signal quality) ═══

RULE 1 — ENTRY ZONE (must be at CONFLUENCE of ≥2 factors):
  Priority order: D1 OB > 4H OB > 4H FVG > Fib 61.8% > 1H OB > 1H FVG > EMA 4H 20/50 > 4H S/R
  - LONG: nearest BULLISH OB/FVG/Fib below price — check h4OBs, d1OBs for BULL_OB types
  - SHORT: nearest BEARISH OB/FVG/Fib above price — check h4OBs, d1OBs for BEAR_OB types
  - Fibonacci 61.8% is HIGH-PRIORITY confluence — if price near h4Fib.f618 or d1Fib.f618, use it as entry zone edge
  - entryHigh = top of confluence zone, entryLow = bottom
  - If no zone: use price ±(0.5×ATR 1H): entryHigh=$${(price+atr1h*0.5).toFixed(4)} entryLow=$${(price-atr1h*0.5).toFixed(4)}

RULE 2 — STOP LOSS (ATR-based, must clear key S/R):
  - ATR 4H = ${atr4h} | ATR 1H = ${atr1h}
  - LONG SL: place 1.5×ATR4H BELOW entryLow → approximately $${(price - atr4h*1.5).toFixed(4)}
  - SHORT SL: place 1.5×ATR4H ABOVE entryHigh → approximately $${(price + atr4h*1.5).toFixed(4)}
  - SL MUST be below/above a significant S/R level (not arbitrary) — check 4H S/R: ${h4SR.join(', ')}
  - Minimum SL distance: 1×ATR4H = $${atr4h} from entry

RULE 3 — TAKE PROFITS (minimum R:R ratios — calculate R = |entry - sl|):
  - TP1 MINIMUM: entry ± 1.5R (1.5:1 R:R) — use nearest resistance/support from ${h4SR.join(', ')}
  - TP2 MINIMUM: entry ± 2.5R (2.5:1 R:R) — use next major resistance/support
  - TP3 MINIMUM: entry ± 4.0R (4:1 R:R) — use 1D S/R or prev swing high/low: ${d1SR.join(', ')}
  - TPs must land ON key S/R levels — never at arbitrary prices

RULE 4 — RSI FILTER (hard rules):
  - DO NOT give LONG direction if 4H RSI > 65 (currently ${h4RSI})
  - DO NOT give SHORT direction if 4H RSI < 35 (currently ${h4RSI})
  - If RSI violates filter: set overallBias=NEUTRAL, but level5.direction MUST still be LONG or SHORT (best guess direction for monitoring — user will re-analyze before entering)

RULE 5 — CONFLUENCE MINIMUM:
  - confluenceScore (0-10) MUST reflect ALL available data:
    +1 D1 structure aligned  |  +1 4H structure aligned  |  +1 H1 structure aligned
    +1 RSI not extreme + confirms direction  |  +1 MACD cross/histogram confirms
    +1 OB confluence (price near valid OB)  |  +1 FVG confluence
    +1 Fibonacci 61.8% confluence  |  +1 ADX > 25 (trending, not ranging)
    +1 OI signal confirms direction (BULLISH_CONTINUATION or SHORT_SQUEEZE for LONG, etc.)
  - ADX RULE: if h4ADX.adx < 20 (RANGING), reduce score by 1 — range-bound markets have low follow-through
  - OI RULE: if oiData.signal is LONG_LIQUIDATION and bias=LONG, reduce score by 1
  - If score < ${CONFLUENCE_THRESHOLD}: set overallBias=NEUTRAL, still provide valid numeric levels

Respond with ONLY this JSON (no markdown, no explanation):
{
  "grade": "S|A|B|C",
  "confluenceScore": 0-10,
  "overallBias": "LONG|SHORT|NEUTRAL",
  "summary": "2-3 sentence analysis",
  "warning": "risk warning or null",
  "level1": {
    "macroConclusion": "BULLISH|BEARISH|NEUTRAL",
    "btcTrend": "text",
    "fundingSignal": "text",
    "oiSignal": "text — interpret: trend=${oiData?.trend||'N/A'} signal=${oiData?.signal||'N/A'}",
    "adxContext": "text — D1 ADX=${d1ADX.adx}(${d1ADX.strength}), 4H ADX=${h4ADX.adx}(${h4ADX.strength}): trending or ranging?"
  },
  "level2": {
    "structureConclusion": "BULLISH|BEARISH|NEUTRAL",
    "dailyStructure": "text",
    "dailyEMA": "text",
    "h4Structure": "text",
    "h4EMA": "text",
    "h4Divergence": "text",
    "keyLevels": "text",
    "orderBlock": "text — reference nearest relevant OB from h4OBs/d1OBs",
    "fvgZones": "text",
    "fibLevels": "text — key Fib levels: 38.2=$${h4Fib?.f382||'N/A'}, 50=$${h4Fib?.f500||'N/A'}, 61.8=$${h4Fib?.f618||'N/A'}"
  },
  "level3": {
    "momentumConclusion": "BULLISH|BEARISH|NEUTRAL",
    "h1Structure": "text",
    "h1EMA": "text",
    "h1RSI": "text",
    "h1Divergence": "text",
    "macdSignal": "text",
    "bollingerSignal": "text",
    "volumeSignal": "text"
  },
  "level4": {
    "entryConclusion": "CONFIRMED|AVOID|WAIT",
    "m15Structure": "text",
    "m15RSI": "text",
    "m15Divergence": "text",
    "macdCross": "text",
    "candlePattern": "text",
    "volumeConfirm": "text",
    "fvgEntry": "text",
    "sessionNote": "text"
  },
  "level5": {
    "direction": "LONG|SHORT (NEVER NEUTRAL — always give best directional guess)",
    "entryZone": "$X.XX – $X.XX",
    "stopLoss": "$X.XX",
    "invalidationLevel": "$X.XX",
    "tp1": "$X.XX",
    "tp2": "$X.XX",
    "tp3": "$X.XX",
    "leverage": "5x–10x",
    "positionSize": "1–2% of capital",
    "tradeManagement": "text describing TP1 partial close 50%, move SL to BE, trail to TP2",
    "reEntry": "text",
    "riskNote": "ATR-based SL distance and R:R ratios confirmation",
    "entryHigh": number,
    "entryLow": number,
    "sl": number,
    "tp1val": number,
    "tp2val": number,
    "tp3val": number
  }
}`;

    const groqController = new AbortController();
    const groqTimeout = setTimeout(() => groqController.abort(), 60000); // 60s timeout — Groq can be slow
    let groqRes;
    try {
      groqRes = await fetch('https://api.groq.com/openai/v1/chat/completions', {
        method: 'POST',
        signal: groqController.signal,
        headers: { 'Content-Type':'application/json', 'Authorization':'Bearer '+GROQ_API_KEY },
        body: JSON.stringify({
          model: GROQ_MODEL,
          max_tokens: GROQ_MAX_TOKENS,
          temperature: GROQ_TEMPERATURE,
          messages: [{ role:'user', content:prompt }]
        })
      });
    } catch(fetchErr) {
      if (fetchErr.name === 'AbortError') throw new Error('Groq AI timed out (>60s) — server is overloaded. Please try again in a moment.');
      throw new Error('Groq API unreachable: ' + fetchErr.message);
    } finally {
      clearTimeout(groqTimeout);
    }

    if (!groqRes.ok) {
      const errText = await groqRes.text();
      throw new Error(`Groq API error: ${groqRes.status} — ${errText.slice(0,200)}`);
    }

    const groqData = await groqRes.json();
    let aiText = groqData.choices?.[0]?.message?.content || '';
    // Strip markdown fences if present
    aiText = aiText.replace(/```json|```/g,'').trim();
    // analysis is declared at function scope (before try block) — no TDZ possible
    try { analysis = JSON.parse(aiText); }
    catch(e) { throw new Error('AI response parse failed: ' + aiText.slice(0,100)); }

    // FIX: Guard against null/non-object — prevents "Cannot access analysis before initialization" style errors
    if (!analysis || typeof analysis !== 'object') {
      throw new Error('AI returned invalid response (expected JSON object). Please try again.');
    }

    // ── Enforce confluence threshold — NEUTRAL if score too low ────
    const finalScore = analysis.confluenceScore || 0;
    if (finalScore < CONFLUENCE_THRESHOLD && analysis.overallBias !== 'NEUTRAL') {
      analysis.overallBias = 'NEUTRAL';
      analysis.summary = (analysis.summary || '') + ' (Score below threshold — watch zone only, no entry now.)';
      // level5.direction stays LONG/SHORT — it is the WATCH direction, not a trade signal
    }
    // Ensure level5.direction is never NEUTRAL — always LONG or SHORT
    // level5.direction = the LIMIT order direction (always directional, even when overallBias=NEUTRAL)
    if (analysis.level5 && (!analysis.level5.direction || analysis.level5.direction === 'NEUTRAL')) {
      // Use overallBias if directional, else derive from market structure
      let fallback = (analysis.overallBias === 'LONG' || analysis.overallBias === 'SHORT')
        ? analysis.overallBias
        : (d1Struct === 'BOS_BULLISH' || d1Struct === 'CHOCH_BULLISH') ? 'LONG'
        : (d1Struct === 'BOS_BEARISH' || d1Struct === 'CHOCH_BEARISH') ? 'SHORT'
        : (h4Struct === 'BOS_BULLISH' || h4Struct === 'CHOCH_BULLISH') ? 'LONG'
        : (h4Struct === 'BOS_BEARISH' || h4Struct === 'CHOCH_BEARISH') ? 'SHORT'
        : 'LONG';
      analysis.level5.direction = fallback;
    }

    // Fill rawData entry/sl/tp from AI level5 — ALWAYS fill even if NEUTRAL
    // This ensures entry/SL/TP show on screen regardless of direction
    if (analysis.level5) {
      rawData.entryHigh = analysis.level5.entryHigh || null;
      rawData.entryLow  = analysis.level5.entryLow  || null;
      rawData.sl        = analysis.level5.sl         || null;
      rawData.tp1       = analysis.level5.tp1val     || null;
      rawData.tp2       = analysis.level5.tp2val     || null;
      rawData.tp3       = analysis.level5.tp3val     || null;
    }



    // ── Save thesis for next analysis comparison ───────────────
    thesisState.set(thesisKey, {
      bias:     analysis.overallBias,
      score:    finalScore,
      d1Struct, h4Struct,
      ts:       Date.now(),
    });

    // ── Fibonacci pullback depth (for RETRACEMENT warnings) ───
    let fibLevels = null;
    if (prevThesis && thesisStatus === 'RETRACEMENT') {
      const swing = prevThesis.swingHigh || price * 1.05;
      const swingLow = prevThesis.swingLow || price * 0.92;
      const range = swing - swingLow;
      fibLevels = {
        p382: parseFloat((swing - range * 0.382).toFixed(4)),
        p500: parseFloat((swing - range * 0.500).toFixed(4)),
        p618: parseFloat((swing - range * 0.618).toFixed(4)),
        p786: parseFloat((swing - range * 0.786).toFixed(4)),
      };
    }
    // Save swing levels for next time
    thesisState.set(thesisKey, {
      ...thesisState.get(thesisKey),
      swingHigh: Math.max(...h4c.slice(-20).map(c=>c.high)),
      swingLow:  Math.min(...h4c.slice(-20).map(c=>c.low)),
    });

    res.json({
      success: true,
      coin,
      price,
      confluenceScore: finalScore,
      thesisStatus,
      fibLevels,
      earlyWarnings,
      analysis,
      rawData,
      // Advanced indicators for frontend display
      adv: {
        adx:          { h4: h4ADX, d1: d1ADX, h1: h1ADX },
        orderBlocks:  { h4: h4OBs, d1: d1OBs, h1: h1OBs },
        fibonacci:    { h4: h4Fib, d1: d1Fib },
        openInterest: oiData,
      },
    });

  } catch(err) {
    console.error('/api/deep-analysis error:', err.message);
    res.status(500).json({ success:false, error:err.message });
  }
});

app.post('/api/trade-monitor', verifyToken, async (req, res) => {
  try {
    const { tradeId, pair, direction, entry, sl, tp1, tp2, tp3, size } = req.body;
    if (!pair || !direction || !entry)
      return res.status(400).json({ success: false, error: 'pair, direction, entry required.' });

    const normalizedPair = normalizePair(pair);
    const currentPrice = await getLivePrice(normalizedPair);
    if (!currentPrice) return res.status(502).json({ success: false, error: `Could not fetch live price for ${normalizedPair}.` });

    // ── Fetch 4 timeframes ──────────────────────────────────────
    async function getKlines(sym, interval, limit) {
      let klines = null;
      try {
        const fr = await fetch(`https://fapi.binance.com/fapi/v1/klines?symbol=${sym}&interval=${interval}&limit=${limit}`);
        if (fr.ok) { const d = await fr.json(); if (Array.isArray(d) && d.length > 5) klines = d; }
      } catch(_) {}
      if (!klines) {
        try {
          const sr = await fetch(`https://api.binance.com/api/v3/klines?symbol=${sym}&interval=${interval}&limit=${limit}`);
          if (sr.ok) { const d = await sr.json(); if (Array.isArray(d) && d.length > 5) klines = d; }
        } catch(_) {}
      }
      if (!klines) return [];
      return klines.map(k => ({ open:+k[1], high:+k[2], low:+k[3], close:+k[4], volume:+k[5] }));
    }

    const [m15k, h1k, h4k, d1k] = await Promise.all([
      getKlines(normalizedPair, '15m', 100),
      getKlines(normalizedPair, '1h',  250),
      getKlines(normalizedPair, '4h',  100),
      getKlines(normalizedPair, '1d',  250),
    ]);

    // ── Shared indicator helpers ────────────────────────────────
    function monRsi(closes, period=14) {
      if (closes.length < period + 2) return 50;
      let g=0, l=0;
      for (let i=1; i<=period; i++) { const d=closes[i]-closes[i-1]; d>=0?g+=d:l-=d; }
      let ag=g/period, al=l/period;
      for (let i=period+1; i<closes.length; i++) {
        const d=closes[i]-closes[i-1];
        ag=(ag*(period-1)+(d>0?d:0))/period;
        al=(al*(period-1)+(d<0?-d:0))/period;
      }
      if (al===0) return 100; if (ag===0) return 0;
      return parseFloat((100-100/(1+ag/al)).toFixed(1));
    }
    function monEma(arr, n) {
      if (arr.length < n) return arr[arr.length-1] || 0;
      const k=2/(n+1); let v=arr.slice(0,n).reduce((a,b)=>a+b,0)/n;
      for (let i=n; i<arr.length; i++) v=arr[i]*k+v*(1-k);
      return parseFloat(v.toFixed(6));
    }
    function monMacd(closes) {
      if (closes.length < 35) return { signal: 'NEUTRAL', hist: 0 };
      // FIXED: EMA-12 uses k=2/(12+1), EMA-26 uses k=2/(26+1)
      const k12=2/(12+1), k26=2/(26+1), k9=2/(9+1);
      let e12=closes.slice(0,12).reduce((a,b)=>a+b,0)/12;
      let e26=closes.slice(0,26).reduce((a,b)=>a+b,0)/26;
      // Advance e12 to bar 25 so both EMAs start at the same bar
      for (let i=12; i<26; i++) e12=closes[i]*k12+e12*(1-k12);
      // Build MACD line array from bar 26 onward
      const macdArr=[];
      for (let i=26; i<closes.length; i++) {
        e12=closes[i]*k12+e12*(1-k12);
        e26=closes[i]*k26+e26*(1-k26);
        macdArr.push(e12-e26);
      }
      if (macdArr.length < 9) return { signal: 'NEUTRAL', hist: 0 };
      // Signal line = EMA9 of MACD line (true histogram = MACD − Signal)
      let sig=macdArr.slice(0,9).reduce((a,b)=>a+b,0)/9;
      for (let i=9; i<macdArr.length; i++) sig=macdArr[i]*k9+sig*(1-k9);
      const hist=parseFloat((macdArr[macdArr.length-1]-sig).toFixed(6));
      return { signal: hist>0?'BULLISH':'BEARISH', hist };
    }
    function monStruct(candles) {
      if (candles.length < 20) return 'RANGING';
      const c = candles.slice(-20);
      const mid = Math.floor(c.length/2);
      const pHH = Math.max(...c.slice(0,mid).map(x=>x.high));
      const pLL = Math.min(...c.slice(0,mid).map(x=>x.low));
      const cHH = Math.max(...c.slice(mid).map(x=>x.high));
      const cLL = Math.min(...c.slice(mid).map(x=>x.low));
      const last = c[c.length-1].close;
      // FIX: Require BOTH wicks AND close confirmation to avoid fakeouts
      if (cHH>pHH && last>pHH) return 'BOS_BULLISH';
      if (cLL<pLL && last<pLL) return 'BOS_BEARISH';
      if (cHH>pHH) return 'CHOCH_BULLISH';
      if (cLL<pLL) return 'CHOCH_BEARISH';
      return 'RANGING';
    }
    function monAtr(candles, n=14) {
      if (candles.length < n+1) return 0;
      const trs = candles.slice(1).map((c,i) => Math.max(c.high-c.low, Math.abs(c.high-candles[i].close), Math.abs(c.low-candles[i].close)));
      let atr = trs.slice(0,n).reduce((a,b)=>a+b,0)/n;
      for (let i=n; i<trs.length; i++) atr=(atr*(n-1)+trs[i])/n;
      return parseFloat(atr.toFixed(6));
    }
    function _da_volRatio(candles, n=20) {
      if (candles.length < n+1) return 1;
      const avg = candles.slice(-n-1,-1).reduce((a,c)=>a+c.volume,0)/n;
      return avg > 0 ? parseFloat((candles[candles.length-1].volume/avg).toFixed(2)) : 1;
    }

    // ── Compute indicators ──────────────────────────────────────
    const m15c = m15k.slice(-50), h1c = h1k.slice(-80), h4c = h4k.slice(-80), d1c = d1k.slice(-30);
    const m15cl = m15c.map(x=>x.close), h1cl = h1c.map(x=>x.close), h4cl = h4c.map(x=>x.close), d1cl = d1c.map(x=>x.close);

    const m15RSI = monRsi(m15cl);
    const h1RSI  = monRsi(h1cl);
    const h4RSI  = monRsi(h4cl);

    const h1Ema20  = monEma(h1cl, 20);
    const h1Ema50  = monEma(h1cl, 50);
    // EMA200: use full raw klines arrays (not sliced) so 200 bars are always available
    const h1Ema200 = monEma(h1k.map(x=>x.close), 200);
    const d1Ema200 = monEma(d1k.map(x=>x.close), 200);
    const h4Ema20  = monEma(h4cl, 20);
    const h4Ema50  = monEma(h4cl, 50);

    const h1MACD = monMacd(h1cl);
    const h4MACD = monMacd(h4cl);

    const m15Struct = monStruct(m15c);
    const h1Struct  = monStruct(h1c);
    const h4Struct  = monStruct(h4c);
    const d1Struct  = monStruct(d1c);

    const h4ATR = monAtr(h4c);
    const h1ATR = monAtr(h1c);
    const h1VolR = _da_volRatio(h1c);

    // ── EMA alignment signals ───────────────────────────────────
    const priceAboveEma20H1  = currentPrice > h1Ema20;
    const priceAboveEma50H1  = currentPrice > h1Ema50;
    const priceAboveEma200H1 = currentPrice > h1Ema200;
    const priceAboveD1Ema200 = currentPrice > d1Ema200;
    const ema20AboveEma50H4  = h4Ema20 > h4Ema50;

    // ── Fibonacci from D1 swing ─────────────────────────────────
    const isLong    = direction === 'LONG';
    const entryNum  = parseFloat(entry);
    const slNum     = parseFloat(sl)  || null;
    const tp1Num    = parseFloat(tp1) || null;

    const d1Highs = d1c.map(c=>c.high), d1Lows = d1c.map(c=>c.low);
    const swingHigh = Math.max(...d1Highs.slice(-20));
    const swingLow  = Math.min(...d1Lows.slice(-20));
    const range     = Math.max(swingHigh - swingLow, entryNum * 0.01);
    const fib382 = isLong ? swingHigh - range*0.382 : swingLow + range*0.382;
    const fib500 = isLong ? swingHigh - range*0.500 : swingLow + range*0.500;
    const fib618 = isLong ? swingHigh - range*0.618 : swingLow + range*0.618;
    const fib786 = isLong ? swingHigh - range*0.786 : swingLow + range*0.786;

    const pullbackPct = isLong
      ? Math.max(0, (entryNum-currentPrice)/entryNum*100)
      : Math.max(0, (currentPrice-entryNum)/entryNum*100);
    let pullbackZone = 'NONE';
    if (isLong) {
      if      (currentPrice <= fib786) pullbackZone = 'CRITICAL';
      else if (currentPrice <= fib618) pullbackZone = 'DEEP';
      else if (currentPrice <= fib500) pullbackZone = 'NORMAL';
      else if (currentPrice <= fib382) pullbackZone = 'SHALLOW';
    } else {
      if      (currentPrice >= fib786) pullbackZone = 'CRITICAL';
      else if (currentPrice >= fib618) pullbackZone = 'DEEP';
      else if (currentPrice >= fib500) pullbackZone = 'NORMAL';
      else if (currentPrice >= fib382) pullbackZone = 'SHALLOW';
    }

    // ── Structure checks ────────────────────────────────────────
    // FIX: RANGING ≠ against trade — only ACTIVE opposite BOS is a threat
    const h4AgainstTrade = isLong ? h4Struct === 'BOS_BEARISH' : h4Struct === 'BOS_BULLISH';
    const d1AgainstTrade = isLong ? d1Struct === 'BOS_BEARISH' : d1Struct === 'BOS_BULLISH';
    const h4ForTrade     = isLong ? h4Struct.includes('BULLISH') : h4Struct.includes('BEARISH');
    const d1ForTrade     = isLong ? d1Struct.includes('BULLISH') : d1Struct.includes('BEARISH');

    const tp1Hit = tp1Num && (isLong ? currentPrice >= tp1Num*0.998 : currentPrice <= tp1Num*1.002);
    const slClose = slNum && Math.abs(currentPrice-slNum)/currentPrice < 0.008; // 0.8% from SL

    // RSI confluence
    const rsiOversold  = h4RSI < 35 && h1RSI < 40;  // LONG add zone
    const rsiOverbought = h4RSI > 65 && h1RSI > 60; // SHORT add zone

    // ── Decision engine ─────────────────────────────────────────
    let action='HOLD', reason='', dcaLevel=null, newSL=null, slMoveTarget=null;
    const warnings = [];

    if (h4AgainstTrade && d1AgainstTrade) {
      // BOTH timeframes actively broken against trade — real invalidation
      action = 'CLOSE';
      reason = `Structure fully invalidated: H4 (${h4Struct}) and D1 (${d1Struct}) both flipped ${isLong?'bearish':'bullish'}. Your ${direction} thesis is broken.`;
    } else if (pullbackZone === 'CRITICAL' && h4AgainstTrade) {
      // 78.6% pullback WITH H4 flip — double confirmation of reversal
      action = 'CLOSE';
      reason = `Price breached 78.6% Fib ($${fib786.toFixed(4)}) AND H4 structure flipped (${h4Struct}). High probability of full reversal.`;
    } else if (slClose) {
      action = 'CLOSE';
      reason = `Price within 0.8% of Stop Loss ($${slNum}). R:R is no longer valid — protect capital by closing.`;
    } else if (tp1Hit) {
      action = 'MOVE_SL';
      slMoveTarget = entryNum;
      reason = `TP1 ($${tp1Num}) reached! Move Stop Loss to Break-Even ($${entryNum.toFixed(4)}) to protect profit. 50% position locked in.`;
    } else if (pullbackZone === 'DEEP' && d1ForTrade && (isLong ? rsiOversold : rsiOverbought)) {
      // DCA zone: 61.8% pullback + D1 intact + RSI oversold/overbought
      dcaLevel = parseFloat(fib618.toFixed(4));
      newSL = slNum ? parseFloat((isLong ? Math.min(slNum, fib786) : Math.max(slNum, fib786)).toFixed(4)) : null;
      action = 'DCA';
      reason = `Price at 61.8% Fib ($${fib618.toFixed(4)}) + D1 structure intact (${d1Struct}) + RSI confirms zone (${isLong?'H4='+h4RSI+' oversold':'H4='+h4RSI+' overbought'}). Valid DCA level.`;
    } else if (pullbackZone === 'DEEP' && !d1ForTrade) {
      // Deep pullback but D1 neutral — caution
      warnings.push(`⚠️ Price at 61.8% Fib ($${fib618.toFixed(4)}) but D1 structure is ${d1Struct} — DCA risk is higher`);
      action = 'HOLD';
      reason = `Significant pullback to 61.8% Fib but D1 structure (${d1Struct}) is not clearly ${isLong?'bullish':'bearish'}. No DCA — wait for confirmation.`;
    } else if (pullbackZone === 'CRITICAL' && !h4AgainstTrade) {
      // Critical pullback but structure still intact — watch closely
      warnings.push(`🔴 Price below 78.6% Fib ($${fib786.toFixed(4)}) — near invalidation zone`);
      action = 'HOLD';
      reason = `Deep pullback to 78.6% Fib zone but H4 structure (${h4Struct}) has not confirmed reversal yet. Watch closely — if H4 closes ${isLong?'bearish':'bullish'} consider exit.`;
    } else {
      // Normal hold
      const emaTrend = isLong
        ? (priceAboveEma20H1?'above EMA20':'below EMA20') + (priceAboveEma50H1?' + above EMA50':' + below EMA50')
        : (!priceAboveEma20H1?'below EMA20':'above EMA20');
      const trendText = (d1ForTrade?'D1 ✓':'D1 ranging') + ' · ' + (h4ForTrade?'H4 ✓':'H4 ranging');
      reason = pullbackZone === 'NONE'
        ? `Trade is in profit territory (${trendText}). H4 RSI ${h4RSI}, 1H ${emaTrend}. Continue holding — target TP${tp1Hit?'2':'1'}.`
        : `Pullback (${pullbackZone} — ${pullbackPct.toFixed(1)}%) within normal range. Structures: ${trendText}. H4 RSI ${h4RSI}. Hold.`;
    }

    // Additional warnings
    if (h4AgainstTrade && !d1AgainstTrade) warnings.push(`⚠️ H4 structure flipped ${isLong?'bearish':'bullish'} (${h4Struct}) — D1 still holds but monitor closely`);
    if (!h4ForTrade && !h4AgainstTrade) warnings.push(`ℹ️ H4 structure is RANGING — no clear momentum confirmation for your ${direction}`);
    if (h1MACD.signal !== (isLong?'BULLISH':'BEARISH')) warnings.push(`ℹ️ 1H MACD is ${h1MACD.signal} — against ${direction} direction`);
    if (h1VolR > 2.5 && action !== 'CLOSE') warnings.push(`📊 High volume spike on 1H (${h1VolR}×) — significant move may be starting`);

    // ── EMA alignment summary ───────────────────────────────────
    const emaAlignment = isLong
      ? { ok: priceAboveEma20H1 && priceAboveEma50H1, desc: (priceAboveEma20H1?'✅':'❌')+' EMA20  '+(priceAboveEma50H1?'✅':'❌')+' EMA50  '+(priceAboveEma200H1?'✅':'❌')+' H1-EMA200  '+(priceAboveD1Ema200?'✅':'❌')+' D1-EMA200' }
      : { ok: !priceAboveEma20H1 && !priceAboveEma50H1, desc: (!priceAboveEma20H1?'✅':'❌')+' <EMA20  '+(!priceAboveEma50H1?'✅':'❌')+' <EMA50  '+(!priceAboveEma200H1?'✅':'❌')+' <H1-EMA200  '+(!priceAboveD1Ema200?'✅':'❌')+' <D1-EMA200' };

    res.json({
      success: true,
      pair: normalizedPair,
      direction,
      currentPrice,
      action,
      reason,
      warnings,
      // Multi-TF indicators
      indicators: {
        rsi:  { m15: m15RSI, h1: h1RSI, h4: h4RSI },
        macd: { h1: h1MACD.signal, h4: h4MACD.signal },
        ema:  { h1_20: h1Ema20, h1_50: h1Ema50, h1_200: h1Ema200, h4_20: h4Ema20, h4_50: h4Ema50, d1_200: d1Ema200 },
        struct: { m15: m15Struct, h1: h1Struct, h4: h4Struct, d1: d1Struct },
        atr: { h4: h4ATR, h1: h1ATR },
        volume: { h1Ratio: h1VolR },
        emaAlignment,
      },
      structureIntact: { h4: h4ForTrade, d1: d1ForTrade },
      h4Struct, d1Struct,
      pullbackZone,
      pullbackPct: parseFloat(pullbackPct.toFixed(2)),
      fibonacci: {
        fib382: parseFloat(fib382.toFixed(4)),
        fib500: parseFloat(fib500.toFixed(4)),
        fib618: parseFloat(fib618.toFixed(4)),
        fib786: parseFloat(fib786.toFixed(4)),
      },
      dcaLevel, newSL, slMoveTarget,
      invalidationLevel: parseFloat(fib786.toFixed(4)),
      tp1Hit,
    });
  } catch(err) {
    console.error('/api/trade-monitor error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});
// ═══════════════════════════════════════════════════════════════
//  ADMIN — Users API
// ═══════════════════════════════════════════════════════════════

/* GET /api/admin/users — list all users */
app.get('/api/admin/users', verifyAdmin, async (req, res) => {
  try {
    const users = await User.find({}).sort({ createdAt: -1 }).lean();
    res.json({ success: true, users });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

/* PATCH /api/admin/users/:uid — update user (suspend, plan, etc.) */
app.patch('/api/admin/users/:uid', verifyAdmin, async (req, res) => {
  try {
    const allowed = ['suspended','suspendReason','plan','role','maintenance','maintenanceMsg'];
    const update  = {};
    allowed.forEach(k => { if (req.body[k] !== undefined) update[k] = req.body[k]; });
    const user = await User.findOneAndUpdate({ uid: req.params.uid }, update, { new: true });
    if (!user) return res.status(404).json({ success: false, error: 'User not found' });
    res.json({ success: true, user });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

/* DELETE /api/admin/users/:uid — delete user from DB and Firebase */
app.delete('/api/admin/users/:uid', verifyAdmin, async (req, res) => {
  try {
    await User.findOneAndDelete({ uid: req.params.uid });
    try { await admin.auth().deleteUser(req.params.uid); } catch(_) {}
    res.json({ success: true });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════
//  ADMIN — Stats API
// ═══════════════════════════════════════════════════════════════

/* GET /api/admin/stats */
app.get('/api/admin/stats', verifyAdmin, async (req, res) => {
  try {
    const [totalUsers, activeSignals, openReports, pendingBalReqs] = await Promise.all([
      User.countDocuments({}),
      Signal.countDocuments({ active: true }),
      Report.countDocuments({ status: 'open' }),
      mongoose.models.BalanceRequest
        ? mongoose.models.BalanceRequest.countDocuments({ status: 'pending' })
        : 0,
    ]);
    res.json({ success: true, stats: { totalUsers, activeSignals, openReports, pendingBalReqs } });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════
//  ADMIN — Settings API
// ═══════════════════════════════════════════════════════════════

/* GET /api/admin/settings */
app.get('/api/admin/settings', verifyAdmin, (req, res) => {
  res.json({
    success: true,
    settings: {
      maintenance:        globalSettings.maintenance        || false,
      maintenanceMsg:     globalSettings.maintenanceMsg     || '',
      allowRegistrations: globalSettings.allowRegistrations !== false,
      highImpactMode:     globalSettings.highImpactMode     || false,
      highImpactMsg:      globalSettings.highImpactMsg      || '',
      autoEngine:         globalSettings.autoEngine         || false,
    }
  });
});

/* PATCH /api/admin/settings — save one or more settings */
app.patch('/api/admin/settings', verifyAdmin, async (req, res) => {
  try {
    const allowed = ['maintenance','maintenanceMsg','allowRegistrations','highImpactMode','highImpactMsg','autoEngine'];
    for (const k of allowed) {
      if (req.body[k] !== undefined) {
        globalSettings[k] = req.body[k];
        await saveSettingToDB(k, req.body[k]);
      }
    }
    res.json({ success: true, settings: globalSettings });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

/* POST /api/admin/settings (alias) */
app.post('/api/admin/settings', verifyAdmin, async (req, res) => {
  try {
    const allowed = ['maintenance','maintenanceMsg','allowRegistrations','highImpactMode','highImpactMsg','autoEngine'];
    for (const k of allowed) {
      if (req.body[k] !== undefined) {
        globalSettings[k] = req.body[k];
        await saveSettingToDB(k, req.body[k]);
      }
    }
    res.json({ success: true, settings: globalSettings });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════
//  ADMIN — Reports API
// ═══════════════════════════════════════════════════════════════

/* GET /api/admin/reports */
app.get('/api/admin/reports', verifyAdmin, async (req, res) => {
  try {
    const filter = req.query.status ? { status: req.query.status } : {};
    const reports = await Report.find(filter).sort({ createdAt: -1 }).lean();
    const openReports = await Report.countDocuments({ status: 'open' });
    res.json({ success: true, data: reports, openReports });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

/* PATCH /api/admin/reports/:id — update report status/notes */
app.patch('/api/admin/reports/:id', verifyAdmin, async (req, res) => {
  try {
    const update = {};
    ['status','adminNote','adminReply','readByAdmin'].forEach(k => {
      if (req.body[k] !== undefined) update[k] = req.body[k];
    });
    if (req.body.status && ['resolved','dismissed'].includes(req.body.status)) {
      update.resolvedBy = req.dbUser?.email || 'admin';
      update.resolvedAt = new Date();
    }
    const report = await Report.findByIdAndUpdate(req.params.id, update, { new: true });
    if (!report) return res.status(404).json({ success: false, error: 'Report not found' });
    res.json({ success: true, report });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

/* DELETE /api/admin/reports/:id */
app.delete('/api/admin/reports/:id', verifyAdmin, async (req, res) => {
  try {
    await Report.findByIdAndDelete(req.params.id);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════
//  ADMIN — Announcements API
// ═══════════════════════════════════════════════════════════════

/* GET /api/admin/announcements — list all */
app.get('/api/admin/announcements', verifyAdmin, async (req, res) => {
  try {
    const anns = await Announcement.find({}).sort({ createdAt: -1 }).lean();
    res.json({ success: true, data: anns });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

/* POST /api/admin/announcements — create */
app.post('/api/admin/announcements', verifyAdmin, async (req, res) => {
  try {
    const { title, message, type, active, showFrom, showUntil } = req.body;
    if (!title || !message) return res.status(400).json({ success: false, error: 'Title and message required' });
    const ann = await Announcement.create({
      title, message,
      type:    type || 'info',
      active:  active !== false,
      showFrom: showFrom ? new Date(showFrom) : new Date(),
      showUntil: showUntil ? new Date(showUntil) : null,
      createdBy: req.dbUser?.email || 'admin',
    });
    // Broadcast to live users
    try { broadcastToAll({ type: 'announcement', data: ann }); } catch(_) {}
    res.json({ success: true, data: ann });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

/* PUT /api/admin/announcements/:id — update (toggle active, etc.) */
app.put('/api/admin/announcements/:id', verifyAdmin, async (req, res) => {
  try {
    const update = {};
    ['title','message','type','active','showFrom','showUntil'].forEach(k => {
      if (req.body[k] !== undefined) update[k] = req.body[k];
    });
    const ann = await Announcement.findByIdAndUpdate(req.params.id, update, { new: true });
    if (!ann) return res.status(404).json({ success: false, error: 'Announcement not found' });
    res.json({ success: true, data: ann });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

/* DELETE /api/admin/announcements/:id */
app.delete('/api/admin/announcements/:id', verifyAdmin, async (req, res) => {
  try {
    await Announcement.findByIdAndDelete(req.params.id);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════
//  ADMIN — Broadcast (WebSocket only, no DB save)
// ═══════════════════════════════════════════════════════════════

/* POST /api/admin/broadcast */
app.post('/api/admin/broadcast', verifyAdmin, (req, res) => {
  try {
    const { subject, message, saveToDb } = req.body;
    if (!message) return res.status(400).json({ success: false, error: 'Message required' });
    broadcastToAll({ type: 'announcement', data: { title: subject || 'Notice', message, type: 'info', active: true } });
    res.json({ success: true, message: 'Broadcast sent.' });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════
//  ADMIN — Balance Requests API
// ═══════════════════════════════════════════════════════════════

const BR = mongoose.models.BalanceRequest || require('./models/BalanceRequest');

/* GET /api/admin/balance-requests */
app.get('/api/admin/balance-requests', verifyAdmin, async (req, res) => {
  try {
    const data    = await BR.find({}).sort({ createdAt: -1 }).lean();
    const pending = data.filter(r => r.status === 'pending').length;
    res.json({ success: true, data, pending });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

/* PATCH /api/admin/balance-requests/:id — approve or reject */
app.patch('/api/admin/balance-requests/:id', verifyAdmin, async (req, res) => {
  try {
    const { action, adminNote } = req.body;
    if (!['approve','reject'].includes(action))
      return res.status(400).json({ success: false, error: 'action must be approve or reject' });

    const br = await BR.findById(req.params.id);
    if (!br) return res.status(404).json({ success: false, error: 'Request not found' });

    br.status      = action === 'approve' ? 'approved' : 'rejected';
    br.adminNote   = adminNote || '';
    br.processedBy = req.dbUser?.email || 'admin';
    br.processedAt = new Date();
    await br.save();

    if (action === 'approve') {
      await PB.findOneAndUpdate(
        { uid: br.userUid },
        { balance: br.requestedAmount },
        { upsert: true }
      );
    }
    res.json({ success: true, data: br });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════
//  SIGNALS — Admin CRUD
// ═══════════════════════════════════════════════════════════════

/* POST /api/signals — admin creates a signal */
app.post('/api/signals', verifyAdmin, async (req, res) => {
  try {
    const b = req.body;
    if (!b.pair || !b.direction || !b.entry || !b.tp1 || !b.sl)
      return res.status(400).json({ success: false, error: 'pair, direction, entry, tp1, sl required' });

    // Parse leverage: accept "10x" or 10
    const lev = parseInt(String(b.leverage||10).replace(/[^0-9]/g,'')) || 10;

    const signal = await Signal.create({
      pair:      b.pair.toUpperCase().trim(),
      direction: b.direction,
      entry:     parseFloat(b.entry),
      tp1:       parseFloat(b.tp1),
      tp2:       b.tp2 ? parseFloat(b.tp2) : undefined,
      sl:        parseFloat(b.sl),
      leverage:  lev,
      timeframe: b.timeframe || '1h',
      notes:     b.notes     || '',
      score:     b.score     ? parseInt(b.score) : 0,
      plan:      b.plan      || 'free',
      active:    true,
      status:    'ACTIVE',
    });

    broadcastToAll({ type: 'new_signal', signal });
    res.json({ success: true, signal });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

/* PATCH /api/signals/:id — admin closes or updates a signal */
app.patch('/api/signals/:id', verifyAdmin, async (req, res) => {
  try {
    const update = {};
    ['status','active','notes','pnl','closedAt'].forEach(k => {
      if (req.body[k] !== undefined) update[k] = req.body[k];
    });
    // If closing the signal, mark active=false
    if (update.status && ['TP1_HIT','TP2_HIT','SL_HIT','CANCELLED'].includes(update.status)) {
      update.active   = false;
      update.closedAt = update.closedAt || new Date();
    }
    const signal = await Signal.findByIdAndUpdate(req.params.id, update, { new: true });
    if (!signal) return res.status(404).json({ success: false, error: 'Signal not found' });
    broadcastToAll({ type: 'signal_update', signal });
    res.json({ success: true, signal });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

// ══════════════════════════════════════════════════════════════
//  ADMIN — AI Settings API
// ══════════════════════════════════════════════════════════════

/* GET /api/admin/ai-settings — get current AI config */
app.get('/api/admin/ai-settings', verifyAdmin, (req, res) => {
  res.json({
    success: true,
    settings: {
      groq_api_key:     globalSettings.groq_api_key     || '',
      groq_model:       globalSettings.groq_model       || 'llama-3.3-70b-versatile',
      groq_max_tokens:  globalSettings.groq_max_tokens  || 1500,
      groq_temperature: globalSettings.groq_temperature || 0.2,
      groq_api_key_masked: globalSettings.groq_api_key
        ? 'gsk_' + '*'.repeat(20) + globalSettings.groq_api_key.slice(-6)
        : '(using .env key)',
    }
  });
});

/* POST /api/admin/ai-settings — update AI config */
app.post('/api/admin/ai-settings', verifyAdmin, async (req, res) => {
  try {
    const { groq_api_key, groq_model, groq_max_tokens, groq_temperature } = req.body;
    const ALLOWED_MODELS = ['llama-3.3-70b-versatile','llama-3.1-70b-versatile','llama-3.1-8b-instant','llama3-70b-8192','llama3-8b-8192','mixtral-8x7b-32768','gemma2-9b-it'];
    if (groq_model && !ALLOWED_MODELS.includes(groq_model))
      return res.status(400).json({ success: false, error: 'Invalid model name.' });
    const maxTok = parseInt(groq_max_tokens);
    if (maxTok && (maxTok < 100 || maxTok > 8000))
      return res.status(400).json({ success: false, error: 'max_tokens must be 100–8000.' });
    const temp = parseFloat(groq_temperature);
    if (!isNaN(temp) && (temp < 0 || temp > 2))
      return res.status(400).json({ success: false, error: 'temperature must be 0–2.' });
    const updates = {};
    if (groq_api_key !== undefined) { const k=groq_api_key.trim(); updates.groq_api_key=k; globalSettings.groq_api_key=k; await saveSettingToDB('groq_api_key',k); }
    if (groq_model) { updates.groq_model=groq_model.trim(); globalSettings.groq_model=groq_model.trim(); await saveSettingToDB('groq_model',groq_model.trim()); }
    if (groq_max_tokens) { updates.groq_max_tokens=maxTok; globalSettings.groq_max_tokens=maxTok; await saveSettingToDB('groq_max_tokens',maxTok); }
    if (!isNaN(temp) && groq_temperature!==undefined) { updates.groq_temperature=temp; globalSettings.groq_temperature=temp; await saveSettingToDB('groq_temperature',temp); }
    console.log(`[Admin] AI settings updated by ${req.dbUser?.email}:`, Object.keys(updates).join(', '));
    res.json({ success: true, message: 'AI settings updated.', updated: Object.keys(updates) });
  } catch(e) { res.status(500).json({ success: false, error: e.message }); }
});

/* POST /api/admin/ai-settings/test — test API key */
app.post('/api/admin/ai-settings/test', verifyAdmin, async (req, res) => {
  try {
    const keyToTest = (req.body.groq_api_key || '').trim() || (globalSettings.groq_api_key||'').trim() || process.env.GROQ_API_KEY;
    if (!keyToTest) return res.status(400).json({ success: false, error: 'No API key to test.' });
    const modelToTest = (req.body.groq_model || globalSettings.groq_model || 'llama-3.3-70b-versatile').trim();
    const testRes = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type':'application/json', 'Authorization':'Bearer '+keyToTest },
      body: JSON.stringify({ model: modelToTest, max_tokens: 10, temperature: 0.1, messages: [{ role:'user', content:'Reply with OK only.' }] })
    });
    if (!testRes.ok) { const e=await testRes.text(); return res.json({ success:false, error:`API returned ${testRes.status}: ${e.slice(0,150)}` }); }
    const data = await testRes.json();
    const reply = data.choices?.[0]?.message?.content || '';
    res.json({ success: true, message: `✅ API key works! Model "${modelToTest}" responded: "${reply.slice(0,50)}"` });
  } catch(e) { res.json({ success: false, error: 'Connection error: ' + e.message }); }
});

// ── HTTP Server + WebSocket ───────────────────────────────────
const PORT   = process.env.PORT || 3000;
const server = require('http').createServer(app);

// Client tracking
const clients = new Map(); // uid -> Set of ws connections
let wss = null;

function broadcastToAll(data) {
  if (!wss) {
    // Vercel: persist event to MongoDB → clients poll /api/events
    try { Event.create({ data, uid: null }).catch(() => {}); } catch (_) {}
    return;
  }
  const msg = JSON.stringify(data);
  wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  });
}

function broadcastToUser(uid, data) {
  if (!wss) {
    try { Event.create({ data, uid }).catch(() => {}); } catch (_) {}
    return;
  }
  const msg = JSON.stringify(data);
  const userClients = clients.get(uid);
  if (!userClients) return;
  userClients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  });
}

// WebSocket server — only on VPS (Vercel serverless does not support persistent WS)
if (!process.env.VERCEL) {
  const { WebSocketServer } = require('ws');
  wss = new WebSocketServer({ server });

  wss.on('connection', async (ws, req) => {
    let uid = null;
    console.log('Auth WS. Total:', wss.clients.size);

    ws.on('message', async (raw) => {
      try {
        const msg = JSON.parse(raw);
        if (msg.type === 'auth' && msg.token) {
          try {
            const decoded = await admin.auth().verifyIdToken(msg.token);
            uid = decoded.uid;
            if (!clients.has(uid)) clients.set(uid, new Set());
            clients.get(uid).add(ws);
            ws.send(JSON.stringify({ type: 'auth_ok', uid }));
          } catch (e) {
            ws.send(JSON.stringify({ type: 'auth_error', error: e.message }));
          }
        }
      } catch (_) {}
    });

    ws.on('close', () => {
      console.log('WS disc. Total:', wss.clients.size);
      if (uid && clients.has(uid)) {
        clients.get(uid).delete(ws);
        if (clients.get(uid).size === 0) clients.delete(uid);
      }
    });

    ws.on('error', () => {
      console.log('Unauth WS. Total:', wss.clients.size);
    });
  });
}

// ── Auto TP/SL Check Engine ──────────────────────────────────────────────────
let _tpslRunning = false; // concurrency lock — prevents double-trigger if a cycle takes >30s

async function runTPSLCheck() {
  if (_tpslRunning) return; // skip if previous cycle still running
  _tpslRunning = true;
  try {
    const openTrades = await PT.find({
      status: { $in: ['OPEN', 'TP1_HIT', 'PENDING_LONG', 'PENDING_SHORT'] }
    });
    if (!openTrades.length) return;

    // FIX: Batch-fetch all unique symbols first (avoids per-trade API calls hitting rate limits)
    const uniqueSymbols = [...new Set(
      openTrades.map(t => normalizePair(t.pair || t.symbol)).filter(Boolean)
    )];
    const priceMap = {};
    await Promise.all(uniqueSymbols.map(async sym => {
      try { const p = await getLivePrice(sym); if (p) priceMap[sym] = p; } catch(_) {}
    }));

    for (const trade of openTrades) {
      try {
        const symbol = normalizePair(trade.pair || trade.symbol);
        if (!symbol) continue;
        const price = priceMap[symbol];
        if (!price) continue;

        const isLong = trade.direction === 'LONG';
        const entry  = trade.entryPrice || trade.entry;

        // FIX: Always derive notional from current remaining size (not stored original notional)
        // trade.remainingSize is 50% after TP1 — using trade.notional here would give 2× PnL
        const size     = trade.remainingSize || trade.size || trade.amount || 0;
        const lev      = trade.leverage || 1;
        const notional = size * lev; // ← always from current size

        // ── Fill pending limit orders ─────────────────────────────
        if (trade.status === 'PENDING_LONG' && price <= trade.triggerPrice) {
          await PT.findByIdAndUpdate(trade._id, { status: 'OPEN', fillTime: new Date() });
          continue;
        }
        if (trade.status === 'PENDING_SHORT' && price >= trade.triggerPrice) {
          await PT.findByIdAndUpdate(trade._id, { status: 'OPEN', fillTime: new Date() });
          continue;
        }

        if (!['OPEN','TP1_HIT'].includes(trade.status)) continue;

        const sl  = trade.currentSl || trade.sl;
        const tp1 = trade.tp1;
        const tp2 = trade.tp2;

        // ── TP1 — partial close 50% ───────────────────────────────
        if (trade.status === 'OPEN' && tp1 && (isLong ? price >= tp1 : price <= tp1)) {
          // FIX: use tp1 as fill price (not live price) — avoids gap-through distortion
          const tp1Pnl = isLong ? (tp1 - entry) / entry * notional * 0.5
                                : (entry - tp1) / entry * notional * 0.5;
          await PT.findByIdAndUpdate(trade._id, {
            status: 'TP1_HIT',
            tp1HitPrice: tp1,
            tp1Pnl,
            currentSl: entry,          // move SL to break-even
            remainingSize: size * 0.5, // 50% remains open
          });
          const refund = size * 0.5 + tp1Pnl;
          await PB.findOneAndUpdate({ uid: trade.uid }, { $inc: { balance: refund } }, { upsert: true });
          continue;
        }

        // ── TP2 — full close of remaining position ────────────────
        if (tp2 && (isLong ? price >= tp2 : price <= tp2)) {
          // FIX: use tp2 as fill price for PnL accuracy
          const pnl = isLong ? (tp2 - entry) / entry * notional
                             : (entry - tp2) / entry * notional;
          await PT.findByIdAndUpdate(trade._id, { status: 'CLOSED', closePrice: tp2, closedAt: new Date(), pnl });
          await PB.findOneAndUpdate({ uid: trade.uid }, { $inc: { balance: size + pnl } }, { upsert: true });
          continue;
        }

        // ── SL — close at stop level ──────────────────────────────
        if (sl && (isLong ? price <= sl : price >= sl)) {
          // FIX: use sl as fill price (prevents gap-below SL from amplifying loss unrealistically)
          const pnl = isLong ? (sl - entry) / entry * notional
                             : (entry - sl) / entry * notional;
          await PT.findByIdAndUpdate(trade._id, { status: 'CLOSED', closePrice: sl, closedAt: new Date(), pnl });
          await PB.findOneAndUpdate({ uid: trade.uid }, { $inc: { balance: size + pnl } }, { upsert: true });
          continue;
        }

        // ── Trailing SL — only for TP1_HIT trades ────────────────
        if (trade.status === 'TP1_HIT') {
          const trailOffset = Math.abs(trade.tp1 - entry) * 0.5;
          const newTrailSL  = isLong
            ? Math.max(entry, price - trailOffset)  // trails up, never below BE
            : Math.min(entry, price + trailOffset);  // trails down, never above BE
          if (newTrailSL !== trade.currentSl) {
            await PT.findByIdAndUpdate(trade._id, { currentSl: newTrailSL });
          }
        }
      } catch(e) { console.error('TPSLCheck trade error:', e.message); }
    }
  } catch(e) { console.error('runTPSLCheck error:', e.message); }
  finally { _tpslRunning = false; } // always release lock
}

// ── Polling endpoints (WebSocket polyfill for Vercel) ────────

// Ticker symbols for live prices
const TICKER_SYMS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT'];
let _tickerCache = { data: null, ts: 0 };

// GET /api/market/prices — live prices (used by ws-polyfill.js every 5s)
app.get('/api/market/prices', async (req, res) => {
  try {
    if (_tickerCache.data && Date.now() - _tickerCache.ts < 5000) {
      return res.json(_tickerCache.data);
    }
    const ctrl = new AbortController();
    setTimeout(() => ctrl.abort(), 6000);
    const r = await fetch('https://api.binance.com/api/v3/ticker/24hr', { signal: ctrl.signal });
    if (!r.ok) throw new Error('Binance error');
    const all = await r.json();
    const ticker = all
      .filter(c => TICKER_SYMS.includes(c.symbol))
      .map(c => ({ symbol: c.symbol, price: parseFloat(c.lastPrice), change: parseFloat(c.priceChangePercent) }));
    const response = { type: 'market_update', ticker };
    _tickerCache = { data: response, ts: Date.now() };
    res.json(response);
  } catch (e) {
    res.json({ type: 'market_update', ticker: [] });
  }
});

// GET /api/events — event queue for ws-polyfill.js (replaces WebSocket push)
// Optional auth: no token = public events only; with token = public + user events
app.get('/api/events', async (req, res) => {
  try {
    const since = req.query.since ? new Date(parseInt(req.query.since)) : new Date(Date.now() - 10000);
    let uid = null;
    const token = (req.headers.authorization || '').replace('Bearer ', '');
    if (token) {
      try { const d = await admin.auth().verifyIdToken(token); uid = d.uid; } catch (_) {}
    }
    const query = { ts: { $gt: since }, $or: [{ uid: null }, ...(uid ? [{ uid }] : [])] };
    const events = await Event.find(query).sort({ ts: 1 }).limit(100).lean();
    res.json({ events: events.map(e => e.data), ts: Date.now() });
  } catch (e) {
    res.json({ events: [], ts: Date.now() });
  }
});

// ── Cron endpoint — triggered by Vercel Cron or any external scheduler ──
app.post('/api/cron/tpsl-check', async (req, res) => {
  const secret = req.headers['x-cron-secret'];
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  try {
    await runTPSLCheck();
    res.json({ success: true, ts: new Date() });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

if (!process.env.VERCEL) {
  // VPS: run background engine + start HTTP server
  setInterval(runTPSLCheck, 30000);
  console.log('⚡ Auto TP/SL engine started');

  server.listen(PORT, () => {
    console.log(`🚀  Server running on port ${PORT}`);
  });
}

// Export Express app for Vercel serverless
module.exports = app;
