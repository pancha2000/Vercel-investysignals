const mongoose = require('mongoose');

const PaperTradeSchema = new mongoose.Schema({
  userUid:      { type: String, required: true, index: true },
  signalId:     { type: mongoose.Schema.Types.ObjectId, ref: 'Signal', default: null },
  pair:         { type: String, required: true },
  direction:    { type: String, enum: ['LONG','SHORT'], required: true },
  orderType:    { type: String, enum: ['MARKET','LIMIT'], default: 'MARKET' },
  entry:        { type: Number, required: true },   // actual fill price
  triggerPrice: { type: Number },                   // limit order trigger
  tp1:          { type: Number },
  tp2:          { type: Number },
  tp3:          { type: Number },
  sl:           { type: Number },
  leverage:     { type: Number, default: 10 },
  size:         { type: Number, default: 100 },     // USDT margin amount
  status:       { type: String, enum: ['PENDING','OPEN','TP1_HIT','TP2_HIT','TP3_HIT','SL_HIT','CLOSED','CANCELLED'], default: 'OPEN' },
  // Partial close tracking
  tp1Closed:    { type: Boolean, default: false },
  tp2Closed:    { type: Boolean, default: false },
  remainingSize:{ type: Number },                   // after partial closes
  // P&L
  pnl:          { type: Number, default: 0 },
  pnlPct:       { type: Number, default: 0 },
  // Timestamps
  openedAt:     { type: Date, default: Date.now },
  filledAt:     { type: Date },                     // when LIMIT order filled
  closedAt:     { type: Date },
  closePrice:   { type: Number },
  notes:        { type: String, default: '' },
  // Auto-check tracking
  lastChecked:  { type: Date, default: Date.now },
}, { timestamps: true });

module.exports = mongoose.model('PaperTrade', PaperTradeSchema);
