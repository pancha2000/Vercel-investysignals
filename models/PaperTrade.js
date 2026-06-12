const mongoose = require('mongoose');

const PaperTradeSchema = new mongoose.Schema({
  userUid:       { type: String, required: true, index: true },
  signalId:      { type: mongoose.Schema.Types.ObjectId, ref: 'Signal' },
  pair:          { type: String, required: true },
  direction:     { type: String, enum: ['LONG','SHORT'], required: true },
  orderType:     { type: String, enum: ['MARKET','LIMIT'], default: 'MARKET' },
  entry:         { type: Number, required: true },
  triggerPrice:  { type: Number },
  tp1:           { type: Number },
  tp2:           { type: Number },
  tp3:           { type: Number },
  sl:            { type: Number },
  leverage:      { type: Number, default: 10 },
  size:          { type: Number, default: 100 },   // USDT amount
  remainingSize: { type: Number },
  // FIX BUG 1: Added 'PENDING' to enum so LIMIT orders don't fail validation
  status: {
    type: String,
    enum: ['PENDING','PENDING_LONG','PENDING_SHORT','OPEN','TP1_HIT','TP2_HIT','TP3_HIT','SL_HIT','CLOSED','CANCELLED'],
    default: 'OPEN'
},
  pnl:       { type: Number, default: 0 },
  pnlPct:    { type: Number, default: 0 },
  openedAt:  { type: Date, default: Date.now },
  filledAt:  { type: Date },
  closedAt:  { type: Date },
  closePrice:{ type: Number },
  beActive:  { type: Boolean, default: false },   // Break-even SL active
  notes:     { type: String, default: '' }
});

module.exports = mongoose.model('PaperTrade', PaperTradeSchema);
