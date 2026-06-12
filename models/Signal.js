const mongoose = require('mongoose');

const SignalSchema = new mongoose.Schema({
  pair:       { type: String, required: true, uppercase: true },
  direction:  { type: String, enum: ['LONG', 'SHORT'], required: true },
  entry:      { type: Number, required: true },
  tp1:        { type: Number, required: true },
  tp2:        { type: Number },
  sl:         { type: Number, required: true },
  leverage:   { type: Number, default: 10 },  // FIX: Number — server parses "10x" strings before saving
  timeframe:  { type: String, default: '1h' },
  notes:      { type: String, default: '' },
  score:      { type: Number, default: 0 },   // FIX: added score field (was missing, admin form sends this)
  plan:       { type: String, default: 'free' }, // signal visibility by plan level
  status:     { type: String, enum: ['ACTIVE','TP1_HIT','TP2_HIT','SL_HIT','CANCELLED'], default: 'ACTIVE' },
  pnl:        { type: Number, default: 0 },
  winRate:    { type: Number },
  active:     { type: Boolean, default: true },
  createdAt:  { type: Date, default: Date.now },
  closedAt:   { type: Date }
});

module.exports = mongoose.model('Signal', SignalSchema);
