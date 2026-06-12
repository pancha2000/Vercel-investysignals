const mongoose = require('mongoose');

const BalanceRequestSchema = new mongoose.Schema({
  userUid:      { type: String, required: true, index: true },
  userEmail:    { type: String, default: '' },
  displayName:  { type: String, default: '' },
  requestType:  { type: String, enum: ['RESET','TOPUP','CUSTOM'], required: true },
  requestedAmount: { type: Number, required: true }, // amount requested
  currentBalance:  { type: Number, default: 0 },
  reason:       { type: String, default: '' },
  status:       { type: String, enum: ['pending','approved','rejected'], default: 'pending' },
  adminNote:    { type: String, default: '' },
  processedBy:  { type: String, default: '' },
  processedAt:  { type: Date },
}, { timestamps: true });

module.exports = mongoose.model('BalanceRequest', BalanceRequestSchema);
