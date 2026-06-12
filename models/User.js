const mongoose = require('mongoose');

const UserSchema = new mongoose.Schema({
  uid:           { type: String, required: true, unique: true },  // Firebase UID
  email:         { type: String },
  displayName:   { type: String },
  // FIX: role should only be 'user' or 'admin' — 'pro'/'elite' are plan values, not roles
  role:          { type: String, enum: ['user', 'admin'], default: 'user' },
  plan:          { type: String, enum: ['free', 'pro', 'elite'], default: 'free' },
  suspended:     { type: Boolean, default: false },
  suspendReason: { type: String, default: '' },
  maintenance:   { type: Boolean, default: false },   // per-user maintenance override
  maintenanceMsg:{ type: String, default: '' },
  paperBalance:  { type: Number, default: 1000 },
  hasSetInitialBalance: { type: Boolean, default: false }, // first-time self-set allowed once
  createdAt:     { type: Date, default: Date.now },
  lastLogin:     { type: Date }
});

module.exports = mongoose.model('User', UserSchema);
