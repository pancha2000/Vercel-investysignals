'use strict';
const mongoose = require('mongoose');

// Events broadcast to clients (replaces WebSocket push on Vercel)
// Auto-deleted after 5 minutes via TTL index
const EventSchema = new mongoose.Schema({
  data: { type: mongoose.Schema.Types.Mixed, required: true },
  uid:  { type: String, default: null }, // null = all users
  ts:   { type: Date,   default: Date.now }
});

EventSchema.index({ ts: 1 }, { expireAfterSeconds: 300 });

module.exports = mongoose.models.Event || mongoose.model('Event', EventSchema);
