const mongoose = require('mongoose');

// Global platform settings — persisted in MongoDB so they survive server restarts
const SettingsSchema = new mongoose.Schema({
  key:   { type: String, required: true, unique: true },
  value: { type: mongoose.Schema.Types.Mixed, default: null }
});

module.exports = mongoose.model('Settings', SettingsSchema);
