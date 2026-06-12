const mongoose = require('mongoose');

const AnnouncementSchema = new mongoose.Schema({
  title:     { type: String, required: true },
  message:   { type: String, required: true },
  type:      { type: String, enum: ['info','warning','success','danger'], default: 'info' },
  active:    { type: Boolean, default: true },
  showFrom:  { type: Date, default: Date.now },
  showUntil: { type: Date },
  audience:  { type: String, default: 'All Users' },
  createdBy: { type: String, default: 'admin' },
}, { timestamps: true });

module.exports = mongoose.model('Announcement', AnnouncementSchema);
