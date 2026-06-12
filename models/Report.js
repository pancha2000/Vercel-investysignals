const mongoose = require('mongoose');

const ReportSchema = new mongoose.Schema({
  reporterUid:   { type: String, default: 'anonymous' },
  reporterEmail: { type: String, default: '' },
  category:      { type: String, enum: ['signal_accuracy','technical_bug','inappropriate_content','other'], required: true },
  message:       { type: String, required: true, maxlength: 2000 },
  context:       { type: String, default: '' },
  status:        { type: String, enum: ['open','in_review','resolved','dismissed'], default: 'open' },
  adminNote:     { type: String, default: '' },
  adminReply:    { type: String, default: '' },
  resolvedBy:    { type: String, default: '' },
  resolvedAt:    { type: Date },
  readByAdmin:   { type: Boolean, default: false },
}, { timestamps: true });

module.exports = mongoose.model('Report', ReportSchema);
