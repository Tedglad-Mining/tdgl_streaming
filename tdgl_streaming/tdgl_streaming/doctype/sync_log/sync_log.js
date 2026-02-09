// Copyright (c) 2026, Percival Rapha and contributors
// For license information, please see license.txt

frappe.ui.form.on('Sync Log', {
	refresh: function(frm) {
		if (frm.doc.ref_doctype && frm.doc.docname) {
			frm.add_custom_button(__(frm.doc.docname), function() {
				frappe.set_route('Form', frm.doc.ref_doctype, frm.doc.docname);
			});
			frm.add_custom_button(__(frm.doc.ref_doctype + ' List'), function() {
				frappe.set_route('List', frm.doc.ref_doctype);
			});
		}
	}
});
