// Copyright (c) 2026, Percival Rapha and contributors
// For license information, please see license.txt

frappe.ui.form.on('Sync Source', {
	refresh: function(frm) {
		if (frm.is_new()) return;

		frm.add_custom_button(__('Pull Now'), function() {
			frappe.call({
				method: 'tdgl_streaming.sync.pull_now',
				args: { sync_source_name: frm.doc.name },
				callback: function() {
					frappe.show_alert({ message: __('Pull enqueued'), indicator: 'green' });
				}
			});
		}, __('Sync'));

		frm.add_custom_button(__('Retry Failed'), function() {
			frappe.call({
				method: 'tdgl_streaming.sync.retry_failed',
				args: { sync_source_name: frm.doc.name },
				freeze: true,
				freeze_message: __('Retrying failed entries...'),
				callback: function(r) {
					if (r.message) {
						frappe.msgprint(
							__('Retried {0}: {1} succeeded, {2} failed',
								[r.message.retried, r.message.succeeded, r.message.failed])
						);
					}
				}
			});
		}, __('Sync'));

		frm.add_custom_button(__('Reset Cursor'), function() {
			frappe.confirm(
				__('This will clear the sync cursor and re-sync ALL documents from scratch. Continue?'),
				function() {
					frappe.call({
						method: 'tdgl_streaming.sync.reset_and_pull',
						args: { sync_source_name: frm.doc.name },
						callback: function() {
							frappe.show_alert({ message: __('Cursor reset, full re-sync enqueued'), indicator: 'green' });
							frm.reload_doc();
						}
					});
				}
			);
		}, __('Sync'));
	}
});
