import frappe
import json
import requests
from frappe.utils import now_datetime

# DocTypes that should never be logged/tracked (internal + Frappe noise)
INTERNAL_DOCTYPES = {
	# Sync system doctypes
	"Sync Config",
	"Sync Config Doctypes",
	"Sync Change Log",
	"Sync Source",
	"Sync Source DocType",
	"Sync Log",
	"Sync Conflict",
	"Sync Local Edit",
	# Frappe internals
	"Communication",
	"Email Queue",
	"Email Queue Recipient",
	"Activity Log",
	"Version",
	"Comment",
	"Session Default",
	"Error Log",
	"Scheduled Job Log",
	"Scheduled Job Type",
	"Access Log",
	"Route History",
	"View Log",
	"Energy Point Log",
	"Notification Log",
	"Document Follow",
	"User Permission",
	"Has Role",
	"DocShare",
	"Webhook Request Log",
	"Prepared Report",
}

# Fields to strip from snapshots before applying on replica
STRIP_FIELDS = {
	"name", "owner", "creation", "modified", "modified_by",
	"_user_tags", "_liked_by", "_comments", "_assign",
	"__islocal", "__unsaved", "__run_link_triggers",
}

STRIP_CHILD_FIELDS = {"parent", "parentfield", "parenttype", "creation", "modified", "modified_by", "owner"}


class SyncConflictDetected(Exception):
	"""Raised when a conflict is detected during apply. Not an error — signals skip."""
	pass


# ============================================================
# PRIMARY SIDE — Change logging
# ============================================================

def _get_sync_config():
	"""Get Sync Config singleton, cached per request."""
	if not hasattr(frappe.local, "_sync_config"):
		try:
			frappe.local._sync_config = frappe.get_cached_doc("Sync Config")
		except frappe.DoesNotExistError:
			frappe.local._sync_config = None
	return frappe.local._sync_config


def _is_doctype_tracked(doctype):
	"""Check if doctype is in Sync Config's tracked list (primary side)."""
	config = _get_sync_config()
	if not config or not config.enabled:
		return False
	for row in config.sync_doctypes:
		if row.ref_doctype == doctype and row.enabled:
			return True
	return False


EVENT_MAP = {
	"after_insert": "Create",
	"on_update": "Update",
	"on_submit": "Submit",
	"on_cancel": "Cancel",
	"on_trash": "Delete",
	"on_update_after_submit": "Update After Submit",
}


def log_change(doc, method):
	"""doc_events handler — log change on primary side.

	Guards:
	- Skip if currently applying replica sync
	- Skip internal doctypes
	- Skip if Sync Config disabled or doctype not tracked
	- Dedup: skip on_update when on_submit fires (both fire on submit)
	"""
	if frappe.flags.get("in_replica_sync"):
		return

	if doc.doctype in INTERNAL_DOCTYPES:
		return

	if not _is_doctype_tracked(doc.doctype):
		return

	update_type = EVENT_MAP.get(method)
	if not update_type:
		return

	# Dedup: on_update fires during insert and submit.
	# Skip on_update in those cases to avoid double-logging.
	if method == "on_update":
		if doc.flags.get("in_insert"):
			return  # after_insert will handle this
		if getattr(doc, "_action", None) == "submit":
			return  # on_submit will handle this

	data = None
	if update_type != "Delete":
		data = frappe.as_json(doc.as_dict())

	frappe.get_doc({
		"doctype": "Sync Change Log",
		"ref_doctype": doc.doctype,
		"docname": doc.name,
		"update_type": update_type,
		"data": data,
		"source_site": frappe.local.site,
	}).insert(ignore_permissions=True)

	_push_notify()


def log_rename(doc, method, old_name, new_name, merge):
	"""after_rename handler — different signature from log_change."""
	if frappe.flags.get("in_replica_sync"):
		return

	if doc.doctype in INTERNAL_DOCTYPES:
		return

	if not _is_doctype_tracked(doc.doctype):
		return

	frappe.get_doc({
		"doctype": "Sync Change Log",
		"ref_doctype": doc.doctype,
		"docname": new_name,
		"update_type": "Rename",
		"data": frappe.as_json({"old_name": old_name, "new_name": new_name}),
		"source_site": frappe.local.site,
	}).insert(ignore_permissions=True)

	_push_notify()


def _push_notify():
	"""Fire-and-forget push notification to replica."""
	config = _get_sync_config()
	if not config or not config.push_enabled or not config.push_url:
		return

	frappe.enqueue(
		"tdgl_streaming.sync._send_push",
		url=config.push_url,
		queue="short",
		enqueue_after_commit=True,
		job_name="sync_push_notify",
	)


def _send_push(url):
	"""Background job: POST to replica's notify endpoint."""
	try:
		requests.post(
			f"{url}/api/method/tdgl_streaming.api.notify",
			timeout=5,
		)
	except Exception:
		pass  # fire and forget


# ============================================================
# REPLICA SIDE — Pull & Apply
# ============================================================

def _get_replica_synced_doctypes():
	"""Get set of doctypes tracked by any enabled Sync Source, cached per request."""
	if not hasattr(frappe.local, "_replica_synced_doctypes"):
		synced = set()
		sources = frappe.get_all("Sync Source", filters={"enabled": 1}, pluck="name")
		for name in sources:
			rows = frappe.get_all(
				"Sync Source DocType",
				filters={"parent": name, "enabled": 1},
				fields=["ref_doctype"],
			)
			for row in rows:
				synced.add(row.ref_doctype)
		frappe.local._replica_synced_doctypes = synced
	return frappe.local._replica_synced_doctypes


def track_local_edit(doc, method):
	"""Track local edits on replica for conflict detection.

	Fires on on_update for all docs. Only records if:
	- Not a sync write
	- Doctype is tracked by a Sync Source
	- Not an internal doctype
	- Not already tracked
	"""
	if frappe.flags.get("in_replica_sync"):
		return

	if doc.doctype in INTERNAL_DOCTYPES:
		return

	synced = _get_replica_synced_doctypes()
	if doc.doctype not in synced:
		return

	# Dedup: check if already tracked
	exists = frappe.db.exists("Sync Local Edit", {
		"ref_doctype": doc.doctype,
		"docname": doc.name,
	})
	if exists:
		return

	frappe.get_doc({
		"doctype": "Sync Local Edit",
		"ref_doctype": doc.doctype,
		"docname": doc.name,
	}).insert(ignore_permissions=True)


def scheduled_pull():
	"""Scheduler entry point — enqueue pull for each enabled Sync Source."""
	sources = frappe.get_all("Sync Source", filters={"enabled": 1}, pluck="name")
	for source_name in sources:
		frappe.enqueue(
			"tdgl_streaming.sync.pull_changes",
			sync_source_name=source_name,
			queue="default",
			job_name=f"sync_pull_{source_name}",
			enqueue_after_commit=True,
			timeout=1500,
		)


def pull_changes(sync_source_name):
	"""Pull changes from primary for a given Sync Source."""
	source = frappe.get_doc("Sync Source", sync_source_name)
	if not source.enabled:
		return

	from frappe.frappeclient import FrappeClient
	client = FrappeClient(
		url=source.primary_url,
		api_key=source.api_key,
		api_secret=source.get_password("api_secret"),
	)

	doctypes = [row.ref_doctype for row in source.sync_doctypes if row.enabled]
	if not doctypes:
		return

	# Initial sync: if last_pulled is empty, pull all existing docs
	if not source.last_pulled:
		_initial_sync(client, source, doctypes)
		return

	# Incremental pull
	response = client.post_api(
		"tdgl_streaming.api.get_changes",
		{
			"last_pulled": str(source.last_pulled),
			"last_pulled_name": source.last_pulled_name or "",
			"doctypes": frappe.as_json(doctypes),
			"limit": source.pull_batch_size or 100,
		},
	)

	changes = response if isinstance(response, list) else response.get("message", [])

	for change in changes:
		_apply_single_change(change, source)

	# Update cursor
	if changes:
		last = changes[-1]
		source.db_set("last_pulled", last.get("creation"))
		source.db_set("last_pulled_name", last.get("name"))
		frappe.db.commit()


def _initial_sync(client, source, doctypes):
	"""Pull all existing docs for each tracked doctype (first-time sync)."""
	for dt in doctypes:
		filters = _get_doctype_filters(source, dt)

		offset = 0
		batch_size = source.pull_batch_size or 100
		while True:
			docs = client.get_list(
				dt,
				filters=filters,
				fields=["name"],
				limit_start=offset,
				limit_page_length=batch_size,
			)
			if not docs:
				break

			for d in docs:
				try:
					full_doc = client.get_doc(dt, d.get("name"))
					_apply_doc_snapshot(full_doc, source, "Create")
				except Exception:
					frappe.log_error(
						f"Initial sync failed: {dt} {d.get('name')}",
						"TDGL Streaming",
					)

			if len(docs) < batch_size:
				break
			offset += batch_size

	# Set cursor to now
	source.db_set("last_pulled", now_datetime())
	source.db_set("last_pulled_name", "")
	frappe.db.commit()


def _get_doctype_filters(source, doctype):
	"""Get replica-side filters for a specific doctype from Sync Source config."""
	for row in source.sync_doctypes:
		if row.ref_doctype == doctype and row.enabled:
			if row.filters:
				return frappe.parse_json(row.filters)
	return {}


def _get_doctype_config(source, doctype):
	"""Get the Sync Source DocType row for a given doctype."""
	for row in source.sync_doctypes:
		if row.ref_doctype == doctype and row.enabled:
			return row
	return None


def _apply_single_change(change, source):
	"""Apply a single change from the primary, with error handling."""
	dt = change.get("ref_doctype")
	dn = change.get("docname")
	update_type = change.get("update_type")

	# Check if doctype matches replica config
	dt_config = _get_doctype_config(source, dt)
	if not dt_config:
		return

	# Check replica-side filters
	if dt_config.filters and change.get("data") and update_type != "Delete":
		filters = frappe.parse_json(dt_config.filters)
		data = frappe.parse_json(change["data"]) if isinstance(change["data"], str) else change["data"]
		if not _matches_filters(data, filters):
			return

	try:
		frappe.flags.in_replica_sync = True

		if update_type == "Create":
			_apply_create(change, source)
		elif update_type in ("Update", "Update After Submit"):
			_apply_update(change, source)
		elif update_type == "Submit":
			_apply_submit(change, source)
		elif update_type == "Cancel":
			_apply_cancel(change, source)
		elif update_type == "Delete":
			_apply_delete(change, source)
		elif update_type == "Rename":
			_apply_rename(change, source)
		else:
			return

		_create_sync_log(change, source, "Synced")

	except SyncConflictDetected:
		# Conflict already logged by _create_conflict — don't log "Synced"
		pass

	except Exception:
		frappe.db.rollback()
		_create_sync_log(change, source, "Failed", error=frappe.get_traceback())
		frappe.log_error(
			f"Sync failed: {dt} {dn} ({update_type})",
			"TDGL Streaming",
		)
		frappe.db.commit()

	finally:
		frappe.flags.in_replica_sync = False


def _apply_create(change, source):
	"""Apply a Create change. Falls through to Update if doc already exists."""
	data = _parse_data(change)
	dt = data.get("doctype")
	dn = change.get("docname")

	if frappe.db.exists(dt, dn):
		if _has_local_edit(dt, dn):
			_create_conflict(change, source, "Update")
			return
		# Already exists, treat as update
		_apply_update(change, source)
		return

	cleaned = _clean_snapshot(data)
	amended_from = cleaned.pop("amended_from", None)

	doc = frappe.get_doc(cleaned)
	doc.flags.ignore_links = True
	doc.flags.ignore_validate = True
	doc.flags.ignore_permissions = True
	doc.flags.ignore_mandatory = True
	doc.flags.ignore_version = True
	doc.insert(set_name=dn, set_child_names=False)

	if amended_from:
		doc.db_set("amended_from", amended_from, update_modified=False)

	_restore_attribution(doc, data)
	_sync_attachments(doc, source)
	frappe.db.commit()


def _apply_update(change, source):
	"""Apply an Update change. Falls through to Create if doc doesn't exist."""
	data = _parse_data(change)
	dt = data.get("doctype")
	dn = change.get("docname")

	if not frappe.db.exists(dt, dn):
		_apply_create(change, source)
		return

	if _has_local_edit(dt, dn):
		_create_conflict(change, source, "Update")
		return

	doc = frappe.get_doc(dt, dn)
	if doc.docstatus == 2:
		return  # Can't update cancelled docs, skip silently

	if data.get("modified") == str(doc.modified):
		return  # Doc unchanged since last sync, skip

	cleaned = _clean_snapshot(data)
	doc.update(cleaned)
	doc.flags.ignore_links = True
	doc.flags.ignore_validate = True
	doc.flags.ignore_permissions = True
	doc.flags.ignore_mandatory = True
	doc.flags.ignore_validate_update_after_submit = True
	doc.flags.ignore_version = True
	doc.save()

	_restore_attribution(doc, data)
	_sync_attachments(doc, source)
	frappe.db.commit()


def _apply_submit(change, source):
	"""Apply a Submit change."""
	data = _parse_data(change)
	dt = data.get("doctype")
	dn = change.get("docname")

	if not frappe.db.exists(dt, dn):
		# Create first, then submit
		_apply_create(change, source)
		# Re-check if it was created
		if not frappe.db.exists(dt, dn):
			return

	doc = frappe.get_doc(dt, dn)

	if doc.docstatus == 0:
		# Draft -> Submit
		cleaned = _clean_snapshot(data)
		cleaned.pop("docstatus", None)
		doc.update(cleaned)
		doc.flags.ignore_links = True
		doc.flags.ignore_validate = True
		doc.flags.ignore_permissions = True
		doc.flags.ignore_mandatory = True
		doc.flags.ignore_validate_update_after_submit = True
		doc.flags.ignore_version = True
		doc.submit()
		_restore_attribution(doc, data)
	elif doc.docstatus == 1:
		# Already submitted, treat as update after submit
		_apply_update(change, source)
		return
	elif doc.docstatus == 2:
		# Cancelled on replica
		if _has_local_edit(dt, dn):
			_create_conflict(change, source, "Submit")
		return

	frappe.db.commit()


def _apply_cancel(change, source):
	"""Apply a Cancel change."""
	dt = change.get("ref_doctype")
	dn = change.get("docname")

	if not frappe.db.exists(dt, dn):
		return

	if _has_local_edit(dt, dn):
		_create_conflict(change, source, "Cancel")
		return

	doc = frappe.get_doc(dt, dn)
	doc.flags.ignore_links = True
	doc.flags.ignore_permissions = True
	doc.flags.ignore_version = True
	doc.cancel()
	frappe.db.commit()


def _apply_delete(change, source):
	"""Apply a Delete change."""
	dt = change.get("ref_doctype")
	dn = change.get("docname")

	if not frappe.db.exists(dt, dn):
		return

	frappe.delete_doc(dt, dn, force=True, ignore_permissions=True)
	# Clean up any local edit tracking
	_remove_local_edit(dt, dn)
	frappe.db.commit()


def _apply_rename(change, source):
	"""Apply a Rename change."""
	dt = change.get("ref_doctype")
	data = _parse_data(change)
	old_name = data.get("old_name")
	new_name = data.get("new_name")

	if not old_name or not new_name:
		return

	if not frappe.db.exists(dt, old_name):
		return

	frappe.rename_doc(dt, old_name, new_name, force=True)
	frappe.db.commit()


# ============================================================
# REPLICA SIDE — Helpers
# ============================================================

def _parse_data(change):
	"""Parse the data field from a change dict."""
	data = change.get("data")
	if isinstance(data, str):
		return json.loads(data)
	return data or {}


def _clean_snapshot(data):
	"""Strip fields that cause issues on insert/update.

	Removes system fields from parent and child rows.
	Preserves docstatus for submit handling.
	"""
	cleaned = {}
	meta = frappe.get_meta(data.get("doctype"))
	table_fields = {df.fieldname for df in meta.get_table_fields()}

	for key, value in data.items():
		if key in STRIP_FIELDS:
			continue

		# Handle child tables
		if key in table_fields and isinstance(value, list):
			cleaned_children = []
			for child in value:
				cleaned_child = {
					k: v for k, v in child.items()
					if k not in STRIP_CHILD_FIELDS
				}
				cleaned_children.append(cleaned_child)
			cleaned[key] = cleaned_children
		else:
			cleaned[key] = value

	return cleaned


def _has_local_edit(doctype, docname):
	"""Check if a document has been locally edited on the replica."""
	return frappe.db.exists("Sync Local Edit", {
		"ref_doctype": doctype,
		"docname": docname,
	})


def _remove_local_edit(doctype, docname):
	"""Remove local edit tracking for a document."""
	edits = frappe.get_all("Sync Local Edit", filters={
		"ref_doctype": doctype,
		"docname": docname,
	}, pluck="name")
	for edit_name in edits:
		frappe.delete_doc("Sync Local Edit", edit_name, ignore_permissions=True, force=True)


def _create_conflict(change, source, conflict_type):
	"""Create a Sync Conflict record and raise SyncConflictDetected."""
	frappe.get_doc({
		"doctype": "Sync Conflict",
		"ref_doctype": change.get("ref_doctype"),
		"docname": change.get("docname"),
		"conflict_type": conflict_type,
		"primary_data": change.get("data") if isinstance(change.get("data"), str) else frappe.as_json(change.get("data")),
		"sync_source": source.name,
	}).insert(ignore_permissions=True)
	frappe.db.commit()

	_create_sync_log(change, source, "Skipped")

	raise SyncConflictDetected(f"Conflict: {change.get('ref_doctype')} {change.get('docname')}")


def _create_sync_log(change, source, status, error=None):
	"""Create a Sync Log record."""
	frappe.get_doc({
		"doctype": "Sync Log",
		"ref_doctype": change.get("ref_doctype"),
		"docname": change.get("docname"),
		"update_type": change.get("update_type"),
		"status": status,
		"sync_source": source.name,
		"data": change.get("data") if isinstance(change.get("data"), str) else frappe.as_json(change.get("data")),
		"error": error,
	}).insert(ignore_permissions=True)
	frappe.db.commit()


def _matches_filters(data, filters):
	"""Check if a document snapshot matches the given filters.

	Supports simple equality filters: {"company": "My Company", "status": "Active"}
	"""
	if not filters:
		return True

	for key, value in filters.items():
		doc_value = data.get(key)
		if isinstance(value, list):
			# Handle Frappe-style filter lists: ["in", ["A", "B"]]
			operator = value[0].lower() if value else "="
			operand = value[1] if len(value) > 1 else None
			if operator == "in" and isinstance(operand, list):
				if doc_value not in operand:
					return False
			elif operator == "not in" and isinstance(operand, list):
				if doc_value in operand:
					return False
			elif operator == "=":
				if doc_value != operand:
					return False
			elif operator == "!=":
				if doc_value == operand:
					return False
		else:
			# Simple equality
			if doc_value != value:
				return False

	return True


def _apply_doc_snapshot(doc_data, source, update_type):
	"""Apply a full doc snapshot (used by initial sync)."""
	change = {
		"ref_doctype": doc_data.get("doctype"),
		"docname": doc_data.get("name"),
		"update_type": update_type,
		"data": doc_data if isinstance(doc_data, dict) else frappe.parse_json(doc_data),
	}
	_apply_single_change(change, source)


def _restore_attribution(doc, original_data):
	"""Restore original user attribution after insert/save.

	Frappe overwrites owner/creation/modified/modified_by with the background
	job user (Administrator). This stamps the primary's original values back
	via db_set, bypassing the ORM.
	"""
	fields = {}
	for field in ("owner", "creation", "modified", "modified_by"):
		val = original_data.get(field)
		if val:
			fields[field] = val

	if fields:
		doc.db_set(fields, update_modified=False)


def _sync_attachments(doc, source):
	"""Download and sync file attachments from primary.

	Checks Attach and Attach Image fields for file URLs,
	downloads from primary, creates local File records.
	"""
	meta = frappe.get_meta(doc.doctype)
	attach_fields = [
		df for df in meta.fields
		if df.fieldtype in ("Attach", "Attach Image")
	]

	if not attach_fields:
		return

	for df in attach_fields:
		file_url = doc.get(df.fieldname)
		if not file_url:
			continue

		# Skip if not a relative file path (external URLs)
		if not file_url.startswith(("/files/", "/private/files/")):
			continue

		# Check if file already exists locally
		if frappe.db.exists("File", {"file_url": file_url}):
			continue

		# Download from primary
		try:
			from frappe.frappeclient import FrappeClient
			client = FrappeClient(
				url=source.primary_url,
				api_key=source.api_key,
				api_secret=source.get_password("api_secret"),
			)

			# Download the file content
			response = client.session.get(
				f"{source.primary_url}{file_url}",
				stream=True,
			)

			if response.status_code != 200:
				continue

			# Determine if private
			is_private = file_url.startswith("/private/")
			filename = file_url.split("/")[-1]

			# Save file locally
			file_doc = frappe.get_doc({
				"doctype": "File",
				"file_name": filename,
				"attached_to_doctype": doc.doctype,
				"attached_to_name": doc.name,
				"attached_to_field": df.fieldname,
				"is_private": 1 if is_private else 0,
				"content": response.content,
			})
			file_doc.flags.ignore_permissions = True
			file_doc.insert()

			# Update the field if the local URL is different
			if file_doc.file_url != file_url:
				doc.db_set(df.fieldname, file_doc.file_url, update_modified=False)

		except Exception:
			frappe.log_error(
				f"Attachment sync failed: {doc.doctype} {doc.name} field {df.fieldname}",
				"TDGL Streaming",
			)


# ============================================================
# REPLICA SIDE — Conflict Resolution
# ============================================================

@frappe.whitelist()
def resolve_conflict(conflict_name, resolution):
	"""Resolve a sync conflict.

	Args:
		conflict_name: Name of the Sync Conflict record
		resolution: "Accept Primary" or "Keep Local"
	"""
	conflict = frappe.get_doc("Sync Conflict", conflict_name)

	if conflict.resolved:
		frappe.throw("Conflict already resolved")

	if resolution == "Accept Primary":
		# Apply the primary's data
		if conflict.primary_data:
			data = frappe.parse_json(conflict.primary_data)
			frappe.flags.in_replica_sync = True
			try:
				dt = conflict.ref_doctype
				dn = conflict.docname

				if conflict.conflict_type == "Cancel":
					if frappe.db.exists(dt, dn):
						doc = frappe.get_doc(dt, dn)
						doc.flags.ignore_links = True
						doc.flags.ignore_permissions = True
						doc.cancel()
						_restore_attribution(doc, data)
				elif conflict.conflict_type == "Delete":
					if frappe.db.exists(dt, dn):
						frappe.delete_doc(dt, dn, force=True, ignore_permissions=True)
				else:
					# Update
					if frappe.db.exists(dt, dn):
						doc = frappe.get_doc(dt, dn)
						cleaned = _clean_snapshot(data)
						doc.update(cleaned)
						doc.flags.ignore_links = True
						doc.flags.ignore_validate = True
						doc.flags.ignore_permissions = True
						doc.flags.ignore_mandatory = True
						doc.save()
						_restore_attribution(doc, data)
					else:
						cleaned = _clean_snapshot(data)
						doc = frappe.get_doc(cleaned)
						doc.flags.ignore_links = True
						doc.flags.ignore_validate = True
						doc.flags.ignore_permissions = True
						doc.flags.ignore_mandatory = True
						doc.insert(set_name=dn, set_child_names=False)
						_restore_attribution(doc, data)
			finally:
				frappe.flags.in_replica_sync = False

		# Remove local edit tracking
		_remove_local_edit(conflict.ref_doctype, conflict.docname)

	conflict.resolution = resolution
	conflict.resolved = 1
	conflict.flags.ignore_permissions = True
	conflict.save()
	frappe.db.commit()


@frappe.whitelist()
def pull_now(sync_source_name):
	"""Trigger an immediate pull for a Sync Source."""
	frappe.enqueue(
		"tdgl_streaming.sync.pull_changes",
		sync_source_name=sync_source_name,
		queue="default",
		job_name=f"sync_pull_{sync_source_name}",
		enqueue_after_commit=True,
		timeout=1500,
	)
	return {"message": "Pull enqueued"}


@frappe.whitelist()
def reset_and_pull(sync_source_name):
	"""Reset cursor, clear old logs, and trigger a full re-sync."""
	source = frappe.get_doc("Sync Source", sync_source_name)
	source.db_set("last_pulled", None)
	source.db_set("last_pulled_name", "")
	frappe.db.delete("Sync Log", {"sync_source": sync_source_name})
	frappe.db.commit()

	frappe.enqueue(
		"tdgl_streaming.sync.pull_changes",
		sync_source_name=sync_source_name,
		queue="default",
		job_name=f"sync_pull_{sync_source_name}",
		enqueue_after_commit=True,
		timeout=1500,
	)
	return {"message": "Cursor reset, full re-sync enqueued"}


@frappe.whitelist()
def retry_failed(sync_source_name):
	"""Retry all failed Sync Log entries for a Sync Source."""
	source = frappe.get_doc("Sync Source", sync_source_name)
	failed_logs = frappe.get_all(
		"Sync Log",
		filters={"status": "Failed", "sync_source": sync_source_name},
		fields=["name", "ref_doctype", "docname", "update_type", "data"],
	)

	succeeded = 0
	failed = 0
	for log in failed_logs:
		change = {
			"ref_doctype": log.ref_doctype,
			"docname": log.docname,
			"update_type": log.update_type,
			"data": log.data,
		}
		try:
			frappe.flags.in_replica_sync = True
			update_type = change.get("update_type")

			if update_type == "Create":
				_apply_create(change, source)
			elif update_type in ("Update", "Update After Submit"):
				_apply_update(change, source)
			elif update_type == "Submit":
				_apply_submit(change, source)
			elif update_type == "Cancel":
				_apply_cancel(change, source)
			elif update_type == "Delete":
				_apply_delete(change, source)
			elif update_type == "Rename":
				_apply_rename(change, source)
			else:
				continue

			frappe.db.set_value("Sync Log", log.name, {
				"status": "Synced",
				"error": "",
			})
			frappe.db.commit()
			succeeded += 1

		except Exception:
			frappe.db.rollback()
			frappe.db.set_value("Sync Log", log.name, {
				"error": frappe.get_traceback(),
			})
			frappe.db.commit()
			failed += 1

		finally:
			frappe.flags.in_replica_sync = False

	return {"retried": len(failed_logs), "succeeded": succeeded, "failed": failed}
