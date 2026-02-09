import frappe
from frappe.utils import cstr


@frappe.whitelist()
def get_changes(last_pulled, last_pulled_name="", doctypes=None, limit=100):
	"""Primary endpoint — return Sync Change Log entries after the cursor.

	Called by replica's pull_changes via FrappeClient.post_api().

	Args:
		last_pulled: Datetime string — return entries after this time
		last_pulled_name: Name of last processed entry — for stable pagination
		doctypes: JSON list of doctype names to filter by
		limit: Max entries to return (default 100)
	"""
	if isinstance(doctypes, str):
		doctypes = frappe.parse_json(doctypes)

	if not doctypes:
		return []

	limit = min(int(limit), 500)  # cap at 500

	# Build IN clause with proper escaping
	doctype_list = ", ".join(frappe.db.escape(dt) for dt in doctypes)
	conditions = [f"`ref_doctype` IN ({doctype_list})"]
	values = {"limit": limit}

	if last_pulled and last_pulled_name:
		conditions.append(
			"(`creation` > %(last_pulled)s OR (`creation` = %(last_pulled)s AND `name` > %(last_pulled_name)s))"
		)
		values["last_pulled"] = cstr(last_pulled)
		values["last_pulled_name"] = cstr(last_pulled_name)
	elif last_pulled:
		conditions.append("`creation` > %(last_pulled)s")
		values["last_pulled"] = cstr(last_pulled)

	where_clause = " AND ".join(conditions)

	changes = frappe.db.sql(
		f"""
		SELECT `name`, `ref_doctype`, `docname`, `update_type`, `data`, `source_site`, `creation`
		FROM `tabSync Change Log`
		WHERE {where_clause}
		ORDER BY `creation` ASC, `name` ASC
		LIMIT %(limit)s
		""",
		values=values,
		as_dict=True,
	)

	return changes


@frappe.whitelist()
def notify():
	"""Replica endpoint — called by primary's push notification.

	Enqueues an immediate pull for all enabled Sync Sources.
	Requires authentication (not allow_guest).
	"""
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

	return {"status": "ok", "sources_queued": len(sources)}
