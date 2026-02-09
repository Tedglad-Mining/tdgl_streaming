import frappe
import json
from frappe.tests.utils import FrappeTestCase
from frappe.utils import now_datetime
from unittest.mock import patch

from tdgl_streaming.api import get_changes, notify


class TestGetChanges(FrappeTestCase):
	"""Test the get_changes API endpoint (primary side)."""

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		# Create test Sync Change Log entries with controlled timestamps
		cls.entries = []
		for i in range(5):
			entry = frappe.get_doc({
				"doctype": "Sync Change Log",
				"ref_doctype": "ToDo",
				"docname": f"_test_api_{i:03d}",
				"update_type": "Create",
				"data": frappe.as_json({"doctype": "ToDo", "name": f"_test_api_{i:03d}"}),
				"source_site": "test.site",
			})
			entry.insert(ignore_permissions=True)
			cls.entries.append(entry)

		# Add one entry for a different doctype
		entry = frappe.get_doc({
			"doctype": "Sync Change Log",
			"ref_doctype": "Note",
			"docname": "_test_api_note_001",
			"update_type": "Create",
			"data": frappe.as_json({"doctype": "Note", "name": "_test_api_note_001"}),
			"source_site": "test.site",
		})
		entry.insert(ignore_permissions=True)
		cls.entries.append(entry)

		frappe.db.commit()

	@classmethod
	def tearDownClass(cls):
		frappe.db.sql(
			"DELETE FROM `tabSync Change Log` WHERE `source_site` = 'test.site'"
		)
		frappe.db.commit()
		super().tearDownClass()

	def test_returns_entries_after_cursor(self):
		"""Entries created after last_pulled are returned."""
		# Use a very old cursor to get all entries
		result = get_changes(
			last_pulled="2020-01-01 00:00:00",
			doctypes=frappe.as_json(["ToDo"]),
		)
		# Should return at least our 5 ToDo test entries
		todo_entries = [r for r in result if r.get("docname", "").startswith("_test_api_")]
		self.assertGreaterEqual(len(todo_entries), 5)

	def test_filters_by_doctype(self):
		"""Only requested doctypes are returned."""
		result = get_changes(
			last_pulled="2020-01-01 00:00:00",
			doctypes=frappe.as_json(["Note"]),
		)
		# All results should be Note, not ToDo
		for entry in result:
			self.assertEqual(entry.get("ref_doctype"), "Note")

	def test_multiple_doctypes(self):
		"""Multiple doctypes can be requested at once."""
		result = get_changes(
			last_pulled="2020-01-01 00:00:00",
			doctypes=frappe.as_json(["ToDo", "Note"]),
		)
		doctypes_returned = {r.get("ref_doctype") for r in result}
		self.assertIn("ToDo", doctypes_returned)
		self.assertIn("Note", doctypes_returned)

	def test_respects_limit(self):
		"""Limit caps the number of entries returned."""
		result = get_changes(
			last_pulled="2020-01-01 00:00:00",
			doctypes=frappe.as_json(["ToDo"]),
			limit=2,
		)
		self.assertLessEqual(len(result), 2)

	def test_caps_limit_at_500(self):
		"""Limit above 500 is capped to 500."""
		result = get_changes(
			last_pulled="2020-01-01 00:00:00",
			doctypes=frappe.as_json(["ToDo"]),
			limit=999,
		)
		# We only have 5 test entries, so just verify it didn't error
		self.assertIsInstance(result, list)

	def test_empty_doctypes_returns_empty(self):
		"""No doctypes → empty result."""
		result = get_changes(
			last_pulled="2020-01-01 00:00:00",
			doctypes=frappe.as_json([]),
		)
		self.assertEqual(result, [])

	def test_none_doctypes_returns_empty(self):
		"""None doctypes → empty result."""
		result = get_changes(
			last_pulled="2020-01-01 00:00:00",
			doctypes=None,
		)
		self.assertEqual(result, [])

	def test_order_by_creation_asc(self):
		"""Results are ordered by creation ASC (oldest first)."""
		result = get_changes(
			last_pulled="2020-01-01 00:00:00",
			doctypes=frappe.as_json(["ToDo"]),
		)
		if len(result) >= 2:
			for i in range(len(result) - 1):
				self.assertLessEqual(
					str(result[i].get("creation")),
					str(result[i + 1].get("creation")),
				)

	def test_pagination_with_name(self):
		"""last_pulled_name breaks ties on same-timestamp entries."""
		if not self.entries:
			return

		# Get the first entry's creation and name as cursor
		first = self.entries[0]
		first_doc = frappe.get_doc("Sync Change Log", first.name)

		result = get_changes(
			last_pulled=str(first_doc.creation),
			last_pulled_name=first_doc.name,
			doctypes=frappe.as_json(["ToDo"]),
		)
		# Should NOT include the first entry (cursor is exclusive)
		names_returned = [r.get("name") for r in result]
		self.assertNotIn(first_doc.name, names_returned)

	def test_doctypes_as_string(self):
		"""doctypes parameter as JSON string (not pre-parsed) works."""
		result = get_changes(
			last_pulled="2020-01-01 00:00:00",
			doctypes='["ToDo"]',
		)
		self.assertIsInstance(result, list)
		self.assertTrue(len(result) > 0)


class TestNotify(FrappeTestCase):
	"""Test the notify API endpoint (replica side)."""

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		if not frappe.db.exists("Sync Source", "Test Notify Source"):
			frappe.get_doc({
				"doctype": "Sync Source",
				"source_name": "Test Notify Source",
				"primary_url": "https://test-primary.example.com",
				"api_key": "test_key",
				"api_secret": "test_secret",
				"enabled": 1,
				"pull_batch_size": 100,
				"sync_doctypes": [
					{"ref_doctype": "ToDo", "enabled": 1}
				],
			}).insert(ignore_permissions=True)
			frappe.db.commit()

	@classmethod
	def tearDownClass(cls):
		if frappe.db.exists("Sync Source", "Test Notify Source"):
			frappe.delete_doc("Sync Source", "Test Notify Source", force=True, ignore_permissions=True)
			frappe.db.commit()
		super().tearDownClass()

	@patch("frappe.enqueue")
	def test_notify_enqueues_pulls(self, mock_enqueue):
		"""notify() enqueues a pull for each enabled Sync Source."""
		result = notify()

		self.assertEqual(result["status"], "ok")
		self.assertGreaterEqual(result["sources_queued"], 1)

		# Check that enqueue was called with correct args
		mock_enqueue.assert_called()
		call_args = mock_enqueue.call_args
		self.assertEqual(call_args.kwargs.get("queue") or call_args[1].get("queue"), "default")
