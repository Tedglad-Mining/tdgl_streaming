import frappe
import json
from frappe.tests.utils import FrappeTestCase
from frappe.utils import now_datetime
from unittest.mock import patch, MagicMock

from tdgl_streaming.sync import (
	log_change,
	log_rename,
	track_local_edit,
	_apply_single_change,
	_apply_create,
	_apply_update,
	_apply_delete,
	_apply_rename,
	_apply_submit,
	_apply_cancel,
	_clean_snapshot,
	_matches_filters,
	_parse_data,
	_restore_attribution,
	_has_local_edit,
	_remove_local_edit,
	resolve_conflict,
	SyncConflictDetected,
	INTERNAL_DOCTYPES,
	STRIP_FIELDS,
	STRIP_CHILD_FIELDS,
)


def _clear_sync_caches():
	"""Clear per-request caches used by sync functions."""
	for attr in ("_sync_config", "_replica_synced_doctypes"):
		if hasattr(frappe.local, attr):
			delattr(frappe.local, attr)


def _make_todo(description="test sync", commit=True):
	"""Create a ToDo and return it."""
	todo = frappe.get_doc({"doctype": "ToDo", "description": description})
	todo.insert(ignore_permissions=True)
	if commit:
		frappe.db.commit()
	return todo


def _make_change(doctype, docname, update_type, data=None):
	"""Build a change dict like what comes from the primary API."""
	return {
		"ref_doctype": doctype,
		"docname": docname,
		"update_type": update_type,
		"data": frappe.as_json(data) if isinstance(data, dict) else data,
	}


def _todo_snapshot(name, description="synced todo", **overrides):
	"""Build a realistic ToDo snapshot dict."""
	snap = {
		"doctype": "ToDo",
		"name": name,
		"description": description,
		"status": "Open",
		"priority": "Medium",
		"owner": "primary@example.com",
		"creation": "2026-01-01 00:00:00.000000",
		"modified": "2026-01-01 00:00:00.000000",
		"modified_by": "primary@example.com",
		"_user_tags": "",
		"_liked_by": "",
		"_comments": "",
		"_assign": "",
	}
	snap.update(overrides)
	return snap


def _note_snapshot(name, title=None, content="synced note", **overrides):
	"""Build a realistic Note snapshot dict for replica apply tests."""
	snap = {
		"doctype": "Note",
		"name": name,
		"title": title or name,
		"content": content,
		"public": 1,
		"owner": "primary@example.com",
		"creation": "2026-01-01 00:00:00.000000",
		"modified": "2026-01-01 00:00:00.000000",
		"modified_by": "primary@example.com",
		"_user_tags": "",
		"_liked_by": "",
		"_comments": "",
		"_assign": "",
	}
	snap.update(overrides)
	return snap


# ============================================================
# HELPER / PURE FUNCTION TESTS
# ============================================================

class TestHelpers(FrappeTestCase):
	"""Tests for pure/helper functions that don't need complex DB setup."""

	def test_parse_data_string(self):
		result = _parse_data({"data": '{"doctype": "ToDo", "name": "X"}'})
		self.assertEqual(result["doctype"], "ToDo")
		self.assertEqual(result["name"], "X")

	def test_parse_data_dict(self):
		d = {"doctype": "ToDo", "name": "X"}
		result = _parse_data({"data": d})
		self.assertIs(result, d)

	def test_parse_data_none(self):
		result = _parse_data({"data": None})
		self.assertEqual(result, {})

	def test_parse_data_missing(self):
		result = _parse_data({})
		self.assertEqual(result, {})

	def test_clean_snapshot_strips_parent_fields(self):
		data = _todo_snapshot("X")
		cleaned = _clean_snapshot(data)
		for field in STRIP_FIELDS:
			self.assertNotIn(field, cleaned, f"{field} should be stripped")

	def test_clean_snapshot_preserves_data_fields(self):
		data = _todo_snapshot("X", description="keep me", status="Open")
		cleaned = _clean_snapshot(data)
		self.assertEqual(cleaned["description"], "keep me")
		self.assertEqual(cleaned["status"], "Open")
		self.assertEqual(cleaned["doctype"], "ToDo")

	def test_matches_filters_equality_match(self):
		self.assertTrue(_matches_filters({"company": "Acme"}, {"company": "Acme"}))

	def test_matches_filters_equality_mismatch(self):
		self.assertFalse(_matches_filters({"company": "Acme"}, {"company": "Other"}))

	def test_matches_filters_in_match(self):
		self.assertTrue(_matches_filters(
			{"status": "Open"},
			{"status": ["in", ["Open", "Closed"]]}
		))

	def test_matches_filters_in_mismatch(self):
		self.assertFalse(_matches_filters(
			{"status": "Draft"},
			{"status": ["in", ["Open", "Closed"]]}
		))

	def test_matches_filters_not_in_match(self):
		self.assertTrue(_matches_filters(
			{"status": "Draft"},
			{"status": ["not in", ["Open", "Closed"]]}
		))

	def test_matches_filters_not_in_mismatch(self):
		self.assertFalse(_matches_filters(
			{"status": "Open"},
			{"status": ["not in", ["Open", "Closed"]]}
		))

	def test_matches_filters_not_equal_match(self):
		self.assertTrue(_matches_filters(
			{"status": "Open"},
			{"status": ["!=", "Closed"]}
		))

	def test_matches_filters_not_equal_mismatch(self):
		self.assertFalse(_matches_filters(
			{"status": "Closed"},
			{"status": ["!=", "Closed"]}
		))

	def test_matches_filters_empty(self):
		self.assertTrue(_matches_filters({"anything": "value"}, {}))

	def test_matches_filters_none(self):
		self.assertTrue(_matches_filters({"anything": "value"}, None))

	def test_matches_filters_multiple_conditions(self):
		data = {"company": "Acme", "status": "Open"}
		self.assertTrue(_matches_filters(data, {"company": "Acme", "status": "Open"}))
		self.assertFalse(_matches_filters(data, {"company": "Acme", "status": "Closed"}))


# ============================================================
# PRIMARY SIDE — CHANGE LOGGING TESTS
# ============================================================

class TestPrimarySideLogging(FrappeTestCase):
	"""Test that doc_events on the primary side create Sync Change Log entries."""

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		# Configure Sync Config singleton to track ToDo
		config = frappe.get_doc("Sync Config")
		config.enabled = 1
		config.push_enabled = 0  # no push in tests
		config.set("sync_doctypes", [])
		config.append("sync_doctypes", {"ref_doctype": "ToDo", "enabled": 1})
		config.flags.ignore_permissions = True
		config.save()
		frappe.db.commit()

	def setUp(self):
		_clear_sync_caches()

	@classmethod
	def tearDownClass(cls):
		# Reset Sync Config
		config = frappe.get_doc("Sync Config")
		config.enabled = 0
		config.set("sync_doctypes", [])
		config.flags.ignore_permissions = True
		config.save()
		frappe.db.commit()
		super().tearDownClass()

	def _count_logs(self, docname, update_type=None, after=None):
		"""Count Sync Change Log entries for a docname."""
		filters = {"ref_doctype": "ToDo", "docname": docname}
		if update_type:
			filters["update_type"] = update_type
		if after:
			filters["creation"] = [">=", after]
		return frappe.db.count("Sync Change Log", filters)

	def test_log_change_creates_entry_on_insert(self):
		"""Inserting a tracked doc creates a Sync Change Log with update_type=Create."""
		todo = _make_todo("test create log")
		self.assertEqual(self._count_logs(todo.name, "Create"), 1)

	def test_log_change_on_update(self):
		"""Updating a tracked doc creates a Sync Change Log with update_type=Update."""
		todo = _make_todo("test update log")
		before = now_datetime()
		todo.description = "updated description"
		todo.save(ignore_permissions=True)
		frappe.db.commit()
		self.assertGreaterEqual(self._count_logs(todo.name, "Update", after=before), 1)

	def test_log_change_on_delete(self):
		"""Deleting a tracked doc creates a Sync Change Log with update_type=Delete and data=None."""
		todo = _make_todo("test delete log")
		name = todo.name
		frappe.delete_doc("ToDo", name, force=True, ignore_permissions=True)
		frappe.db.commit()
		self.assertEqual(self._count_logs(name, "Delete"), 1)
		# Verify data is None for delete
		log = frappe.get_all("Sync Change Log", filters={
			"ref_doctype": "ToDo", "docname": name, "update_type": "Delete"
		}, fields=["data"])
		self.assertFalse(log[0].data)

	def test_log_change_skips_internal_doctypes(self):
		"""Saving an internal doctype does NOT create a Sync Change Log."""
		before = now_datetime()
		# Sync Log is in INTERNAL_DOCTYPES
		frappe.get_doc({
			"doctype": "Sync Log",
			"ref_doctype": "ToDo",
			"docname": "test",
			"status": "Synced",
			"update_type": "Create",
		}).insert(ignore_permissions=True)
		frappe.db.commit()
		logs = frappe.get_all("Sync Change Log", filters={
			"ref_doctype": "Sync Log",
			"creation": [">=", before],
		})
		self.assertEqual(len(logs), 0)

	def test_log_change_skips_untracked_doctype(self):
		"""Saving a doctype NOT in Sync Config does not create a log."""
		before = now_datetime()
		note = frappe.get_doc({"doctype": "Note", "title": f"test_untracked_{before}"})
		note.insert(ignore_permissions=True)
		frappe.db.commit()
		logs = frappe.get_all("Sync Change Log", filters={
			"ref_doctype": "Note",
			"docname": note.name,
			"creation": [">=", before],
		})
		self.assertEqual(len(logs), 0)

	def test_log_change_skips_when_in_replica_sync(self):
		"""The in_replica_sync flag suppresses logging."""
		frappe.flags.in_replica_sync = True
		try:
			todo = frappe.get_doc({"doctype": "ToDo", "description": "replica sync test"})
			todo.insert(ignore_permissions=True)
			frappe.db.commit()
			self.assertEqual(self._count_logs(todo.name), 0)
		finally:
			frappe.flags.in_replica_sync = False

	def test_dedup_insert_creates_single_entry(self):
		"""Insert should create exactly one log entry (Create), not two.

		on_update also fires during insert, but dedup logic skips it.
		"""
		todo = _make_todo("test dedup insert")
		# Should have exactly 1 Create, 0 Updates
		self.assertEqual(self._count_logs(todo.name, "Create"), 1)
		self.assertEqual(self._count_logs(todo.name, "Update"), 0)

	def test_log_rename(self):
		"""Renaming a tracked doc creates a Rename log with old/new name in data."""
		todo = _make_todo("test rename log")
		old_name = todo.name
		new_name = f"_test_renamed_{old_name}"
		frappe.rename_doc("ToDo", old_name, new_name, force=True)
		frappe.db.commit()

		logs = frappe.get_all("Sync Change Log", filters={
			"ref_doctype": "ToDo",
			"docname": new_name,
			"update_type": "Rename",
		}, fields=["data"])
		self.assertEqual(len(logs), 1)
		data = json.loads(logs[0].data)
		self.assertEqual(data["old_name"], old_name)
		self.assertEqual(data["new_name"], new_name)

		# Cleanup rename
		frappe.rename_doc("ToDo", new_name, old_name, force=True)
		frappe.db.commit()


# ============================================================
# REPLICA SIDE — APPLY TESTS (uses Note to avoid ToDo.on_update crash)
# ============================================================

class TestReplicaApply(FrappeTestCase):
	"""Test applying changes from primary on the replica side.

	Uses Note instead of ToDo because ToDo.on_update accesses self._assignment
	which is only set in validate(). Since sync uses ignore_validate=True,
	ToDo.on_update would crash with AttributeError.
	"""

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		# Disable primary-side logging to avoid interference
		config = frappe.get_doc("Sync Config")
		config.enabled = 0
		config.set("sync_doctypes", [])
		config.flags.ignore_permissions = True
		config.save()
		frappe.db.commit()

		# Create a Sync Source for replica tests (tracking Note)
		if frappe.db.exists("Sync Source", "Test Primary"):
			frappe.delete_doc("Sync Source", "Test Primary", force=True, ignore_permissions=True)
		frappe.get_doc({
			"doctype": "Sync Source",
			"source_name": "Test Primary",
			"primary_url": "https://test-primary.example.com",
			"api_key": "test_key",
			"api_secret": "test_secret",
			"enabled": 1,
			"pull_batch_size": 100,
			"sync_doctypes": [
				{"ref_doctype": "Note", "enabled": 1}
			],
		}).insert(ignore_permissions=True)
		frappe.db.commit()

		cls.source = frappe.get_doc("Sync Source", "Test Primary")

	def setUp(self):
		_clear_sync_caches()

	@classmethod
	def tearDownClass(cls):
		# Clean up test records
		for dt in ("Sync Log", "Sync Conflict", "Sync Local Edit", "Sync Change Log"):
			frappe.db.sql(f"DELETE FROM `tab{dt}` WHERE `ref_doctype` = 'Note'")
		if frappe.db.exists("Sync Source", "Test Primary"):
			frappe.delete_doc("Sync Source", "Test Primary", force=True, ignore_permissions=True)
		frappe.db.commit()
		super().tearDownClass()

	def _cleanup_note(self, name):
		"""Delete a test Note if it exists."""
		if frappe.db.exists("Note", name):
			frappe.delete_doc("Note", name, force=True, ignore_permissions=True)
			frappe.db.commit()

	def test_apply_create(self):
		"""_apply_single_change with Create builds a new doc with the correct name."""
		name = "_test_sync_create_001"
		self._cleanup_note(name)

		snap = _note_snapshot(name, content="created via sync")
		change = _make_change("Note", name, "Create", snap)
		_apply_single_change(change, self.source)

		self.assertTrue(frappe.db.exists("Note", name))
		note = frappe.get_doc("Note", name)
		self.assertEqual(note.content, "created via sync")

		# Sync Log should be Synced
		log = frappe.get_all("Sync Log", filters={
			"ref_doctype": "Note", "docname": name, "status": "Synced"
		})
		self.assertTrue(log)
		self._cleanup_note(name)

	def test_apply_create_restores_attribution(self):
		"""After create, owner/creation/modified/modified_by match the primary snapshot."""
		name = "_test_sync_attr_001"
		self._cleanup_note(name)

		snap = _note_snapshot(
			name,
			owner="user@primary.com",
			creation="2025-06-15 10:30:00.000000",
			modified="2025-06-15 11:00:00.000000",
			modified_by="editor@primary.com",
		)
		change = _make_change("Note", name, "Create", snap)
		_apply_single_change(change, self.source)

		vals = frappe.db.get_value("Note", name,
			["owner", "creation", "modified", "modified_by"], as_dict=True)
		self.assertEqual(vals.owner, "user@primary.com")
		self.assertEqual(str(vals.creation), "2025-06-15 10:30:00")
		self.assertEqual(vals.modified_by, "editor@primary.com")
		self._cleanup_note(name)

	def test_apply_create_falls_to_update_if_exists(self):
		"""If doc already exists (no local edit), Create falls through to Update."""
		name = "_test_sync_fallthru_001"
		self._cleanup_note(name)

		# Pre-create the doc
		frappe.flags.in_replica_sync = True
		try:
			doc = frappe.get_doc({"doctype": "Note", "title": name, "content": "original"})
			doc.flags.ignore_permissions = True
			doc.insert(set_name=name)
			frappe.db.commit()
		finally:
			frappe.flags.in_replica_sync = False

		# Now apply a Create with new content
		snap = _note_snapshot(name, content="updated via fallthrough")
		change = _make_change("Note", name, "Create", snap)
		_apply_single_change(change, self.source)

		note = frappe.get_doc("Note", name)
		self.assertEqual(note.content, "updated via fallthrough")
		self._cleanup_note(name)

	def test_apply_update(self):
		"""Apply an Update change to an existing doc."""
		name = "_test_sync_update_001"
		self._cleanup_note(name)

		# Create first
		snap = _note_snapshot(name, content="before update")
		change = _make_change("Note", name, "Create", snap)
		_apply_single_change(change, self.source)

		# Now update
		snap2 = _note_snapshot(name, content="after update")
		change2 = _make_change("Note", name, "Update", snap2)
		_apply_single_change(change2, self.source)

		note = frappe.get_doc("Note", name)
		self.assertEqual(note.content, "after update")
		self._cleanup_note(name)

	def test_apply_update_falls_to_create_if_missing(self):
		"""If doc doesn't exist, Update falls through to Create."""
		name = "_test_sync_upd_create_001"
		self._cleanup_note(name)

		snap = _note_snapshot(name, content="created via update")
		change = _make_change("Note", name, "Update", snap)
		_apply_single_change(change, self.source)

		self.assertTrue(frappe.db.exists("Note", name))
		note = frappe.get_doc("Note", name)
		self.assertEqual(note.content, "created via update")
		self._cleanup_note(name)

	def test_apply_update_restores_attribution(self):
		"""After update, modified/modified_by match the primary snapshot."""
		name = "_test_sync_upd_attr_001"
		self._cleanup_note(name)

		# Create
		snap = _note_snapshot(name)
		change = _make_change("Note", name, "Create", snap)
		_apply_single_change(change, self.source)

		# Update with different attribution
		snap2 = _note_snapshot(
			name,
			modified="2025-12-25 12:00:00.000000",
			modified_by="santa@primary.com",
		)
		change2 = _make_change("Note", name, "Update", snap2)
		_apply_single_change(change2, self.source)

		vals = frappe.db.get_value("Note", name, ["modified", "modified_by"], as_dict=True)
		self.assertEqual(vals.modified_by, "santa@primary.com")
		self._cleanup_note(name)

	def test_apply_delete(self):
		"""Apply a Delete change removes the doc."""
		name = "_test_sync_delete_001"
		self._cleanup_note(name)

		# Create first
		snap = _note_snapshot(name)
		change = _make_change("Note", name, "Create", snap)
		_apply_single_change(change, self.source)
		self.assertTrue(frappe.db.exists("Note", name))

		# Delete
		del_change = _make_change("Note", name, "Delete")
		_apply_single_change(del_change, self.source)
		self.assertFalse(frappe.db.exists("Note", name))

	def test_apply_delete_skips_missing(self):
		"""Deleting a non-existent doc doesn't error."""
		change = _make_change("Note", "_test_sync_ghost_001", "Delete")
		# Should not raise
		_apply_single_change(change, self.source)

	def test_apply_rename(self):
		"""Apply a Rename change renames the doc."""
		old_name = "_test_sync_rename_old_001"
		new_name = "_test_sync_rename_new_001"
		self._cleanup_note(old_name)
		self._cleanup_note(new_name)

		# Create
		snap = _note_snapshot(old_name, content="will be renamed")
		change = _make_change("Note", old_name, "Create", snap)
		_apply_single_change(change, self.source)

		# Rename
		rename_data = {"old_name": old_name, "new_name": new_name}
		rename_change = _make_change("Note", new_name, "Rename", rename_data)
		_apply_single_change(rename_change, self.source)

		self.assertFalse(frappe.db.exists("Note", old_name))
		self.assertTrue(frappe.db.exists("Note", new_name))
		self._cleanup_note(new_name)

	def test_apply_rename_skips_missing(self):
		"""Renaming a non-existent doc doesn't error."""
		rename_data = {"old_name": "_test_sync_ghost_002", "new_name": "_test_sync_ghost_003"}
		change = _make_change("Note", "_test_sync_ghost_003", "Rename", rename_data)
		# Should not raise
		_apply_single_change(change, self.source)

	def test_apply_error_logged_as_failed(self):
		"""When apply throws an exception, Sync Log records it as Failed."""
		name = "_test_sync_error_001"

		# Force an error by providing invalid data (missing doctype in snapshot)
		bad_change = _make_change("Note", name, "Create", {"content": "no doctype"})
		_apply_single_change(bad_change, self.source)

		logs = frappe.get_all("Sync Log", filters={
			"ref_doctype": "Note", "docname": name, "status": "Failed"
		}, fields=["error"])
		self.assertTrue(logs)
		self.assertTrue(logs[0].error)  # has traceback

	def test_apply_error_doesnt_block_next(self):
		"""One failure doesn't prevent the next change from being applied."""
		good_name = "_test_sync_good_001"
		self._cleanup_note(good_name)

		# Bad change (will fail)
		bad_change = _make_change("Note", "_test_sync_bad_001", "Create",
			{"content": "no doctype"})
		_apply_single_change(bad_change, self.source)

		# Good change (should succeed)
		snap = _note_snapshot(good_name, content="should work")
		good_change = _make_change("Note", good_name, "Create", snap)
		_apply_single_change(good_change, self.source)

		self.assertTrue(frappe.db.exists("Note", good_name))
		self._cleanup_note(good_name)


# ============================================================
# SUBMIT / CANCEL TESTS (mocked — Note/ToDo are not submittable)
# ============================================================

class TestSubmitCancel(FrappeTestCase):
	"""Test submit/cancel apply paths using mocks since test doctypes are not submittable."""

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		if frappe.db.exists("Sync Source", "Test Primary"):
			frappe.delete_doc("Sync Source", "Test Primary", force=True, ignore_permissions=True)
		frappe.get_doc({
			"doctype": "Sync Source",
			"source_name": "Test Primary",
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
		cls.source = frappe.get_doc("Sync Source", "Test Primary")

	def setUp(self):
		_clear_sync_caches()

	@classmethod
	def tearDownClass(cls):
		for dt in ("Sync Log", "Sync Conflict", "Sync Local Edit"):
			frappe.db.sql(f"DELETE FROM `tab{dt}` WHERE `ref_doctype` = 'ToDo'")
		if frappe.db.exists("Sync Source", "Test Primary"):
			frappe.delete_doc("Sync Source", "Test Primary", force=True, ignore_permissions=True)
		frappe.db.commit()
		super().tearDownClass()

	def test_apply_submit_draft(self):
		"""Submit on a draft doc (docstatus=0) calls doc.submit()."""
		mock_doc = MagicMock()
		mock_doc.docstatus = 0

		snap = _todo_snapshot("_test_submit_001")
		change = _make_change("ToDo", "_test_submit_001", "Submit", snap)

		original_get_doc = frappe.get_doc

		def mock_get_doc(*args, **kwargs):
			if len(args) >= 2 and args[0] == "ToDo" and args[1] == "_test_submit_001":
				return mock_doc
			return original_get_doc(*args, **kwargs)

		with patch("frappe.get_doc", side_effect=mock_get_doc), \
			 patch("frappe.db.exists", return_value=True):

			frappe.flags.in_replica_sync = True
			try:
				_apply_submit(change, self.source)
			finally:
				frappe.flags.in_replica_sync = False

			mock_doc.submit.assert_called_once()

	def test_apply_submit_cancelled_with_local_edit(self):
		"""Submit on a cancelled doc with local edit creates a conflict."""
		name = "_test_submit_cancel_001"

		mock_doc = MagicMock()
		mock_doc.docstatus = 2

		snap = _todo_snapshot(name)
		change = _make_change("ToDo", name, "Submit", snap)

		# Create a real local edit record
		if not frappe.db.exists("Sync Local Edit", {"ref_doctype": "ToDo", "docname": name}):
			frappe.get_doc({
				"doctype": "Sync Local Edit",
				"ref_doctype": "ToDo",
				"docname": name,
			}).insert(ignore_permissions=True)
			frappe.db.commit()

		original_get_doc = frappe.get_doc
		original_exists = frappe.db.exists

		def mock_get_doc(*args, **kwargs):
			# Return mock only for the specific doc load
			if len(args) >= 2 and args[0] == "ToDo" and args[1] == name:
				return mock_doc
			return original_get_doc(*args, **kwargs)

		def mock_exists(*args, **kwargs):
			# Return True for "does ToDo exist?" but let real DB handle Sync Local Edit
			if len(args) >= 2 and args[0] == "ToDo" and isinstance(args[1], str):
				return True
			return original_exists(*args, **kwargs)

		with patch("frappe.get_doc", side_effect=mock_get_doc), \
			 patch("frappe.db.exists", side_effect=mock_exists):

			frappe.flags.in_replica_sync = True
			try:
				with self.assertRaises(SyncConflictDetected):
					_apply_submit(change, self.source)
			finally:
				frappe.flags.in_replica_sync = False

		# Verify conflict was created
		conflicts = frappe.get_all("Sync Conflict", filters={
			"ref_doctype": "ToDo", "docname": name
		})
		self.assertTrue(conflicts)

	def test_apply_cancel_with_local_edit(self):
		"""Cancel on a doc with local edit creates a conflict."""
		name = "_test_cancel_conflict_001"

		# Create real local edit
		if not frappe.db.exists("Sync Local Edit", {"ref_doctype": "ToDo", "docname": name}):
			frappe.get_doc({
				"doctype": "Sync Local Edit",
				"ref_doctype": "ToDo",
				"docname": name,
			}).insert(ignore_permissions=True)
			frappe.db.commit()

		change = _make_change("ToDo", name, "Cancel")

		original_exists = frappe.db.exists

		def mock_exists(*args, **kwargs):
			# Return True for "does ToDo exist?" but let real DB handle Sync Local Edit
			if len(args) >= 2 and args[0] == "ToDo" and isinstance(args[1], str):
				return True
			return original_exists(*args, **kwargs)

		with patch("frappe.db.exists", side_effect=mock_exists):
			frappe.flags.in_replica_sync = True
			try:
				with self.assertRaises(SyncConflictDetected):
					_apply_cancel(change, self.source)
			finally:
				frappe.flags.in_replica_sync = False

		# Verify conflict was created
		conflicts = frappe.get_all("Sync Conflict", filters={
			"ref_doctype": "ToDo", "docname": name
		})
		self.assertTrue(conflicts)

	def test_apply_cancel_skips_missing(self):
		"""Cancel on a non-existent doc just returns."""
		change = _make_change("ToDo", "_test_cancel_ghost_001", "Cancel")
		# Should not raise
		_apply_cancel(change, self.source)


# ============================================================
# CONFLICT DETECTION TESTS
# ============================================================

class TestConflictDetection(FrappeTestCase):
	"""Test local edit tracking and conflict creation."""

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		# Disable primary-side logging
		config = frappe.get_doc("Sync Config")
		config.enabled = 0
		config.set("sync_doctypes", [])
		config.flags.ignore_permissions = True
		config.save()
		frappe.db.commit()

		# Create Sync Source tracking ToDo (enables track_local_edit)
		if frappe.db.exists("Sync Source", "Test Primary"):
			frappe.delete_doc("Sync Source", "Test Primary", force=True, ignore_permissions=True)
		frappe.get_doc({
			"doctype": "Sync Source",
			"source_name": "Test Primary",
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

		cls.source = frappe.get_doc("Sync Source", "Test Primary")

	def setUp(self):
		_clear_sync_caches()

	@classmethod
	def tearDownClass(cls):
		for dt in ("Sync Log", "Sync Conflict", "Sync Local Edit", "Sync Change Log"):
			frappe.db.sql(f"DELETE FROM `tab{dt}` WHERE `ref_doctype` = 'ToDo'")
		if frappe.db.exists("Sync Source", "Test Primary"):
			frappe.delete_doc("Sync Source", "Test Primary", force=True, ignore_permissions=True)
		frappe.db.commit()
		super().tearDownClass()

	def _cleanup_todo(self, name):
		if frappe.db.exists("ToDo", name):
			frappe.delete_doc("ToDo", name, force=True, ignore_permissions=True)
			frappe.db.commit()

	def test_track_local_edit_on_update(self):
		"""Editing a synced ToDo locally creates a Sync Local Edit."""
		todo = _make_todo("test local edit tracking")
		name = todo.name

		# The insert above triggers on_update → track_local_edit
		# Since Sync Source tracks ToDo, a Sync Local Edit should exist
		self.assertTrue(_has_local_edit("ToDo", name))

	def test_track_local_edit_dedup(self):
		"""Editing the same doc twice creates only one Sync Local Edit."""
		todo = _make_todo("test dedup local edit")
		name = todo.name

		# Edit again
		todo.description = "edited again"
		todo.save(ignore_permissions=True)
		frappe.db.commit()

		edits = frappe.get_all("Sync Local Edit", filters={
			"ref_doctype": "ToDo", "docname": name,
		})
		self.assertEqual(len(edits), 1)

	def test_track_local_edit_skips_internal(self):
		"""Internal doctypes don't get tracked."""
		before = now_datetime()
		frappe.get_doc({
			"doctype": "Sync Log",
			"ref_doctype": "ToDo",
			"docname": "test",
			"status": "Synced",
			"update_type": "Create",
		}).insert(ignore_permissions=True)
		frappe.db.commit()

		edits = frappe.get_all("Sync Local Edit", filters={
			"ref_doctype": "Sync Log",
			"creation": [">=", before],
		})
		self.assertEqual(len(edits), 0)

	def test_track_local_edit_skips_replica_sync(self):
		"""in_replica_sync flag suppresses local edit tracking."""
		frappe.flags.in_replica_sync = True
		try:
			todo = frappe.get_doc({"doctype": "ToDo", "description": "sync write"})
			todo.insert(ignore_permissions=True)
			frappe.db.commit()
			self.assertFalse(_has_local_edit("ToDo", todo.name))
		finally:
			frappe.flags.in_replica_sync = False

	def test_track_local_edit_skips_untracked(self):
		"""Doctypes not in any Sync Source don't get tracked."""
		before = now_datetime()
		note = frappe.get_doc({"doctype": "Note", "title": f"test_untracked_{before}"})
		note.insert(ignore_permissions=True)
		frappe.db.commit()

		edits = frappe.get_all("Sync Local Edit", filters={
			"ref_doctype": "Note",
			"docname": note.name,
		})
		self.assertEqual(len(edits), 0)

	def test_conflict_on_create_with_local_edit(self):
		"""Create change for a doc with local edit creates a Sync Conflict."""
		name = "_test_sync_conflict_create_001"
		self._cleanup_todo(name)

		# Create the doc locally (will auto-track local edit via track_local_edit)
		todo = frappe.get_doc({"doctype": "ToDo", "description": "local version"})
		todo.insert(ignore_permissions=True, set_name=name)
		frappe.db.commit()
		self.assertTrue(_has_local_edit("ToDo", name))

		# Apply a Create from primary — conflict path bails before save,
		# so ToDo.on_update crash doesn't happen.
		snap = _todo_snapshot(name, description="primary version")
		change = _make_change("ToDo", name, "Create", snap)
		_apply_single_change(change, self.source)

		# Should be a conflict, not a normal sync
		conflicts = frappe.get_all("Sync Conflict", filters={
			"ref_doctype": "ToDo", "docname": name,
		})
		self.assertTrue(conflicts)

		# Sync Log should be Skipped
		logs = frappe.get_all("Sync Log", filters={
			"ref_doctype": "ToDo", "docname": name, "status": "Skipped",
		})
		self.assertTrue(logs)

		# Doc should keep local version
		todo = frappe.get_doc("ToDo", name)
		self.assertEqual(todo.description, "local version")
		self._cleanup_todo(name)

	def test_conflict_on_update_with_local_edit(self):
		"""Update change for a doc with local edit creates a Sync Conflict."""
		name = "_test_sync_conflict_update_001"
		self._cleanup_todo(name)

		# Create via sync (no local edit)
		frappe.flags.in_replica_sync = True
		try:
			frappe.get_doc({"doctype": "ToDo", "description": "synced"}).insert(
				ignore_permissions=True, set_name=name
			)
			frappe.db.commit()
		finally:
			frappe.flags.in_replica_sync = False

		# Edit locally → creates local edit
		todo = frappe.get_doc("ToDo", name)
		todo.description = "locally edited"
		todo.save(ignore_permissions=True)
		frappe.db.commit()
		self.assertTrue(_has_local_edit("ToDo", name))

		# Apply an Update from primary — conflict path bails before save
		snap = _todo_snapshot(name, description="primary update")
		change = _make_change("ToDo", name, "Update", snap)
		_apply_single_change(change, self.source)

		conflicts = frappe.get_all("Sync Conflict", filters={
			"ref_doctype": "ToDo", "docname": name,
		})
		self.assertTrue(conflicts)

		# Doc should keep local version
		todo = frappe.get_doc("ToDo", name)
		self.assertEqual(todo.description, "locally edited")
		self._cleanup_todo(name)


# ============================================================
# CONFLICT RESOLUTION TESTS
# ============================================================

class TestResolveConflict(FrappeTestCase):
	"""Test the resolve_conflict function."""

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		config = frappe.get_doc("Sync Config")
		config.enabled = 0
		config.set("sync_doctypes", [])
		config.flags.ignore_permissions = True
		config.save()
		frappe.db.commit()

		if frappe.db.exists("Sync Source", "Test Primary"):
			frappe.delete_doc("Sync Source", "Test Primary", force=True, ignore_permissions=True)
		frappe.get_doc({
			"doctype": "Sync Source",
			"source_name": "Test Primary",
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

		cls.source = frappe.get_doc("Sync Source", "Test Primary")

	def setUp(self):
		_clear_sync_caches()

	@classmethod
	def tearDownClass(cls):
		for dt in ("Sync Log", "Sync Conflict", "Sync Local Edit"):
			frappe.db.sql(f"DELETE FROM `tab{dt}` WHERE `ref_doctype` IN ('ToDo', 'Note')")
		if frappe.db.exists("Sync Source", "Test Primary"):
			frappe.delete_doc("Sync Source", "Test Primary", force=True, ignore_permissions=True)
		frappe.db.commit()
		super().tearDownClass()

	def _cleanup_note(self, name):
		if frappe.db.exists("Note", name):
			frappe.delete_doc("Note", name, force=True, ignore_permissions=True)
			frappe.db.commit()

	def test_resolve_accept_primary(self):
		"""Accept Primary applies the primary's data and removes local edit.

		Uses Note instead of ToDo to avoid ToDo.on_update crash with ignore_validate.
		"""
		name = "_test_sync_resolve_accept_001"
		self._cleanup_note(name)

		# Create Note locally
		frappe.get_doc({"doctype": "Note", "title": name, "content": "local"}).insert(
			ignore_permissions=True, set_name=name
		)
		frappe.db.commit()

		# Create local edit
		frappe.get_doc({
			"doctype": "Sync Local Edit",
			"ref_doctype": "Note",
			"docname": name,
		}).insert(ignore_permissions=True)
		frappe.db.commit()

		# Create conflict with Note snapshot
		primary_snap = _note_snapshot(name, content="primary version")
		conflict = frappe.get_doc({
			"doctype": "Sync Conflict",
			"ref_doctype": "Note",
			"docname": name,
			"conflict_type": "Update",
			"primary_data": frappe.as_json(primary_snap),
			"sync_source": self.source.name,
		})
		conflict.insert(ignore_permissions=True)
		frappe.db.commit()

		# Resolve: Accept Primary
		resolve_conflict(conflict.name, "Accept Primary")

		# Doc should now have primary's content
		note = frappe.get_doc("Note", name)
		self.assertEqual(note.content, "primary version")

		# Conflict should be resolved
		conflict.reload()
		self.assertTrue(conflict.resolved)
		self.assertEqual(conflict.resolution, "Accept Primary")

		# Local edit should be removed
		self.assertFalse(_has_local_edit("Note", name))
		self._cleanup_note(name)

	def test_resolve_keep_local(self):
		"""Keep Local marks conflict resolved without changing the doc."""
		name = "_test_sync_resolve_keep_001"

		# Create doc locally
		frappe.get_doc({"doctype": "ToDo", "description": "my local version"}).insert(
			ignore_permissions=True, set_name=name
		)
		frappe.db.commit()

		# Create conflict
		primary_snap = _todo_snapshot(name, description="primary version")
		conflict = frappe.get_doc({
			"doctype": "Sync Conflict",
			"ref_doctype": "ToDo",
			"docname": name,
			"conflict_type": "Update",
			"primary_data": frappe.as_json(primary_snap),
			"sync_source": self.source.name,
		})
		conflict.insert(ignore_permissions=True)
		frappe.db.commit()

		# Resolve: Keep Local
		resolve_conflict(conflict.name, "Keep Local")

		# Doc should be unchanged
		todo = frappe.get_doc("ToDo", name)
		self.assertEqual(todo.description, "my local version")

		# Conflict should be marked resolved
		conflict.reload()
		self.assertTrue(conflict.resolved)
		self.assertEqual(conflict.resolution, "Keep Local")

		# Cleanup
		if frappe.db.exists("ToDo", name):
			frappe.delete_doc("ToDo", name, force=True, ignore_permissions=True)
			frappe.db.commit()

	def test_resolve_already_resolved_throws(self):
		"""Resolving an already-resolved conflict throws an error."""
		conflict = frappe.get_doc({
			"doctype": "Sync Conflict",
			"ref_doctype": "ToDo",
			"docname": "_test_sync_double_resolve",
			"conflict_type": "Update",
			"primary_data": "{}",
			"sync_source": self.source.name,
			"resolved": 1,
			"resolution": "Keep Local",
		})
		conflict.insert(ignore_permissions=True)
		frappe.db.commit()

		with self.assertRaises(frappe.ValidationError):
			resolve_conflict(conflict.name, "Accept Primary")
