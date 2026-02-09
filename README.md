## TDGL Streaming

One-way document sync from a primary ERPNext/Frappe instance to a replica. Push+poll architecture with full document snapshots and conflict detection.

### How It Works

```
PRIMARY                                  REPLICA
--------                                 --------
Doc saved/submitted/cancelled
  -> log_change() fires
  -> Sync Change Log created
  -> push notify (optional)
                                         scheduled_pull() every 60s
                                           -> calls get_changes API on primary
                                           -> applies each change locally
                                           -> logs result in Sync Log
```

**Primary** logs every change to tracked DocTypes into `Sync Change Log` entries (full JSON snapshots). **Replica** polls the primary's API for new entries, applies them locally, and tracks results in `Sync Log`.

### DocTypes

#### Primary-side
| DocType | Purpose |
|---|---|
| **Sync Config** (Singleton) | Master switch, list of tracked DocTypes, push URL |
| **Sync Config Doctypes** (Child) | Which DocTypes to track (ref_doctype + enabled) |
| **Sync Change Log** | Immutable log of every change (Create/Update/Submit/Cancel/Delete/Rename) |

#### Replica-side
| DocType | Purpose |
|---|---|
| **Sync Source** | Connection to a primary (URL, API key/secret, cursor, batch size) |
| **Sync Source DocType** (Child) | Which DocTypes to pull + optional JSON filters |
| **Sync Log** | Result of each apply (Synced/Failed/Skipped) |
| **Sync Conflict** | When primary change collides with a local edit |
| **Sync Local Edit** | Tracks which docs were edited locally on replica |

### Setup

#### On the Primary

1. Install the app: `bench --site primary.site install-app tdgl_streaming`
2. Run `bench migrate`
3. Go to **Sync Config** (single doc)
4. Check **Enabled**
5. Add rows to **Sync DocTypes** — pick each DocType to track, check Enabled
6. (Optional) Set **Push URL** to the replica's URL and check **Push Enabled** for real-time notifications

#### On the Replica

1. Install the app: `bench --site replica.site install-app tdgl_streaming`
2. Run `bench migrate`
3. Create a **Sync Source** record:
   - **Source Name**: human label (e.g. "HQ Primary")
   - **Primary URL**: full URL of the primary site (e.g. `https://primary.example.com`)
   - **API Key** + **API Secret**: create an API key on the primary for a System Manager user
   - **Pull Batch Size**: 100 (default)
4. Add rows to **Sync DocTypes** — pick DocTypes to pull, set filters if needed
5. The first pull runs automatically — all existing docs for tracked DocTypes are pulled in bulk

**Important**: If you create documents locally on the replica in synced DocTypes, set a **distinct naming series prefix** on the replica to avoid name collisions with the primary (e.g. `SINV-R-` instead of `SINV-`).

### Change Tracking (Primary)

The app hooks into **all** document events via wildcard `doc_events`:

| Event | Update Type | When |
|---|---|---|
| `after_insert` | Create | New doc inserted |
| `on_update` | Update | Existing doc saved |
| `on_submit` | Submit | Doc submitted (docstatus 0→1) |
| `on_cancel` | Cancel | Doc cancelled (docstatus 1→2) |
| `on_trash` | Delete | Doc deleted |
| `on_update_after_submit` | Update After Submit | Submitted doc modified |
| `after_rename` | Rename | Doc renamed |

Each event creates a `Sync Change Log` entry with the full document snapshot as JSON (`doc.as_dict()`). Internal/system DocTypes are excluded.

Dedup guards prevent double-logging when multiple events fire for the same action (e.g. `on_update` also fires during insert and submit).

### Pull Cycle (Replica)

Every 60 seconds, `scheduled_pull` enqueues a background job for each enabled Sync Source:

1. Call primary's `get_changes` API with a cursor (last_pulled datetime + name)
2. For each change entry:
   - Check if DocType is in the Sync Source config
   - Apply replica-side filters (JSON field on Sync Source DocType rows)
   - Apply the change (create/update/submit/cancel/delete/rename)
   - On success: log as Synced
   - On failure: rollback, log as Failed with traceback, **continue to next entry**
3. Advance cursor

Concurrent pulls for the same source are prevented via RQ job name deduplication.

### Initial Sync

When a Sync Source has no `last_pulled` value (first pull), the app automatically runs a full sync:

- For each tracked DocType, paginate through all docs on the primary
- Pull each doc's full snapshot via `get_doc` API
- Apply locally as a Create
- After completion, set the cursor to now

### Conflict Detection

When a document is edited locally on the replica (outside of sync), it's tracked in `Sync Local Edit`. When the primary later sends a change for that same doc:

- A `Sync Conflict` record is created with the primary's data
- The change is logged as **Skipped** — the doc is NOT overwritten
- Admin resolves the conflict from the Sync Conflict list:
  - **Accept Primary**: applies the primary's data, removes local edit tracking
  - **Keep Local**: marks as resolved, keeps the replica's version

### File Attachments

After applying a doc, the app checks for `Attach` and `Attach Image` fields. If the field contains a relative file URL (like `/files/invoice.pdf`), it:

1. Checks if the file already exists locally
2. Downloads it from the primary via authenticated request
3. Creates a local `File` record
4. Updates the field URL if needed

### API Endpoints

#### `tdgl_streaming.api.get_changes` (Primary)
Returns Sync Change Log entries after a cursor. Called by replica's pull.

- `last_pulled` (Datetime) — entries after this time
- `last_pulled_name` (String) — for stable pagination within same timestamp
- `doctypes` (JSON list) — filter by DocType
- `limit` (Int, default 100, max 500)

#### `tdgl_streaming.api.notify` (Replica)
Triggers an immediate pull. Called by primary's push notification. Requires authentication.

#### `tdgl_streaming.sync.resolve_conflict` (Replica)
Resolves a Sync Conflict. Whitelisted.

- `conflict_name` — name of the Sync Conflict record
- `resolution` — "Accept Primary" or "Keep Local"

### Files

```
tdgl_streaming/
  hooks.py      — doc_events, scheduler, ignore_links_on_delete
  sync.py       — all sync logic (primary logging + replica apply)
  api.py        — whitelisted API endpoints
  tdgl_streaming/
    doctype/
      sync_config/          — Primary config (singleton)
      sync_config_doctypes/ — Child: tracked DocTypes
      sync_change_log/      — Change log entries
      sync_source/          — Replica config per primary
      sync_source_doctype/  — Child: DocTypes to pull + filters
      sync_log/             — Apply results
      sync_conflict/        — Conflict records
      sync_local_edit/      — Local edit tracking
```

### License

MIT
