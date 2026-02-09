from . import __version__ as app_version

app_name = "tdgl_streaming"
app_title = "TDGL Streaming"
app_publisher = "Percival Rapha"
app_description = "Streaming service for TDGL instances"
app_email = "percival.rapha@gmail.com"
app_license = "MIT"

# Document Events
doc_events = {
	"*": {
		"after_insert": "tdgl_streaming.sync.log_change",
		"on_update": [
			"tdgl_streaming.sync.log_change",
			"tdgl_streaming.sync.track_local_edit",
		],
		"on_submit": "tdgl_streaming.sync.log_change",
		"on_cancel": "tdgl_streaming.sync.log_change",
		"on_trash": "tdgl_streaming.sync.log_change",
		"on_update_after_submit": "tdgl_streaming.sync.log_change",
		"after_rename": "tdgl_streaming.sync.log_rename",
	}
}

# Scheduled Tasks
scheduler_events = {
	"cron": {
		"* * * * *": ["tdgl_streaming.sync.scheduled_pull"]
	}
}

# Ignore links to sync DocTypes when deleting documents
ignore_links_on_delete = ["Sync Change Log", "Sync Log", "Sync Local Edit"]
