<# 
Convenience wrapper to refresh convention pages + JSON.
Sets PYTHONPATH so the job can import app.* without exporting env vars globally.
#>
$env:PYTHONPATH = "mdm_comics_backend"
& ".\mdm_comics_backend\.venv\Scripts\python.exe" "mdm_comics_backend\app\jobs\conventions_refresh.py"
