# Azure Functions JobSpy starter

This project runs a daily Azure Functions timer job that:
- scrapes LinkedIn + Indeed
- keeps Cork graduate-style roles
- writes CSV files under `/tmp/jobspy-output`
- emails the CSV files to Gmail

## Required Azure app settings
Add these in **Function App > Settings > Environment variables**:
- `GMAIL_SENDER`
- `GMAIL_APP_PASSWORD`
- `GMAIL_RECIPIENT`
- `JOBSPY_OUTPUT_DIR=/tmp/jobspy-output`

## Schedule
Current cron in `function_app.py`:
- `17 7 * * *`

That means **07:17 UTC every day**.

## GitHub deployment secret
Add this repo secret:
- `AZURE_FUNCTIONAPP_PUBLISH_PROFILE`

Value: contents of the publish profile downloaded from your Azure Function App.
