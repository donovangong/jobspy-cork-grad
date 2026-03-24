import logging
import azure.functions as func
from job_logic import run_once

app = func.FunctionApp()


@app.function_name(name="jobspy_daily_timer")
@app.schedule(
    schedule="17 7 * * *",
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True,
)
def jobspy_daily_timer(mytimer: func.TimerRequest) -> None:
    if mytimer.past_due:
        logging.warning("The timer is past due.")

    result = run_once()
    logging.info("JobSpy run result: %s", result)
