import logging
import azure.functions as func

from job_logic import run_pipeline

app = func.FunctionApp()

# Azure Functions 的定时表达式是 6 段：
# second minute hour day month day-of-week
# 下面这个表示：每天 UTC 08:10 跑一次
@app.timer_trigger(schedule="0 10 8 * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def daily_jobspy_runner(mytimer: func.TimerRequest) -> None:
    logging.info("JobSpy timer trigger started.")
    try:
        result = run_pipeline()
        logging.info("JobSpy run finished: %s", result)
    except Exception as e:
        logging.exception("JobSpy run failed: %s", e)
        raise
