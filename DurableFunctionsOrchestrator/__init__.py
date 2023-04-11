import logging
import json
import datetime
import azure.functions as func
import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    # Define the timeout duration as one minute from the current time
    timeout = context.current_utc_datetime + datetime.timedelta(minutes=1)

    # Call the HistoricalDB activity function and wait for a result
    historical_db_task = context.call_activity('dbTEST','SHAAAJAALAALA')

    # Set a timer to check for the HistoricalDB activity function result after one minute
    timeout_task = context.create_timer(timeout)

    # Wait for either the HistoricalDB activity function result or the timeout
    historical_db_result, timeout_result = yield context.task_any([historical_db_task, timeout_task])

    # If the timeout task completed first, raise an exception
    if timeout_result:
        raise Exception("Orchestration timed out.")

    # Call the Hello activity functions and wait for their results
    hello_seattle_task = context.call_activity('Hello', "Seattle")
    hello_london_task = context.call_activity('Hello', "London")
    hello_results = yield context.task_all([hello_seattle_task, hello_london_task])

    # Combine the HistoricalDB and Hello activity function results and return them
    return [historical_db_result, hello_results[0], hello_results[1]]

# Define the entry point for the Durable Function
main = df.Orchestrator.create(orchestrator_function)