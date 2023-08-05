import concurrent.futures
import os
import pandas as pd
import time
import logging
import colorlog

# Sample functions to simulate API calls
def challenge(host):
    # Simulate API call and return jobid
    jobid = f"job_{host}"
    return jobid

def check_status(jobid):
    # Simulate API call to check status and return status
    status = f"status_{jobid}"
    return status

def process_host(host):
    jobid = challenge(host)
    return host, jobid

def get_status_info(host, jobid, check_status_func, logger):
    max_retries = 5
    retries = 0

    while retries < max_retries:
        status = check_status_func(jobid)
        logger.info(f"Checking status for Host: {host}, JobID: {jobid}, Status: {status}")

        if status == "completed":
            logger.info(f"Status check complete for Host: {host}, JobID: {jobid}, Status: {status}")
            return host, jobid, status

        retries += 1
        time.sleep(1)  # Adjust the sleep time as needed

    logger.warning(f"Status check timeout for Host: {host}, JobID: {jobid}, Status: {status}")
    return host, jobid, status


def main():
    # Sample list of hostnames
    hostnames = ['h1', 'h2', 'h3', 'h4', 'h5']

    # Determine the number of CPU cores available
    num_cores = os.cpu_count()

    # Calculate the maximum number of concurrent threads (slightly less than the number of cores)
    max_threads = max(num_cores - 1, 1)

    # Create a logger for host-level messages
    host_logger = colorlog.getLogger(f"HostLogger")
    host_logger.setLevel(logging.INFO)
    handler = colorlog.StreamHandler()
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s[%(levelname)s] %(asctime)s %(message)s%(reset)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        }
    )
    handler.setFormatter(formatter)
    host_logger.addHandler(handler)

    with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
        # Submit tasks to the executor to get jobids for all hosts
        future_to_host = {
            executor.submit(process_host, host): host for host in hostnames
        }

        # Initialize an empty list to store the job info
        job_info = []

        # Wait for all tasks to complete and get jobids
        for future in concurrent.futures.as_completed(future_to_host):
            host = future_to_host[future]
            try:
                host, jobid = future.result()
                # Append the job info as a tuple to the list
                job_info.append((host, jobid))
            except Exception as exc:
                host_logger.error(f"Host: {host}, Error occurred: {exc}")

        host_logger.info("Job IDs obtained for all hosts.")

        # Submit tasks to the executor to get status info for all hosts
        future_to_status = {
            executor.submit(get_status_info, host, jobid, check_status, host_logger): (host, jobid) for host, jobid in job_info
        }

        # Wait for all status info tasks to complete
        for future in concurrent.futures.as_completed(future_to_status):
            host, jobid = future_to_status[future]
            try:
                host, jobid, status = future.result()
                # Update the job_info list with status
                job_info[job_info.index((host, jobid))] = (host, jobid, status)
            except Exception as exc:
                host_logger.error(f"Host: {host}, Error occurred: {exc}")

    # Create a pandas DataFrame from the job_info list
    df = pd.DataFrame(job_info, columns=['Host', 'JobID', 'Status'])

    # Export the DataFrame to Excel
    excel_filename = 'job_info.xlsx'
    df.to_excel(excel_filename, index=False)

    host_logger.info(f"Job information exported to {excel_filename} successfully.")

main()
