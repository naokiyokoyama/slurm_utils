import subprocess
import time

from slurm_utils.monitor import SlurmMonitor


def main(max_jobs):
    cooldown = 60 * 2  # 2 minutes
    monitor = SlurmMonitor(cooldown)

    while True:
        refreshed = monitor.refresh(silent=True)

        if not refreshed:
            time.sleep(10)
            continue

        # Get all job_ids that have an ST of R
        running_job_ids = monitor.latest_info.loc[
            monitor.latest_info["ST"] == "R", "JOBID"
        ].tolist()

        # Count how many jobs are running
        num_running_jobs = len(running_job_ids)
        num_to_submit = max_jobs - num_running_jobs
        if num_to_submit <= 0:
            print("No jobs to submit")
            time.sleep(10)
            continue

        # Get all jobids that have a NODELIST(REASON) of (JobHeldUser)
        jobids = monitor.latest_info.loc[
            monitor.latest_info["NODELIST(REASON)"] == "(JobHeldUser)", "JOBID"
        ].tolist()

        # Submit the first num_to_submit jobs
        num_jobs_available = len(jobids[:num_to_submit])
        if num_jobs_available > 0:
            print(f"Submitting {num_jobs_available} jobs")
            print(jobids[:num_to_submit])
            for jobid in jobids[:num_to_submit]:
                subprocess.run(f"scontrol release {jobid}", shell=True)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("max_jobs", type=int, default=100)
    args = parser.parse_args()
    main(args.max_jobs)
