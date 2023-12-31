"""
This script monitors a file that should be recording outputs from a slurm job.
If the file has not been updated in a certain amount of time, the script will
kill the corresponding slurm job.

Procedure:
1. When the slurm job is launched, it should call this script with a special flag that
will run this script once and then exit. This registers information about the job to the
script in the form of a json file that records the job id and the path to the file that
was submitted to slurm. The arguments to this script should be the path to the output
file and the path to the sbatch file that was submitted to slurm.
2. In a separate process, this script should be run in a loop. It will check the
directory for json files that record information about jobs. For each job, it will check
the last modified time of the output file. If the output file matches either of the
following criteria, this script must kill the corresponding slurm job and resubmit it:
- The output file has not been modified in a certain amount of time.
- The output file contains the string "srun: error" or "Error" in it.
"""
import argparse
import glob
import json
import os
import subprocess
import time

JOB_REGISTRATION_DIR = "/coc/testnvme/nyokoyama3/repos/slurm_utils/job_registration"
STALE_TIME = 30 * 60  # 30 minutes


def register_job(job_id: int, output_file: str, sbatch_file: str) -> None:
    job_registration = {
        "job_id": job_id,
        "output_file": os.path.abspath(output_file),
        "sbatch_file": os.path.abspath(sbatch_file),
    }
    with open(f"{JOB_REGISTRATION_DIR}/{job_id}.json", "w") as f:
        json.dump(job_registration, f)


def job_has_error(job_id: int) -> bool:
    # Check if the content of the log file contains the string "srun: error" or "Error"
    content = get_current_output_file_contents(job_id)
    error_occurred = False
    for substr in ["srun: error", "Error"]:
        if substr in content:
            # To avoid looping, replace these strings with something else (their .upper())
            # equivalent
            content = content.replace(substr, substr.upper())
            # Write the content back to the file
            with open(f"{JOB_REGISTRATION_DIR}/{job_id}.json", "r") as f:
                job_registration = json.load(f)
            with open(job_registration["output_file"], "w") as f:
                f.write(content)
            error_occurred = True
    return error_occurred


def job_has_stalled(job_id: int, file_contents_memory: dict) -> bool:
    return time.time() - file_contents_memory[job_id]["last_modified"] > STALE_TIME


def update_memory(file_contents_memory: dict) -> dict:
    """
    file_contents_memory is a dictionary mapping job_ids to a dict containing the last
    seen contents of the output file and the last time the output file was modified.

    For each job, update the memory to reflect the current contents of the output file.
    If the job_id is not in the given memory, add it. If it is, only update it if the
    current contents of the output file are different from the contents in memory; if
    they are, then update the contents and the last modified time.

    Remove any keys that are not in the job registration directory.
    """
    jsons = glob.glob(f"{JOB_REGISTRATION_DIR}/*.json")
    job_ids = [int(j.split("/")[-1].split(".")[0]) for j in jsons]
    new_memory = {}
    for job_id in job_ids:
        content = get_current_output_file_contents(job_id)
        if job_id not in file_contents_memory:
            new_memory[job_id] = {
                "content": content,
                "last_modified": time.time(),
            }
        else:
            if content != file_contents_memory[job_id]["content"]:
                new_memory[job_id] = {
                    "content": content,
                    "last_modified": time.time(),
                }
            else:
                new_memory[job_id] = file_contents_memory[job_id]
    return new_memory


def get_current_output_file_contents(job_id: int) -> str:
    try:
        with open(f"{JOB_REGISTRATION_DIR}/{job_id}.json", "r") as f:
            job_registration = json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        print(f"Error reading job registration for job {job_id}: {e}")
        return ""

    try:
        with open(job_registration["output_file"], "r") as f:
            content = f.read()
        return content
    except IOError as e:
        print(f"Error reading output file for job {job_id}: {e}")
        return ""


def poll_jobs(file_contents_memory: dict, first: bool) -> None:
    file_contents_memory = update_memory(file_contents_memory)
    # Check if any jobs have died
    try:
        job_files = os.listdir(JOB_REGISTRATION_DIR)
    except IOError as e:
        print(f"Error accessing job registration directory: {e}")
        return

    for job_json in job_files:
        job_id = int(job_json.split(".")[0])
        if job_has_error(job_id):
            job_is_dead = not first
            if job_is_dead:
                print(f"Job {job_id} has errored. Cancelling...")
        elif job_has_stalled(job_id, file_contents_memory):
            print(f"Job {job_id} has stalled. Cancelling...")
            job_is_dead = True
        else:
            job_is_dead = False

        if job_is_dead:
            try:
                with open(f"{JOB_REGISTRATION_DIR}/{job_id}.json", "r") as f:
                    job_registration = json.load(f)
                subprocess.run(["scancel", str(job_id)])

                print(f"Job {job_id} has been cancelled. Resubmitting...")
                sbatch_dir = os.path.dirname(job_registration["sbatch_file"])
                subprocess.run(
                    ["sbatch", job_registration["sbatch_file"]], cwd=sbatch_dir
                )

                os.remove(f"{JOB_REGISTRATION_DIR}/{job_id}.json")
            except (IOError, subprocess.SubprocessError) as e:
                print(f"Error handling job {job_id}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--register-job", action="store_true")
    parser.add_argument("--job_id", type=int)
    parser.add_argument("--sbatch_file", type=str)
    parser.add_argument("--output_file", type=str, default="")
    args = parser.parse_args()

    if args.register_job:
        # Assert that the appropriate arguments were passed
        assert args.job_id is not None
        assert args.sbatch_file is not None

        if args.output_file == "":
            # If the output file was not found, search for it in the same directory as
            # the sbatch file
            parent_dir = os.path.dirname(args.sbatch_file)
            out_files = glob.glob(f"{parent_dir}/*.out")
            assert len(out_files) == 1
            args.output_file = out_files[0]

        register_job(args.job_id, args.output_file, args.sbatch_file)
    else:
        file_contents_memory = {}
        first = True
        while True:
            poll_jobs(file_contents_memory, first)
            time.sleep(30)
            first = False
