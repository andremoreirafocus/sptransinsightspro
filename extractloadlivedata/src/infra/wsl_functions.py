import subprocess
import platform
import logging

logger = logging.getLogger(__name__)


def is_wsl():
    # Method 1: Check the kernel release string
    if "microsoft" in platform.release().lower():
        return True

    # Method 2: Check /proc/version for the 'Microsoft' signature
    try:
        with open("/proc/version", "r") as f:
            if "microsoft" in f.read().lower():
                return True
    except FileNotFoundError:
        pass

    return False


def is_disk_space_ok_wsl(threshold=95):
    # 1. Define threshold via param
    # 2. Define the command you want to run
    command = "df -kh | grep /mnt/c | tr -s ' '| cut -f5 -d' ' | cut -f1 -d'%'"
    try:
        # 3. Run the command and capture the output
        # 'capture_output=True' grabs stdout and stderr
        # 'text=True' returns the output as a string instead of bytes
        result = subprocess.run(
            command, capture_output=True, text=True, shell=True, check=True
        )
        # 4. Clean and convert the output
        # .strip() removes whitespace/newlines
        output_value = float(result.stdout.strip())
        # 5. Perform the comparison
        disk_space_ok = output_value < threshold
        if disk_space_ok:
            logger.info(
                f"Success: Disk usage in C drive ({output_value}) is smaller than threshold ({threshold})."
            )
        else:
            logger.error(
                f"CRITICAL: Disk usage in C drive ({output_value}) is greater or equal than threshold ({threshold})."
            )
        return disk_space_ok

    except subprocess.CalledProcessError as e:
        logger.error(f"Error: Command failed with return code {e.returncode}")
        return True  # let dependable process run
    except ValueError:
        logger.error(
            f"Error: Could not convert command output '{result.stdout.strip()}' to a number."
        )
        return True  # let dependable process run


if __name__ == "__main__":
    is_disk_space_ok_wsl()
