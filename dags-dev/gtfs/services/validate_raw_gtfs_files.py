import csv
import logging
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)
DEFAULT_MIN_LINES = 2


def validate_raw_gtfs_files(
    config: Dict[str, Any],
    files_list: Optional[List[str]],
    min_lines: int = DEFAULT_MIN_LINES,
    read_file_fn: Callable[..., Any] = open,
) -> Dict[str, Any]:
    """Validate extracted GTFS text files before writing to raw storage.

    Rules per file:
    - file exists and is readable
    - file is CSV-parseable
    - file has at least min_lines rows

    Returns:
        {
          "is_valid": bool,
          "errors_by_file": {"file.txt": ["..."]},
          "validated_files_count": int
        }
    """

    def get_config(cfg):
        extraction = cfg["general"]["extraction"]
        return extraction["local_downloads_folder"]

    folder = get_config(config)
    errors_by_file = {}
    for file_name in files_list or []:
        local_file_path = f"{folder}/{file_name}"
        file_errors = []
        try:
            with read_file_fn(local_file_path, "r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)
                line_count = 0
                for _ in reader:
                    line_count += 1
        except FileNotFoundError:
            logger.error(
                "RAW GTFS VALIDATION - file_not_found: file='%s', folder='%s'",
                file_name,
                folder,
            )
            file_errors.append("file_not_found")
            errors_by_file[file_name] = file_errors
            continue
        except PermissionError:
            logger.error(
                "RAW GTFS VALIDATION - file_not_readable: file='%s', folder='%s'",
                file_name,
                folder,
            )
            file_errors.append("file_not_readable")
            errors_by_file[file_name] = file_errors
            continue
        except UnicodeDecodeError:
            logger.error(
                "RAW GTFS VALIDATION - invalid_encoding_utf8: file='%s', folder='%s'",
                file_name,
                folder,
            )
            file_errors.append("invalid_encoding_utf8")
            errors_by_file[file_name] = file_errors
            continue
        except csv.Error as e:
            logger.error(
                "RAW GTFS VALIDATION - invalid_csv: file='%s', folder='%s', error='%s'",
                file_name,
                folder,
                e,
            )
            file_errors.append(f"invalid_csv:{e}")
            errors_by_file[file_name] = file_errors
            continue
        except Exception as e:
            logger.error(
                "RAW GTFS VALIDATION - unexpected_validation_error: file='%s', folder='%s', error='%s'",
                file_name,
                folder,
                e,
            )
            file_errors.append(f"unexpected_validation_error:{e}")
            errors_by_file[file_name] = file_errors
            continue
        if line_count < min_lines:
            file_errors.append(
                f"insufficient_lines:expected_at_least_{min_lines}:found_{line_count}"
            )
        if file_errors:
            errors_by_file[file_name] = file_errors
    return {
        "is_valid": len(errors_by_file) == 0,
        "errors_by_file": errors_by_file,
        "validated_files_count": len(files_list or []),
    }
