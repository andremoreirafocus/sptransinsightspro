import csv
from typing import Any, Callable, Dict, List, Optional

from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)
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
    structured_logger.info(
        event="raw_validation_started",
        message="Validating raw GTFS files",
        metadata={"file_count": len(files_list or [])},
    )
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
            structured_logger.error(
                event="raw_file_validation_error",
                message=f"File not found: '{file_name}'",
                metadata={"file_name": file_name, "error_type": "file_not_found", "folder": folder},
            )
            file_errors.append("file_not_found")
            errors_by_file[file_name] = file_errors
            continue
        except PermissionError:
            structured_logger.error(
                event="raw_file_validation_error",
                message=f"File not readable: '{file_name}'",
                metadata={"file_name": file_name, "error_type": "file_not_readable", "folder": folder},
            )
            file_errors.append("file_not_readable")
            errors_by_file[file_name] = file_errors
            continue
        except UnicodeDecodeError:
            structured_logger.error(
                event="raw_file_validation_error",
                message=f"Invalid UTF-8 encoding: '{file_name}'",
                metadata={"file_name": file_name, "error_type": "invalid_encoding_utf8", "folder": folder},
            )
            file_errors.append("invalid_encoding_utf8")
            errors_by_file[file_name] = file_errors
            continue
        except csv.Error as e:
            structured_logger.error(
                event="raw_file_validation_error",
                message=f"Invalid CSV: '{file_name}'",
                metadata={"file_name": file_name, "error_type": "invalid_csv", "folder": folder},
            )
            file_errors.append(f"invalid_csv:{e}")
            errors_by_file[file_name] = file_errors
            continue
        except Exception as e:
            structured_logger.error(
                event="raw_file_validation_error",
                message=f"Unexpected validation error: '{file_name}'",
                metadata={"file_name": file_name, "error_type": "unexpected_validation_error", "folder": folder},
            )
            file_errors.append(f"unexpected_validation_error:{e}")
            errors_by_file[file_name] = file_errors
            continue
        if line_count < min_lines:
            structured_logger.error(
                event="raw_file_validation_error",
                message=f"Insufficient lines in '{file_name}'",
                metadata={
                    "file_name": file_name,
                    "error_type": "insufficient_lines",
                    "line_count": line_count,
                    "min_lines": min_lines,
                    "folder": folder,
                },
            )
            file_errors.append(
                f"insufficient_lines:expected_at_least_{min_lines}:found_{line_count}"
            )
        if file_errors:
            errors_by_file[file_name] = file_errors
    structured_logger.info(
        event="raw_validation_completed",
        message="Raw GTFS validation completed",
        metadata={
            "is_valid": len(errors_by_file) == 0,
            "validated_files_count": len(files_list or []),
            "error_count": len(errors_by_file),
        },
    )
    return {
        "is_valid": len(errors_by_file) == 0,
        "errors_by_file": errors_by_file,
        "validated_files_count": len(files_list or []),
    }
