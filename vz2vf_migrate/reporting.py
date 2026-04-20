import csv
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    directory_fd = os.open(path, flags)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _ensure_directory(path: Path) -> None:
    if path.exists():
        return
    path.mkdir(parents=True, exist_ok=True)
    if path.parent != path:
        _fsync_directory(path.parent)


def _write_json_atomically(path: Path, payload: dict) -> None:
    _ensure_directory(path.parent)
    temp_path: Path | None = None
    try:
        with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
            temp_path = Path(handle.name)
            handle.write(json.dumps(payload, indent=2, sort_keys=True))
            handle.flush()
            os.fsync(handle.fileno())
        temp_path.replace(path)
        _fsync_directory(path.parent)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()


class FileLogger:
    def __init__(self, path: Path) -> None:
        self.path = path
        _ensure_directory(self.path.parent)

    def log(self, message: str) -> None:
        created = not self.path.exists()
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(message + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        if created:
            _fsync_directory(self.path.parent)


class CsvReporter:
    HEADERS = [
        "vpsid",
        "vps_name",
        "hostname",
        "old_rbd_image",
        "new_vf_server_id",
        "new_rbd_image",
        "ip_addresses",
        "status",
        "notes",
    ]

    def __init__(self, path: Path) -> None:
        self.path = path
        _ensure_directory(self.path.parent)
        if not self.path.exists():
            with self.path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=self.HEADERS)
                writer.writeheader()
                handle.flush()
                os.fsync(handle.fileno())
            _fsync_directory(self.path.parent)

    def write_row(self, row: dict[str, str]) -> None:
        with self.path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=self.HEADERS)
            writer.writerow(row)
            handle.flush()
            os.fsync(handle.fileno())


class VmJsonReporter:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        _ensure_directory(self.directory)

    def write(self, vpsid: int | str, payload: dict) -> None:
        _write_json_atomically(self.directory / f"vm-{vpsid}.json", payload)
