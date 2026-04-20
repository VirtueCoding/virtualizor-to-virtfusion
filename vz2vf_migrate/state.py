import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from json import JSONDecodeError


VALID_STAGE_STATUS = {"done"}


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


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def _invalid(self, message: str) -> ValueError:
        return ValueError(f"Invalid migration state file {self.path}: {message}. Delete or repair the file and retry.")

    def _validate_stage_record(self, vm_id: str, stage: str, record: object) -> None:
        if not isinstance(record, dict):
            raise self._invalid(f"stage {stage!r} for VM {vm_id!r} must be an object")
        status = record.get("status")
        data = record.get("data")
        if status not in VALID_STAGE_STATUS:
            raise self._invalid(
                f"stage {stage!r} for VM {vm_id!r} has unsupported status {status!r}"
            )
        if not isinstance(data, dict):
            raise self._invalid(f"stage {stage!r} for VM {vm_id!r} must include object data")
        self._validate_stage_data(vm_id, stage, data)

    def _require_keys(self, vm_id: str, stage: str, data: dict, keys: tuple[str, ...]) -> None:
        missing = [key for key in keys if key not in data]
        if missing:
            raise self._invalid(
                f"stage {stage!r} for VM {vm_id!r} is missing required data keys: {', '.join(missing)}"
            )

    def _validate_stage_data(self, vm_id: str, stage: str, data: dict) -> None:
        if stage == "resolve_whmcs":
            self._require_keys(
                vm_id,
                stage,
                data,
                ("source_name", "client_id", "firstname", "lastname", "email", "product_id", "product_name", "hosting_id", "domain"),
            )
            return
        if stage == "resolve_user":
            self._require_keys(vm_id, stage, data, ("user_id",))
            return
        if stage == "create_server":
            self._require_keys(vm_id, stage, data, ("server_id", "uuid", "storage_names"))
            return
        if stage == "build_server":
            self._require_keys(vm_id, stage, data, ("server_id",))
            return
        if stage == "preserve_hostname":
            self._require_keys(vm_id, stage, data, ("name",))
            return
        if stage == "shutdown_source":
            self._require_keys(vm_id, stage, data, ("commands", "final_state"))
            if data.get("final_state") != "shut off":
                raise self._invalid(f"stage {stage!r} for VM {vm_id!r} must record final_state 'shut off'")
            return
        if stage == "copy_disks":
            self._require_keys(vm_id, stage, data, ("commands", "storage_names"))
            return
        if stage == "inject_guest":
            self._require_keys(vm_id, stage, data, ("command", "disk_path"))
            return
        if stage == "preserve_ipv6":
            self._require_keys(vm_id, stage, data, ("server_id", "addresses"))
            return
        if stage == "boot_target":
            self._require_keys(vm_id, stage, data, ("server_id",))
            return
        if stage == "rewrite_whmcs":
            self._require_keys(vm_id, stage, data, ("hosting_id", "server_id", "row_counts"))
            return
        if stage not in {"add_ipv4"}:
            raise self._invalid(f"stage {stage!r} for VM {vm_id!r} is not recognized")

    def _validate_payload(self, payload: object) -> dict:
        if not isinstance(payload, dict):
            raise self._invalid("top-level payload must be an object")
        for vm_id, vm_state in payload.items():
            if not isinstance(vm_state, dict):
                raise self._invalid(f"VM {vm_id!r} state must be an object")
            stages = vm_state.get("stages")
            if not isinstance(stages, dict):
                raise self._invalid(f"VM {vm_id!r} must include a stages object")
            for stage, record in stages.items():
                self._validate_stage_record(str(vm_id), stage, record)
        return payload

    def _read(self) -> dict:
        if not self.path.exists():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except JSONDecodeError as exc:
            raise self._invalid(str(exc)) from exc
        return self._validate_payload(payload)

    def read(self) -> dict:
        return self._read()

    def completed_stages(self, vpsid: int | str) -> dict[str, dict]:
        payload = self._read()
        vm_state = payload.get(str(vpsid), {})
        return dict(vm_state.get("stages", {}))

    def mark_stage(self, vpsid: str, stage: str, status: str, data: dict) -> None:
        payload = self._read()
        vm_state = payload.setdefault(vpsid, {"stages": {}})
        vm_state["stages"][stage] = {"status": status, "data": data}
        _write_json_atomically(self.path, payload)
