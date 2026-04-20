from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

VF_MASTER_DEFAULT_PHP_BIN = "/opt/virtfusion/php/bin/php"


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    name: str
    user: str
    password: str = field(repr=False)


@dataclass(frozen=True)
class VirtFusionMasterConfig:
    host: str
    port: int
    user: str
    helper_path: str
    php_bin: str = VF_MASTER_DEFAULT_PHP_BIN
    ssh_bin: str = "ssh"


@dataclass(frozen=True)
class MigrationConfig:
    vf_api_url: str
    vf_api_token: str = field(repr=False)
    vf_hypervisor_id: int
    vf_package_id: int
    vf_storage_profile_id: int | None
    vf_network_profile_id: int | None
    vf_additional_storage1_profile_id: int | None
    vf_additional_storage2_profile_id: int | None
    vf_os_template_id: int
    vz_mysql_bin: str
    whmcs_mysql_bin: str
    vz_db: DatabaseConfig
    whmcs_a_db: DatabaseConfig
    whmcs_b_db: DatabaseConfig
    whmcs_product_map: Mapping[int, int]
    whmcs_a_target_server_id: int | None
    whmcs_b_target_server_id: int | None
    ceph_username: str
    ceph_keyring: str
    vf_master: VirtFusionMasterConfig | None
    state_path: Path
    logs_dir: Path


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _optional_int(values: Mapping[str, str], key: str) -> int | None:
    raw = values.get(key)
    if raw in (None, ""):
        return None
    return int(raw)


def _database(values: Mapping[str, str], prefix: str) -> DatabaseConfig:
    return DatabaseConfig(
        host=values[f"{prefix}_HOST"],
        port=int(values[f"{prefix}_PORT"]),
        name=values[f"{prefix}_NAME"],
        user=values[f"{prefix}_USER"],
        password=values[f"{prefix}_PASS"],
    )


def _virtfusion_master(values: Mapping[str, str]) -> VirtFusionMasterConfig | None:
    host = values.get("VF_MASTER_HOST", "").strip()
    if not host:
        return None

    helper_path = values.get("VF_MASTER_HELPER_PATH", "").strip()
    if not helper_path:
        raise ValueError("VF_MASTER_HELPER_PATH is required when VF_MASTER_HOST is set")

    port = int(values.get("VF_MASTER_PORT") or "22")
    user = values.get("VF_MASTER_USER") or "root"
    php_bin = values.get("VF_MASTER_PHP_BIN") or VF_MASTER_DEFAULT_PHP_BIN
    ssh_bin = values.get("VF_MASTER_SSH_BIN") or "ssh"
    return VirtFusionMasterConfig(
        host=host,
        port=port,
        user=user,
        helper_path=helper_path,
        php_bin=php_bin,
        ssh_bin=ssh_bin,
    )


def load_whmcs_product_map(path: Path) -> dict[int, int]:
    if not path.exists():
        raise ValueError(f"WHMCS product map file not found: {path}")

    mapping: dict[int, int] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.count("=") != 1:
            raise ValueError(f"Invalid WHMCS product map line {line_number}: expected old_id=new_id")
        source_raw, target_raw = [part.strip() for part in line.split("=", 1)]
        if not source_raw or not target_raw:
            raise ValueError(f"Invalid WHMCS product map line {line_number}: expected old_id=new_id")
        try:
            source_id = int(source_raw)
        except ValueError:
            raise ValueError(
                f"Invalid WHMCS product map line {line_number}: source ID '{source_raw}' is not a valid integer"
            )
        try:
            target_id = int(target_raw)
        except ValueError:
            raise ValueError(
                f"Invalid WHMCS product map line {line_number}: target ID '{target_raw}' is not a valid integer"
            )
        if source_id in mapping:
            raise ValueError(f"Duplicate WHMCS product map source ID {source_id} on line {line_number}")
        mapping[source_id] = target_id
    if not mapping:
        raise ValueError(f"WHMCS product map file is empty: {path}")
    return mapping


def build_config(
    values: Mapping[str, str],
    cwd: Path,
    require_whmcs_targets: bool = True,
) -> MigrationConfig:
    whmcs_a_target_server_id = _optional_int(values, "WHMCS_A_TARGET_SERVER_ID")
    whmcs_b_target_server_id = _optional_int(values, "WHMCS_B_TARGET_SERVER_ID")
    map_path = Path(values.get("WHMCS_PRODUCT_MAP_FILE") or "whmcsmap.conf")
    if not map_path.is_absolute():
        map_path = cwd / map_path
    whmcs_product_map = load_whmcs_product_map(map_path) if require_whmcs_targets else {}
    if require_whmcs_targets and whmcs_a_target_server_id is None:
        raise ValueError("WHMCS A target server ID is required when --whmcs is used")
    if require_whmcs_targets and whmcs_b_target_server_id is None:
        raise ValueError("WHMCS B target server ID is required when --whmcs is used")

    return MigrationConfig(
        vf_api_url=values["VF_API_URL"],
        vf_api_token=values["VF_API_TOKEN"],
        vf_hypervisor_id=int(values["VF_HYPERVISOR_ID"]),
        vf_package_id=int(values.get("VF_PACKAGE_ID", "1")),
        vf_storage_profile_id=_optional_int(values, "VF_STORAGE_PROFILE_ID"),
        vf_network_profile_id=_optional_int(values, "VF_NETWORK_PROFILE_ID"),
        vf_additional_storage1_profile_id=_optional_int(values, "VF_ADDITIONAL_STORAGE1_PROFILE_ID"),
        vf_additional_storage2_profile_id=_optional_int(values, "VF_ADDITIONAL_STORAGE2_PROFILE_ID"),
        vf_os_template_id=int(values["VF_OS_TEMPLATE_ID"]),
        vz_mysql_bin=values.get("VZ_MYSQL_BIN", "/usr/local/emps/bin/mysql"),
        whmcs_mysql_bin=values.get("WHMCS_MYSQL_BIN", "mysql"),
        vz_db=_database(values, "VZ_DB"),
        whmcs_a_db=_database(values, "WHMCS_A_DB"),
        whmcs_b_db=_database(values, "WHMCS_B_DB"),
        whmcs_product_map=whmcs_product_map,
        whmcs_a_target_server_id=whmcs_a_target_server_id,
        whmcs_b_target_server_id=whmcs_b_target_server_id,
        ceph_username=values["CEPH_USERNAME"],
        ceph_keyring=values.get("CEPH_KEYRING") or f"/etc/ceph/ceph.client.{values['CEPH_USERNAME']}.keyring",
        vf_master=_virtfusion_master(values),
        state_path=cwd / "migration_state.json",
        logs_dir=cwd / "logs",
    )
