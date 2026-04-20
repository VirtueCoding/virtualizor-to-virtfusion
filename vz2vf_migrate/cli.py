import argparse
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence

from vz2vf_migrate.config import build_config, load_env_file
from vz2vf_migrate.inventory import normalize_inventory_rows
from vz2vf_migrate.migrator import migrate_vm
from vz2vf_migrate.reporting import CsvReporter, FileLogger, VmJsonReporter
from vz2vf_migrate.runner import CommandRunner
from vz2vf_migrate.services import MigrationServices
from vz2vf_migrate.state import StateStore
from vz2vf_migrate.virtfusion import VirtFusionClient
from vz2vf_migrate.virtfusion_master import VirtFusionMasterClient
from vz2vf_migrate.virtualizor import VirtualizorRepository
from vz2vf_migrate.whmcs import WhmcsRepository


def parse_single_vps(value: str) -> list[int]:
    vps_ids: list[int] = []
    for segment in value.split(","):
        trimmed = segment.strip()
        if not trimmed:
            raise argparse.ArgumentTypeError("VPSID list must not contain empty segments")
        try:
            vps_ids.append(int(trimmed))
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"invalid VPSID: {trimmed!r}") from exc
    return vps_ids


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="vz2vf-migrate",
        description="Python CLI for migrating Virtualizor KVM VMs into VirtFusion on the same Ceph RBD pool.",
    )
    parser.add_argument("serid", type=int, help="Virtualizor source node ID")
    parser.add_argument("--execute", action="store_true", help="Perform real mutations")
    parser.add_argument(
        "--single-vps",
        type=parse_single_vps,
        dest="single_vps",
        help="Only migrate one or more comma-separated VPSIDs",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        dest="continue_on_error",
        help="Continue processing after a VPS-level failure",
    )
    parser.add_argument(
        "--skip-inject",
        action="store_true",
        dest="skip_inject",
        help=(
            "Skip guest package injection; by default, qemu-guest-agent is installed when "
            "missing and cloud-init is attempted when missing and available, with "
            "package-presence checks before installation."
        ),
    )
    parser.add_argument(
        "--skip-shutdown",
        action="store_true",
        dest="skip_shutdown",
        help="Skip shutting down the source VPS before migration",
    )
    parser.add_argument("--whmcs", action="store_true", help="Rewrite the existing WHMCS service")
    parser.add_argument("--env-file", default=".env", dest="env_file", help="Path to the env file")
    return parser


def run_selected_vms(
    vms: list[dict],
    migrate_one: Callable[[dict], dict],
    continue_on_error: bool,
) -> list[dict]:
    results: list[dict] = []
    for vm in vms:
        try:
            results.append(migrate_one(vm))
        except Exception as exc:
            results.append({"vpsid": vm.get("vpsid"), "status": "failed", "notes": str(exc)})
            if not continue_on_error:
                break
    return results


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _result_row(result, execute: bool = True) -> dict[str, str]:
    payload = asdict(result) if is_dataclass(result) else dict(result)
    row = {key: "" if value is None else str(value) for key, value in payload.items()}
    if not execute and row.get("status") == "complete":
        row["status"] = "planned"
        if row.get("notes") in {"", "migration finished"}:
            row["notes"] = "dry-run"
    return row


def _failed_row(vm, error: Exception) -> dict[str, str]:
    ip_addresses = [vm.primary_ipv4 or "", *vm.additional_ipv4, *vm.ipv6_addresses]
    return {
        "vpsid": str(vm.vpsid),
        "vps_name": vm.vps_name,
        "hostname": vm.hostname,
        "old_rbd_image": vm.disks[0].rbd_name,
        "new_vf_server_id": "",
        "new_rbd_image": "",
        "ip_addresses": ",".join(part for part in ip_addresses if part),
        "status": "failed",
        "notes": str(error),
    }


def _stage_completed(completed_stages: dict[str, dict], stage: str) -> bool:
    value = completed_stages.get(stage)
    if isinstance(value, dict):
        return value.get("status") == "done"
    return value is not None


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    values = load_env_file(Path(args.env_file))
    config = build_config(values, Path.cwd(), require_whmcs_targets=args.whmcs)

    run_id = _run_id()
    run_log = FileLogger(config.logs_dir / run_id / "run.log")
    runner = CommandRunner(execute=args.execute, logger=run_log)
    state = StateStore(config.state_path)
    csv_reporter = CsvReporter(Path.cwd() / f"migration-summary-{args.serid}-{run_id}.csv")
    vm_reporter = VmJsonReporter(config.logs_dir / run_id)
    virtualizor_repo = VirtualizorRepository(runner, config.vz_mysql_bin, config.vz_db)
    whmcs_repo = WhmcsRepository(runner, config.whmcs_mysql_bin, config.whmcs_a_db, config.whmcs_b_db)
    virtfusion_client = VirtFusionClient(config.vf_api_url, config.vf_api_token)
    vf_master_client = VirtFusionMasterClient(runner, config.vf_master) if config.vf_master is not None else None
    services = MigrationServices(
        runner=runner,
        whmcs_repo=whmcs_repo,
        virtfusion_client=virtfusion_client,
        vf_master_client=vf_master_client,
        logger=run_log,
        state=state,
        vm_reporter=vm_reporter,
        execute=args.execute,
        enable_whmcs=args.whmcs,
        skip_shutdown=args.skip_shutdown,
        skip_inject=args.skip_inject,
        vf_hypervisor_id=config.vf_hypervisor_id,
        vf_package_id=config.vf_package_id,
        vf_storage_profile_id=config.vf_storage_profile_id,
        vf_network_profile_id=config.vf_network_profile_id,
        vf_additional_storage1_profile_id=config.vf_additional_storage1_profile_id,
        vf_additional_storage2_profile_id=config.vf_additional_storage2_profile_id,
        vf_os_template_id=config.vf_os_template_id,
        whmcs_mysql_bin=config.whmcs_mysql_bin,
        whmcs_a_db=config.whmcs_a_db,
        whmcs_b_db=config.whmcs_b_db,
        whmcs_product_map=config.whmcs_product_map,
        whmcs_a_target_server_id=config.whmcs_a_target_server_id,
        whmcs_b_target_server_id=config.whmcs_b_target_server_id,
        ceph_username=config.ceph_username,
        ceph_keyring=config.ceph_keyring,
    )

    rows = virtualizor_repo.fetch_inventory_rows(args.serid, args.single_vps)
    vms = normalize_inventory_rows(rows, Path("/etc/libvirt/qemu"))
    if args.single_vps:
        matched_vpsids = {vm.vpsid for vm in vms}
        missing_vpsids = list(dict.fromkeys(vpsid for vpsid in args.single_vps if vpsid not in matched_vpsids))
        if missing_vpsids:
            missing_list = ", ".join(str(vpsid) for vpsid in missing_vpsids)
            raise RuntimeError(f"Missing requested VPSID(s): {missing_list}")
    if not vms:
        raise RuntimeError(f"No VMs matched serid={args.serid}")
    completed_stages_by_vm = {vm.vpsid: state.completed_stages(vm.vpsid) for vm in vms}
    if args.execute and config.vf_master is None and any(
        vm.ipv6_addresses and not _stage_completed(completed_stages_by_vm[vm.vpsid], "preserve_ipv6")
        for vm in vms
    ):
        raise ValueError("VirtFusion master config is required for execute-mode IPv6 migrations")

    saw_failure = False
    for vm in vms:
        services.begin_vm(vm)
        try:
            result = migrate_vm(
                vm,
                services,
                enable_whmcs=args.whmcs,
                completed_stages=completed_stages_by_vm[vm.vpsid],
            )
            row = _result_row(result, execute=args.execute)
            csv_reporter.write_row(row)
            services.finish_vm(row)
            if row["status"] == "failed":
                saw_failure = True
                if not args.continue_on_error:
                    break
        except Exception as exc:
            saw_failure = True
            row = _failed_row(vm, exc)
            csv_reporter.write_row(row)
            services.finish_vm(row, error=exc)
            if not args.continue_on_error:
                break

    return 1 if saw_failure else 0
