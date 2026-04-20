from __future__ import annotations

import json
import re
import time
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Callable

from vz2vf_migrate.host_ops import (
    build_domstate_command,
    build_guest_inject_command,
    build_rbd_copy_commands,
    build_shutdown_commands,
    should_skip_guest_injection,
)
from vz2vf_migrate.virtfusion import build_server_payload, parse_response_data_id, parse_server_details
from vz2vf_migrate.virtfusion_master import build_preserve_ipv6_payload
from vz2vf_migrate.whmcs_rewrite import (
    build_assignedips,
    build_mod_virtfusion_direct_verify_query,
    build_mysql_transaction_script,
    build_mod_virtfusion_direct_insert_query,
    build_mod_virtfusion_direct_update_query,
    build_tblhosting_update_query,
    build_tblhosting_verify_query,
    parse_mysql_transaction_result,
)

SHUTDOWN_COMMAND_TIMEOUT_SECONDS = 30
SHUTDOWN_TIMEOUT_SECONDS = 300
SHUTDOWN_POLL_INTERVAL_SECONDS = 5
TARGET_SHUTDOWN_TIMEOUT_SECONDS = 120
TARGET_POWEROFF_TIMEOUT_SECONDS = 120
TARGET_SHUTDOWN_POLL_INTERVAL_SECONDS = 5
DOMSTATE_COMMAND_TIMEOUT_SECONDS = 10
RBD_COPY_TIMEOUT_SECONDS = 24 * 60 * 60
RBD_IMAGE_TIMEOUT_SECONDS = 300
RBD_IMAGE_POLL_INTERVAL_SECONDS = 5
RBD_IMAGE_COMMAND_TIMEOUT_SECONDS = 10
BUILD_SERVER_TIMEOUT_SECONDS = 300
BUILD_SERVER_POLL_INTERVAL_SECONDS = 5
GUEST_INJECT_TIMEOUT_SECONDS = 30 * 60
WHMCS_REWRITE_TIMEOUT_SECONDS = 30


class MigrationServices:
    def __init__(
        self,
        runner,
        whmcs_repo,
        virtfusion_client,
        vf_master_client,
        logger,
        state,
        vm_reporter,
        execute,
        enable_whmcs,
        skip_shutdown,
        skip_inject,
        vf_hypervisor_id,
        vf_package_id,
        vf_storage_profile_id,
        vf_network_profile_id,
        vf_additional_storage1_profile_id,
        vf_additional_storage2_profile_id,
        vf_os_template_id,
        whmcs_mysql_bin,
        whmcs_a_db,
        whmcs_b_db,
        whmcs_product_map,
        whmcs_a_target_server_id,
        whmcs_b_target_server_id,
        ceph_username,
        ceph_keyring,
    ) -> None:
        self.runner = runner
        self.whmcs_repo = whmcs_repo
        self.virtfusion_client = virtfusion_client
        self.vf_master_client = vf_master_client
        self.logger = logger
        self.state = state
        self.vm_reporter = vm_reporter
        self.execute = execute
        self.enable_whmcs = enable_whmcs
        self.skip_shutdown = skip_shutdown
        self.skip_inject = skip_inject
        self.vf_hypervisor_id = vf_hypervisor_id
        self.vf_package_id = vf_package_id
        self.vf_storage_profile_id = vf_storage_profile_id
        self.vf_network_profile_id = vf_network_profile_id
        self.vf_additional_storage1_profile_id = vf_additional_storage1_profile_id
        self.vf_additional_storage2_profile_id = vf_additional_storage2_profile_id
        self.vf_os_template_id = vf_os_template_id
        self.whmcs_mysql_bin = whmcs_mysql_bin
        self.whmcs_a_db = whmcs_a_db
        self.whmcs_b_db = whmcs_b_db
        self.whmcs_product_map = dict(whmcs_product_map)
        self.whmcs_a_target_server_id = whmcs_a_target_server_id
        self.whmcs_b_target_server_id = whmcs_b_target_server_id
        self.ceph_username = ceph_username
        self.ceph_keyring = ceph_keyring
        self._vm = None
        self._artifact: dict[str, Any] | None = None

    def begin_vm(self, vm) -> None:
        notes: list[str] = []
        if vm.ipv6_addresses:
            notes.append("IPv6 preservation planned via VirtFusion master helper before target boot")
        self._vm = vm
        self._artifact = {
            "source_vm": asdict(vm),
            "stages": {},
            "notes": notes,
            "result": None,
            "error": None,
            "whmcs_match": None,
            "virtfusion": {},
            "started_at": self._now(),
            "finished_at": None,
        }
        self._write_artifact()

    def finish_vm(self, result: dict | None = None, error: Exception | None = None) -> None:
        if self._vm is None or self._artifact is None:
            return
        if result is not None:
            self._artifact["result"] = result
        if error is not None:
            self._artifact["error"] = {
                "type": type(error).__name__,
                "message": str(error),
                "stage": getattr(error, "_migration_stage", None),
            }
        self._artifact["finished_at"] = self._now()
        self._write_artifact()
        self._vm = None
        self._artifact = None

    def resolve_whmcs(self, vm):
        def action() -> Any:
            match = self.whmcs_repo.find_service(vm.vpsid)
            if match is None:
                raise RuntimeError(f"No WHMCS service found for VPS {vm.vpsid}")
            self.logger.log(f"VPS {vm.vpsid} matched {self._match_value(match, 'source_name')}")
            match_data = self._serialize_match(match)
            self._artifact["whmcs_match"] = match_data
            return self._success(match, match_data)

        return self._run_stage("resolve_whmcs", action)

    def resolve_user(self, match):
        def action() -> Any:
            client_id = int(self._match_value(match, "client_id"))
            status, body = self.virtfusion_client.find_user_by_ext_relation(client_id)
            if status == 200:
                user_id = parse_response_data_id(body, "VirtFusion user lookup")
                data = {"user_id": user_id, "mode": "existing", "client_id": client_id}
                self._artifact["virtfusion"]["user_id"] = user_id
                return self._success(user_id, data)
            if status != 404:
                raise RuntimeError(f"VirtFusion user lookup failed with status {status}")
            if not self.execute:
                data = {"user_id": 0, "mode": "planned", "client_id": client_id}
                self._artifact["virtfusion"]["user_id"] = 0
                return self._planned(0, data)
            status, body = self.virtfusion_client.create_user(
                self._match_value(match, "firstname"),
                self._match_value(match, "lastname"),
                self._match_value(match, "email"),
                client_id,
            )
            if status not in (200, 201):
                raise RuntimeError(f"VirtFusion user create failed with status {status}")
            user_id = parse_response_data_id(body, "VirtFusion user create")
            data = {"user_id": user_id, "mode": "created", "client_id": client_id}
            self._artifact["virtfusion"]["user_id"] = user_id
            return self._success(user_id, data)

        return self._run_stage("resolve_user", action)

    def create_server(self, vm, user_id):
        def action() -> Any:
            payload = build_server_payload(
                vm,
                user_id=user_id,
                hypervisor_id=self.vf_hypervisor_id,
                package_id=self.vf_package_id,
                storage_profile_id=self.vf_storage_profile_id,
                network_profile_id=self.vf_network_profile_id,
                additional_storage1_profile_id=self.vf_additional_storage1_profile_id,
                additional_storage2_profile_id=self.vf_additional_storage2_profile_id,
            )
            if not self.execute:
                server = {
                    "server_id": 0,
                    "uuid": f"dry-run-vps-{vm.vpsid}",
                    "name": "",
                    "hostname": None,
                    "storage_names": [f"dry-run-vps-{vm.vpsid}-disk{disk.slot}" for disk in vm.disks],
                    "primary_ipv4": vm.primary_ipv4,
                    "ipv4_addresses": [vm.primary_ipv4] if vm.primary_ipv4 else [],
                }
                data = {**server, "payload": payload}
                self._artifact["virtfusion"].update(data)
                return self._planned(server, data)
            status, body = self.virtfusion_client.create_server(payload)
            if status not in (200, 201):
                raise RuntimeError(f"VirtFusion server create failed with status {status}")
            server_id = parse_response_data_id(body, "VirtFusion server create")
            status, body = self.virtfusion_client.get_server(server_id)
            if status != 200:
                raise RuntimeError(f"VirtFusion server lookup failed with status {status}")
            server = asdict(parse_server_details(body or {}))
            self._artifact["virtfusion"].update(server)
            self._artifact["virtfusion"]["payload"] = payload
            return self._success(server, server)

        return self._run_stage("create_server", action)

    def build_server(self, server, vm, match=None) -> None:
        def action() -> Any:
            server_id = server["server_id"]
            if not self.execute:
                return self._planned(None, {"server_id": server_id})
            # Check if already commissioned (idempotent for resume)
            status, body = self.virtfusion_client.get_server(server_id)
            if status != 200:
                raise RuntimeError(f"VirtFusion server lookup failed with status {status}: {body}")
            data = (body or {}).get("data", {})
            commission_status = data.get("commissionStatus", 0)
            if commission_status >= 3:
                return self._success(None, {
                    "server_id": server_id,
                    "commission_status": commission_status,
                    "poll_count": 0,
                    "skipped": True,
                    "reason": "already_commissioned",
                }, persist=True)
            # If build is already in progress (1 or 2), skip POST and just poll
            if commission_status > 0:
                commission_status, poll_count = self._wait_for_build(server_id)
                return self._success(None, {
                    "server_id": server_id,
                    "commission_status": commission_status,
                    "poll_count": poll_count,
                    "resumed": True,
                })
            build_name, build_hostname = self._build_server_identity(vm, match)
            status, body = self.virtfusion_client.build_server(
                server_id,
                self.vf_os_template_id,
                build_name,
                build_hostname,
            )
            if status not in (200, 201, 202):
                raise RuntimeError(f"VirtFusion server build failed with status {status}: {body}")
            commission_status, poll_count = self._wait_for_build(server_id)
            return self._success(None, {
                "server_id": server_id,
                "commission_status": commission_status,
                "poll_count": poll_count,
            })

        self._run_stage("build_server", action)

    def preserve_hostname(self, vm, server) -> None:
        def action() -> Any:
            requested_name = vm.hostname.strip() if vm.hostname else ""
            if not requested_name:
                return self._success(None, {
                    "skipped": True,
                    "reason": "no_source_hostname",
                    "name": server.get("name", vm.vps_name),
                    "hostname": server.get("hostname"),
                })

            planned_call = self._server_name_api_call(server["server_id"], requested_name)
            data = {
                "requested_name": requested_name,
                "planned_call": planned_call,
                "name": requested_name,
                "hostname": server.get("hostname"),
            }

            if not self.execute:
                server["name"] = requested_name
                self._artifact["virtfusion"]["name"] = requested_name
                return self._planned(None, data)

            status, body = self.virtfusion_client.modify_server_name(server["server_id"], requested_name)
            if status not in (200, 201, 202):
                raise RuntimeError(f"VirtFusion server name update failed with status {status}: {body}")

            status, body = self.virtfusion_client.get_server(server["server_id"])
            if status != 200:
                raise RuntimeError(f"VirtFusion server lookup failed with status {status}: {body}")

            refreshed = asdict(parse_server_details(body or {}))
            server.update(refreshed)
            self._artifact["virtfusion"].update(refreshed)
            data["name"] = server["name"]
            data["hostname"] = server["hostname"]

            if server["name"] != requested_name:
                raise RuntimeError(
                    f"VirtFusion server name mismatch after rename for VM {vm.vpsid}: "
                    f"expected {requested_name!r}, got {server['name']!r}"
                )

            return self._success(None, data, persist=True)

        self._run_stage("preserve_hostname", action)

    def shutdown_source(self, vm) -> None:
        def action() -> Any:
            commands = build_shutdown_commands(vm.vps_name)
            state_command = build_domstate_command(vm.vps_name)
            if self.skip_shutdown:
                data = {"skipped": True, "reason": "skip_shutdown", "commands": commands, "state_command": state_command}
                return self._success(None, data) if self.execute else self._planned(None, data)
            if not self.execute:
                return self._planned(
                    None,
                    {
                        "commands": commands,
                        "state_command": state_command,
                        "timeout_seconds": SHUTDOWN_TIMEOUT_SECONDS,
                    },
                )
            # Check if already shut off before issuing shutdown command
            runner = self._require_runner()
            result = runner.run_readonly(state_command, timeout=DOMSTATE_COMMAND_TIMEOUT_SECONDS)
            already_off = result.returncode == 0 and result.stdout.strip().lower().startswith("shut off")
            if already_off:
                return self._success(None, {
                    "commands": commands,
                    "state_command": state_command,
                    "final_state": "shut off",
                    "poll_count": 0,
                    "timeout_seconds": SHUTDOWN_TIMEOUT_SECONDS,
                    "skipped": True,
                    "reason": "already_shut_off",
                }, persist=True)
            self._run_commands(commands, timeout=SHUTDOWN_COMMAND_TIMEOUT_SECONDS)
            final_state, poll_count = self._wait_for_shutdown(vm.vps_name)
            return self._success(
                None,
                {
                    "commands": commands,
                    "state_command": state_command,
                    "final_state": final_state,
                    "poll_count": poll_count,
                    "timeout_seconds": SHUTDOWN_TIMEOUT_SECONDS,
                },
            )

        self._run_stage("shutdown_source", action)

    def copy_disks(self, vm, server) -> None:
        def action() -> Any:
            server_id = server.get("server_id")
            if server_id is not None:
                server_id = int(server_id)
            if self.execute and server_id is None:
                raise RuntimeError("copy_disks requires server_id in execute mode")
            pool = self._pool_name(vm.disks[0].source_path)
            intended_storage_names = list(server["storage_names"])
            commands = self._build_copy_disk_commands(vm, pool, intended_storage_names)
            if not self.execute:
                return self._planned(None, {"commands": commands, "storage_names": intended_storage_names})
            target_power_state = self._ensure_target_server_powered_off(server_id)
            fresh_storage_names = self._refresh_target_storage_names(server_id)
            if fresh_storage_names != intended_storage_names:
                raise RuntimeError(
                    "fresh VirtFusion storage names do not match the intended target images; refusing to delete"
                )
            readiness_poll_count = 0
            for new_image in fresh_storage_names:
                readiness_poll_count += self._wait_for_rbd_image(pool, new_image)
            self._run_commands(commands, timeout=RBD_COPY_TIMEOUT_SECONDS)
            return self._success(
                None,
                {
                    "commands": commands,
                    "server_id": server_id,
                    "storage_names": fresh_storage_names,
                    "target_power_state": target_power_state,
                    "readiness_poll_count": readiness_poll_count,
                },
            )

        self._run_stage("copy_disks", action)

    def inject_guest(self, vm, server) -> None:
        def action() -> Any:
            pool = self._pool_name(vm.disks[0].source_path)
            image_name = server["storage_names"][0]
            map_command = self._rbd_command("map", f"{pool}/{image_name}")
            if self.skip_inject:
                data = {"skipped": True, "reason": "skip_inject"}
                return self._success(None, data) if self.execute else self._planned(None, data)
            if should_skip_guest_injection(vm.os_name):
                data = {"skipped": True, "reason": "unsupported_os", "os_name": vm.os_name}
                return self._success(None, data) if self.execute else self._planned(None, data)
            if not self.execute:
                disk_path = "/dev/rbdX"
                command = build_guest_inject_command(disk_path)
                return self._planned(None, {"command": command, "disk_path": disk_path, "map_command": map_command})
            readiness_poll_count = self._wait_for_rbd_image(pool, image_name)
            runner = self._require_runner()
            map_result = runner.run(map_command, timeout=RBD_IMAGE_COMMAND_TIMEOUT_SECONDS)
            if map_result.returncode != 0:
                raise RuntimeError(map_result.stderr.strip() or f"Command failed: {' '.join(map_command)}")
            disk_path = map_result.stdout.strip()
            if not disk_path:
                raise RuntimeError(f"RBD map did not return a device path for {pool}/{image_name}")
            unmap_command = self._rbd_command("unmap", disk_path)
            command = build_guest_inject_command(disk_path)
            try:
                self._run_command(command, timeout=GUEST_INJECT_TIMEOUT_SECONDS)
            finally:
                self._run_command(unmap_command, timeout=RBD_IMAGE_COMMAND_TIMEOUT_SECONDS)
            return self._success(
                None,
                {
                    "command": command,
                    "disk_path": disk_path,
                    "map_command": map_command,
                    "unmap_command": unmap_command,
                    "readiness_poll_count": readiness_poll_count,
                },
            )

        self._run_stage("inject_guest", action)

    def add_ipv4(self, vm, server) -> None:
        def action() -> Any:
            def record_final_ipv4_state() -> None:
                data["primary_ipv4"] = server.get("primary_ipv4")
                data["ipv4_addresses"] = self._server_ipv4_addresses(server)

            desired_primary = vm.primary_ipv4
            desired_additional = [address for address in vm.additional_ipv4 if address and address != desired_primary]
            desired_ipv4_set = {desired_primary, *desired_additional}
            if self.execute:
                status, body = self.virtfusion_client.get_server(server["server_id"])
                if status != 200:
                    raise RuntimeError(f"VirtFusion server lookup failed with status {status}: {body}")
                server.update(asdict(parse_server_details(body or {})))
            current_ipv4_addresses = self._server_ipv4_addresses(server)
            current_primary = server.get("primary_ipv4") or (current_ipv4_addresses[0] if current_ipv4_addresses else None)
            desired_primary_missing = desired_primary not in current_ipv4_addresses if desired_primary else False
            data = {
                "exact_ipv4_preserved": True,
                "target_primary_before": current_primary,
                "target_primary_after": current_primary,
                "added_primary": [],
                "removed_auto_assigned": [],
                "added_additional": [],
                "planned_calls": [],
            }

            if desired_primary is None and not desired_additional:
                if current_ipv4_addresses:
                    raise RuntimeError(
                        f"VM {vm.vpsid} has no source IPv4 but target has unexpected target IPv4(s): "
                        f"{', '.join(current_ipv4_addresses)}"
                    )
                data["skipped"] = True
                data["reason"] = "no_source_ipv4"
                record_final_ipv4_state()
                return self._success(None, data, persist=True)

            if desired_primary is None:
                raise RuntimeError(f"Primary IPv4 is required for VM {vm.vpsid} when preserving IPv4 assignments")

            if current_primary != desired_primary:
                if desired_primary_missing:
                    data["planned_calls"].append(self._ipv4_api_call("POST", server["server_id"], [desired_primary], "primary"))
                    if self.execute:
                        status, body = self.virtfusion_client.add_ipv4(server["server_id"], [desired_primary], "primary")
                        if status not in (200, 201, 204):
                            raise RuntimeError(f"VirtFusion IPv4 add failed with status {status}: {body}")
                    data["added_primary"] = [desired_primary]
                current_ipv4_addresses = [desired_primary, *[address for address in current_ipv4_addresses if address != desired_primary]]
                data["target_primary_after"] = desired_primary

                if current_primary:
                    data["planned_calls"].append(self._ipv4_api_call("DELETE", server["server_id"], [current_primary]))
                    if self.execute:
                        status, body = self.virtfusion_client.delete_ipv4(server["server_id"], [current_primary])
                        if status not in (200, 201, 204):
                            raise RuntimeError(f"VirtFusion IPv4 delete failed with status {status}: {body}")
                    current_ipv4_addresses = [address for address in current_ipv4_addresses if address != current_primary]
                    data["removed_auto_assigned"] = [current_primary]
                self._update_server_ipv4(server, current_ipv4_addresses)

            undesired_ipv4_addresses = (
                [address for address in current_ipv4_addresses if address not in desired_ipv4_set]
                if desired_primary in current_ipv4_addresses
                else []
            )
            if undesired_ipv4_addresses:
                data["planned_calls"].append(self._ipv4_api_call("DELETE", server["server_id"], undesired_ipv4_addresses))
                if self.execute:
                    status, body = self.virtfusion_client.delete_ipv4(server["server_id"], undesired_ipv4_addresses)
                    if status not in (200, 201, 204):
                        raise RuntimeError(f"VirtFusion IPv4 delete failed with status {status}: {body}")
                current_ipv4_addresses = [
                    address for address in current_ipv4_addresses if address not in set(undesired_ipv4_addresses)
                ]
                if data["removed_auto_assigned"]:
                    data["removed_auto_assigned"] = list(
                        dict.fromkeys([*data["removed_auto_assigned"], *undesired_ipv4_addresses])
                    )
                else:
                    data["removed_auto_assigned"] = list(undesired_ipv4_addresses)
                self._update_server_ipv4(server, current_ipv4_addresses)

            missing_additional = [address for address in desired_additional if address not in current_ipv4_addresses]
            if missing_additional:
                data["planned_calls"].append(self._ipv4_api_call("POST", server["server_id"], missing_additional, "secondary"))
                if self.execute:
                    status, body = self.virtfusion_client.add_ipv4(server["server_id"], missing_additional, "secondary")
                    if status not in (200, 201, 204):
                        raise RuntimeError(f"VirtFusion IPv4 add failed with status {status}: {body}")
                current_ipv4_addresses = [*current_ipv4_addresses, *missing_additional]
                self._update_server_ipv4(server, current_ipv4_addresses)
                data["added_additional"] = missing_additional

            data["target_primary_after"] = server.get("primary_ipv4") or desired_primary
            record_final_ipv4_state()
            if not self.execute:
                return self._planned(None, data)
            self._update_server_ipv4(server, current_ipv4_addresses)
            record_final_ipv4_state()
            return self._success(None, data, persist=True)

        self._run_stage("add_ipv4", action)

    def preserve_ipv6(self, vm, server) -> None:
        def action() -> Any:
            if not vm.ipv6_addresses:
                return self._success(None, {"addresses": [], "skipped": True, "reason": "no_ipv6"})
            if not vm.ipv6_subnet or vm.ipv6_cidr is None:
                raise RuntimeError(f"IPv6 assignment is incomplete for VM {vm.vpsid}; subnet and cidr are required")
            payload = build_preserve_ipv6_payload(vm, server["server_id"])
            if not self.execute:
                return self._planned(None, {"payload": payload})
            if self.vf_master_client is None:
                raise RuntimeError("VirtFusion master client is required for preserve_ipv6 stage in execute mode")
            helper_data = self.vf_master_client.preserve_ipv6(vm, server["server_id"])
            data = {"server_id": server["server_id"], "addresses": list(vm.ipv6_addresses), **helper_data}
            return self._success(None, data)

        self._run_stage("preserve_ipv6", action)

    def boot_target(self, server) -> None:
        def action() -> Any:
            if not self.execute:
                return self._planned(None, {"server_id": server["server_id"]})
            status, body = self.virtfusion_client.boot_server(server["server_id"])
            if status not in (200, 201, 202):
                raise RuntimeError(f"VirtFusion boot failed with status {status}: {body}")
            return self._success(None, {"server_id": server["server_id"]})

        self._run_stage("boot_target", action)

    def preflight_whmcs_target(self, match) -> None:
        self._target_server_id_for_match(match)

    def rewrite_whmcs(self, vm, match, server) -> None:
        def action() -> Any:
            source_product_id = int(self._match_value(match, "product_id"))
            target_product_id = self.whmcs_product_map.get(source_product_id)
            if target_product_id is None:
                raise RuntimeError(
                    f"No WHMCS target product mapping for source packageid {source_product_id}"
                )
            assigned_ips = build_assignedips(vm.additional_ipv4, vm.ipv6_addresses)
            target_server_id = self._target_server_id_for_match(match)
            update_query = build_tblhosting_update_query(
                hosting_id=int(self._match_value(match, "hosting_id")),
                product_id=target_product_id,
                server_id=target_server_id,
                domain=vm.hostname,
                dedicated_ip=server.get("primary_ipv4") or vm.primary_ipv4 or "",
                assigned_ips=assigned_ips,
            )
            server_object_json = json.dumps(server, sort_keys=True)
            module_update = build_mod_virtfusion_direct_update_query(
                hosting_id=int(self._match_value(match, "hosting_id")),
                server_id=server["server_id"],
                server_object_json=server_object_json,
            )
            module_insert = build_mod_virtfusion_direct_insert_query(
                hosting_id=int(self._match_value(match, "hosting_id")),
                server_id=server["server_id"],
                server_object_json=server_object_json,
            )
            verify_tblhosting = build_tblhosting_verify_query(
                hosting_id=int(self._match_value(match, "hosting_id")),
                product_id=target_product_id,
                server_id=target_server_id,
                domain=vm.hostname,
                dedicated_ip=server.get("primary_ipv4") or vm.primary_ipv4 or "",
                assigned_ips=assigned_ips,
            )
            verify_module = build_mod_virtfusion_direct_verify_query(
                hosting_id=int(self._match_value(match, "hosting_id")),
                server_id=server["server_id"],
                server_object_json=server_object_json,
            )
            data = {
                "hosting_id": int(self._match_value(match, "hosting_id")),
                "server_id": server["server_id"],
                "queries": [update_query, module_update, module_insert],
                "verify_queries": [verify_tblhosting, verify_module],
                "database": self._match_value(match, "source_name"),
            }
            if not self.execute:
                return self._planned(None, data)
            db = self._db_for_match(match)
            row_counts = self._run_mysql_transaction(db, [*data["queries"], *data["verify_queries"]])
            if row_counts["rewrite_ok"] != 1:
                raise RuntimeError(f"WHMCS rewrite row counts invalid: {row_counts}")
            data["row_counts"] = row_counts
            return self._success(None, data)

        self._run_stage("rewrite_whmcs", action)

    def _run_stage(self, stage: str, action: Callable[[], dict[str, Any]]) -> Any:
        self._ensure_active_vm()
        started_at = self._now()
        record = self._artifact["stages"].get(stage, {})
        record.update({"status": "running", "started_at": started_at, "finished_at": None, "data": {}})
        self._artifact["stages"][stage] = record
        self._write_artifact()
        try:
            outcome = action()
        except Exception as exc:
            finished_at = self._now()
            record["status"] = "failed"
            record["finished_at"] = finished_at
            record["data"] = {**record.get("data", {}), "error": {"type": type(exc).__name__, "message": str(exc)}}
            self._artifact["error"] = {
                "type": type(exc).__name__,
                "message": str(exc),
                "stage": stage,
            }
            setattr(exc, "_migration_stage", stage)
            self._write_artifact()
            raise
        finished_at = self._now()
        record["status"] = outcome["status"]
        record["finished_at"] = finished_at
        record["data"] = outcome["data"]
        self._write_artifact()
        if outcome["status"] == "planned":
            self.logger.log(f"PLAN {stage}: {json.dumps(outcome['data'], sort_keys=True)}")
        if self.execute and outcome.get("persist", outcome["status"] == "done"):
            self.state.mark_stage(str(self._vm.vpsid), stage, outcome["status"], outcome["data"])
        return outcome["result"]

    @staticmethod
    def _success(result: Any, data: dict[str, Any], persist: bool | None = None) -> dict[str, Any]:
        if persist is None:
            persist = not data.get("skipped", False)
        return {"status": "done", "result": result, "data": data, "persist": persist}

    @staticmethod
    def _planned(result: Any, data: dict[str, Any]) -> dict[str, Any]:
        return {"status": "planned", "result": result, "data": data, "persist": False}

    def _run_commands(self, commands: list[list[str]], timeout: float | None = None) -> None:
        for command in commands:
            self._run_command(command, timeout=timeout)

    def _build_copy_disk_commands(self, vm, pool: str, target_images: list[str]) -> list[list[str]]:
        self._validate_copy_disk_targets(vm, target_images)
        if len(target_images) != len(vm.disks):
            raise RuntimeError("target RBD image count does not match source disk count")
        try:
            commands = [
                command
                for disk, new_image in zip(vm.disks, target_images, strict=True)
                for command in build_rbd_copy_commands(
                    pool=pool,
                    old_image=disk.rbd_name,
                    new_image=new_image,
                )
            ]
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc
        return self._with_rbd_auth(commands)

    @staticmethod
    def _validate_copy_disk_targets(vm, target_images: list[str]) -> None:
        if not target_images:
            raise RuntimeError("target RBD image list must not be empty")
        if any(not image for image in target_images):
            raise RuntimeError("target RBD image names must not be empty")
        if len(set(target_images)) != len(target_images):
            raise RuntimeError("duplicate target RBD image names are not allowed")
        source_images = {disk.rbd_name for disk in vm.disks}
        collisions = sorted(source_images.intersection(target_images))
        if collisions:
            raise RuntimeError(f"target RBD image must not match source RBD image: {collisions[0]!r}")

    def _run_command(
        self,
        command: list[str],
        timeout: float | None = None,
        env: dict[str, str] | None = None,
    ) -> None:
        runner = self._require_runner()
        result = runner.run(command, timeout=timeout, env=env)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or f"Command failed: {' '.join(command)}")

    def _wait_for_shutdown(self, vps_name: str) -> tuple[str, int]:
        runner = self._require_runner()
        deadline = time.monotonic() + SHUTDOWN_TIMEOUT_SECONDS
        poll_count = 0
        while True:
            result = runner.run_readonly(
                build_domstate_command(vps_name),
                timeout=DOMSTATE_COMMAND_TIMEOUT_SECONDS,
            )
            poll_count += 1
            if result.returncode != 0:
                raise RuntimeError(result.stderr.strip() or f"Unable to read shutdown state for {vps_name}")
            state = result.stdout.strip().lower()
            if state.startswith("shut off"):
                return "shut off", poll_count
            if time.monotonic() >= deadline:
                raise RuntimeError(f"timed out waiting for {vps_name} to shut off")
            time.sleep(SHUTDOWN_POLL_INTERVAL_SECONDS)

    def _target_runtime_state(self, server_id: int) -> dict[str, Any]:
        status, body = self.virtfusion_client.get_server(server_id, remote_state=True)
        if status != 200:
            raise RuntimeError(
                f"VirtFusion target runtime state lookup failed with status {status}: {body}"
            )
        if body is None:
            payload: dict[str, Any] = {}
        elif not isinstance(body, dict):
            raise RuntimeError(
                f"VirtFusion target runtime state could not be determined for server {server_id}: "
                "response payload invalid"
            )
        else:
            payload = body
        data = payload.get("data", {})
        if not isinstance(data, dict):
            raise RuntimeError(
                f"VirtFusion target runtime state could not be determined for server {server_id}: "
                "remoteState missing or invalid"
            )
        remote_state = data.get("remoteState")
        if not isinstance(remote_state, dict):
            raise RuntimeError(
                f"VirtFusion target runtime state could not be determined for server {server_id}: "
                "remoteState missing or invalid"
            )
        running = remote_state.get("running")
        if not isinstance(running, bool):
            raise RuntimeError(
                f"VirtFusion target runtime state could not be determined for server {server_id}: "
                "remoteState.running missing"
            )
        return {"remote_state": remote_state, "running": running}

    def _refresh_target_storage_names(self, server_id: int) -> list[str]:
        status, body = self.virtfusion_client.get_server(server_id)
        if status != 200:
            raise RuntimeError(f"VirtFusion server lookup failed with status {status}: {body}")
        return asdict(parse_server_details(body or {}))["storage_names"]

    def _ensure_target_server_powered_off(self, server_id: int) -> dict[str, Any]:
        initial_state = self._target_runtime_state(server_id)
        metadata = {
            "server_id": server_id,
            "initial": initial_state,
            "final": initial_state,
            "shutdown_requested": False,
            "poweroff_requested": False,
            "graceful_poll_count": 0,
            "poweroff_poll_count": 0,
        }
        if not initial_state["running"]:
            metadata["skipped"] = True
            metadata["reason"] = "already_powered_off"
            return metadata

        status, body = self.virtfusion_client.shutdown_server(server_id)
        if status not in (200, 201, 202):
            raise RuntimeError(f"VirtFusion target shutdown failed with status {status}: {body}")
        metadata["shutdown_requested"] = True

        final_state, graceful_poll_count, timed_out = self._wait_for_target_powered_off(
            server_id,
            TARGET_SHUTDOWN_TIMEOUT_SECONDS,
        )
        metadata["final"] = final_state
        metadata["graceful_poll_count"] = graceful_poll_count
        if not timed_out:
            return metadata

        status, body = self.virtfusion_client.poweroff_server(server_id)
        if status not in (200, 201, 202):
            raise RuntimeError(f"VirtFusion target poweroff failed with status {status}: {body}")
        metadata["poweroff_requested"] = True

        final_state, poweroff_poll_count, timed_out = self._wait_for_target_powered_off(
            server_id,
            TARGET_POWEROFF_TIMEOUT_SECONDS,
        )
        metadata["final"] = final_state
        metadata["poweroff_poll_count"] = poweroff_poll_count
        if timed_out:
            raise RuntimeError(f"timed out waiting for VirtFusion server {server_id} to power off")
        return metadata

    def _wait_for_target_powered_off(self, server_id: int, timeout_seconds: int) -> tuple[dict[str, Any], int, bool]:
        deadline = time.monotonic() + timeout_seconds
        poll_count = 0
        while True:
            state = self._target_runtime_state(server_id)
            poll_count += 1
            if not state["running"]:
                return state, poll_count, False
            if time.monotonic() >= deadline:
                state = self._target_runtime_state(server_id)
                poll_count += 1
                return state, poll_count, state["running"]
            time.sleep(TARGET_SHUTDOWN_POLL_INTERVAL_SECONDS)

    def _wait_for_build(self, server_id: int) -> tuple[int, int]:
        deadline = time.monotonic() + BUILD_SERVER_TIMEOUT_SECONDS
        poll_count = 0
        while True:
            status, body = self.virtfusion_client.get_server(server_id)
            poll_count += 1
            if status != 200:
                raise RuntimeError(
                    f"VirtFusion server lookup failed during build poll with status {status}: {body}"
                )
            data = (body or {}).get("data", {})
            commission_status = data.get("commissionStatus", 0)
            if commission_status >= 3:
                return commission_status, poll_count
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"timed out waiting for VirtFusion server {server_id} to be commissioned "
                    f"(commissionStatus={commission_status})"
                )
            time.sleep(BUILD_SERVER_POLL_INTERVAL_SECONDS)

    def _wait_for_rbd_image(self, pool: str, image: str) -> int:
        runner = self._require_runner()
        deadline = time.monotonic() + RBD_IMAGE_TIMEOUT_SECONDS
        poll_count = 0
        command = self._rbd_command("ls", pool)
        while True:
            result = runner.run_readonly(command, timeout=RBD_IMAGE_COMMAND_TIMEOUT_SECONDS)
            poll_count += 1
            if result.returncode != 0:
                raise RuntimeError(result.stderr.strip() or f"Command failed: {' '.join(command)}")
            if result.returncode == 0 and image in result.stdout.splitlines():
                return poll_count
            if time.monotonic() >= deadline:
                raise RuntimeError(f"timed out waiting for RBD image {pool}/{image}")
            time.sleep(RBD_IMAGE_POLL_INTERVAL_SECONDS)

    def _rbd_command(self, *args: str) -> list[str]:
        return ["rbd", "--id", self.ceph_username, "--keyring", self.ceph_keyring, *args]

    def _with_rbd_auth(self, commands: list[list[str]]) -> list[list[str]]:
        return [self._rbd_command(*command[1:]) if command and command[0] == "rbd" else command for command in commands]

    def _run_mysql_transaction(self, db, queries: list[str]) -> dict[str, int]:
        runner = self._require_runner()
        db_info = self._db_info(db)
        command = [
            self.whmcs_mysql_bin,
            "--batch",
            "--raw",
            "--skip-column-names",
            "-u",
            db_info.user,
            "-h",
            db_info.host,
            "-P",
            str(db_info.port),
            db_info.name,
        ]
        script = build_mysql_transaction_script(*queries)
        result = runner.run_with_input(
            command,
            script,
            timeout=WHMCS_REWRITE_TIMEOUT_SECONDS,
            env={"MYSQL_PWD": db_info.password},
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "WHMCS rewrite failed")
        try:
            return parse_mysql_transaction_result(result.stdout)
        except ValueError as exc:
            raise RuntimeError("WHMCS rewrite returned invalid transaction output") from exc

    def _db_for_match(self, match):
        source_name = self._match_value(match, "source_name")
        if source_name == "whmcs_a":
            return self.whmcs_a_db
        if source_name == "whmcs_b":
            return self.whmcs_b_db
        raise RuntimeError(f"Unsupported WHMCS source {source_name}")

    def _target_server_id_for_match(self, match) -> int:
        source_name = self._match_value(match, "source_name")
        if source_name == "whmcs_a":
            if self.whmcs_a_target_server_id is None:
                raise RuntimeError("whmcs_a_target_server_id is required but was not configured")
            return self.whmcs_a_target_server_id
        if source_name == "whmcs_b":
            if self.whmcs_b_target_server_id is None:
                raise RuntimeError("whmcs_b_target_server_id is required but was not configured")
            return self.whmcs_b_target_server_id
        raise RuntimeError(f"Unsupported WHMCS source {source_name}")

    @staticmethod
    def _db_info(db) -> SimpleNamespace:
        if db is None:
            raise RuntimeError("WHMCS database configuration is required")
        if isinstance(db, dict):
            return SimpleNamespace(**db)
        return db

    @staticmethod
    def _pool_name(source_path: str) -> str:
        parts = source_path.split("/")
        if len(parts) < 5:
            raise RuntimeError(f"Unexpected source disk path: {source_path}")
        return parts[3]

    @staticmethod
    def _match_value(match, key: str) -> Any:
        if isinstance(match, dict):
            return match[key]
        return getattr(match, key)

    @classmethod
    def _build_server_identity(cls, vm, match) -> tuple[str, str]:
        source_hostname = (vm.hostname or "").strip()
        source_name = source_hostname or (vm.vps_name or "").strip()
        whmcs_domain = ""
        if match is not None:
            whmcs_domain = (cls._match_value(match, "domain") or "").strip()

        if cls._is_fqdn(source_hostname):
            build_hostname = source_hostname
        elif cls._is_fqdn(whmcs_domain):
            build_hostname = whmcs_domain
        else:
            build_hostname = cls._synthetic_build_hostname(source_hostname or (vm.vps_name or ""), vm.vpsid)

        return (source_name or build_hostname, build_hostname)

    @staticmethod
    def _is_fqdn(value: str) -> bool:
        if not value or "." not in value or " " in value:
            return False
        labels = value.split(".")
        return all(label and len(label) <= 63 for label in labels)

    @staticmethod
    def _synthetic_build_hostname(value: str, vpsid: int) -> str:
        label = re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")
        if not label:
            label = f"vm-{vpsid}"
        return f"{label}.migration.invalid"

    @staticmethod
    def _serialize_match(match) -> dict[str, Any]:
        if is_dataclass(match):
            return asdict(match)
        if isinstance(match, dict):
            return dict(match)
        return {
            "source_name": getattr(match, "source_name"),
            "client_id": getattr(match, "client_id"),
            "firstname": getattr(match, "firstname"),
            "lastname": getattr(match, "lastname"),
            "email": getattr(match, "email"),
            "product_id": getattr(match, "product_id"),
            "product_name": getattr(match, "product_name"),
            "hosting_id": getattr(match, "hosting_id"),
            "domain": getattr(match, "domain"),
        }

    @staticmethod
    def _server_ipv4_addresses(server: dict[str, Any]) -> list[str]:
        primary = server.get("primary_ipv4")
        addresses = [address for address in (server.get("ipv4_addresses") or []) if address]
        normalized = ([primary] if primary else []) + [address for address in addresses if address != primary]
        return list(dict.fromkeys(normalized))

    def _update_server_ipv4(self, server: dict[str, Any], addresses: list[str]) -> None:
        server["ipv4_addresses"] = list(addresses)
        server["primary_ipv4"] = addresses[0] if addresses else None
        self._artifact["virtfusion"]["ipv4_addresses"] = list(addresses)
        self._artifact["virtfusion"]["primary_ipv4"] = server["primary_ipv4"]

    @staticmethod
    def _ipv4_api_call(method: str, server_id: int, addresses: list[str], interface: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"ip": addresses}
        if interface is not None:
            payload["interface"] = interface
        return {
            "method": method,
            "path": f"/servers/{server_id}/ipv4",
            "json": payload,
        }

    @staticmethod
    def _server_name_api_call(server_id: int, name: str) -> dict[str, Any]:
        return {
            "method": "PUT",
            "path": f"/servers/{server_id}/modify/name",
            "json": {"name": name},
        }

    def _ensure_active_vm(self) -> None:
        if self._vm is None or self._artifact is None:
            raise RuntimeError("begin_vm() must be called before running migration stages")

    def _write_artifact(self) -> None:
        self._ensure_active_vm()
        self.vm_reporter.write(self._vm.vpsid, self._artifact)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _require_runner(self):
        if self.runner is None:
            raise RuntimeError("Command runner is required for this stage")
        return self.runner
