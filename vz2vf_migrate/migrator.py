from vz2vf_migrate.models import SourceVm, VmResult


def _join_ip_addresses(vm: SourceVm) -> str:
    parts = [vm.primary_ipv4 or "", *vm.additional_ipv4, *vm.ipv6_addresses]
    return ",".join(part for part in parts if part)


def _old_rbd_image(vm: SourceVm) -> str:
    return vm.disks[0].rbd_name if vm.disks else ""


def _failed_vm_result(vm: SourceVm, notes: str) -> VmResult:
    return VmResult(
        vpsid=vm.vpsid,
        vps_name=vm.vps_name,
        hostname=vm.hostname,
        old_rbd_image=_old_rbd_image(vm),
        new_vf_server_id="",
        new_rbd_image="",
        ip_addresses=_join_ip_addresses(vm),
        status="failed",
        notes=notes,
    )


def _resume_stage_data(completed_stages: dict, stage: str):
    value = completed_stages.get(stage)
    if isinstance(value, dict) and "status" in value and "data" in value:
        return value["data"]
    return value


def _add_ipv4_stage_complete(completed_stages: dict) -> bool:
    data = _resume_stage_data(completed_stages, "add_ipv4")
    if not (
        isinstance(data, dict)
        and data.get("exact_ipv4_preserved") is True
        and "primary_ipv4" in data
        and "ipv4_addresses" in data
        and isinstance(data.get("ipv4_addresses"), list)
    ):
        return False
    if data.get("reason") == "no_source_ipv4":
        return data.get("primary_ipv4") is None and data.get("ipv4_addresses") == []
    return data.get("primary_ipv4") is not None


def _merge_resumed_add_ipv4_server_state(server: dict | None, completed_stages: dict) -> dict | None:
    if not isinstance(server, dict) or not _add_ipv4_stage_complete(completed_stages):
        return server
    add_ipv4_data = _resume_stage_data(completed_stages, "add_ipv4")
    if not isinstance(add_ipv4_data, dict):
        return server
    for key in ("primary_ipv4", "ipv4_addresses"):
        if key in add_ipv4_data:
            server[key] = add_ipv4_data[key]
    return server


def _preserve_hostname_stage_complete(completed_stages: dict) -> bool:
    data = _resume_stage_data(completed_stages, "preserve_hostname")
    return isinstance(data, dict) and "name" in data


def _merge_preserved_hostname(server: dict | None, completed_stages: dict) -> dict | None:
    if not isinstance(server, dict) or not _preserve_hostname_stage_complete(completed_stages):
        return server
    preserve_hostname_data = _resume_stage_data(completed_stages, "preserve_hostname")
    if not isinstance(preserve_hostname_data, dict):
        return server
    for key in ("name", "hostname"):
        if key in preserve_hostname_data:
            server[key] = preserve_hostname_data[key]
    return server


def migrate_vm(
    vm: SourceVm,
    services,
    enable_whmcs: bool,
    completed_stages: dict | None = None,
) -> VmResult:
    completed_stages = completed_stages or {}

    if not vm.disks:
        return _failed_vm_result(vm, "source VM must include at least one usable RBD disk")

    if len(vm.disks) > 3:
        return _failed_vm_result(vm, "source VM has more than 3 disks")

    server = _resume_stage_data(completed_stages, "create_server")
    match = _resume_stage_data(completed_stages, "resolve_whmcs")
    if server is None:
        user_id = _resume_stage_data(completed_stages, "resolve_user")
        if isinstance(user_id, dict) and "user_id" in user_id:
            user_id = user_id["user_id"]
        if user_id is None:
            if match is None:
                match = services.resolve_whmcs(vm)
            # preflight before resolve_user whether match came from state or a fresh lookup
            if enable_whmcs and match is not None:
                services.preflight_whmcs_target(match)
            user_id = services.resolve_user(match)
        elif enable_whmcs and match is not None:
            # resolve_user was completed in a prior run; validate target server before create_server
            # (rewrite_whmcs() contains its own guard as defense in depth)
            services.preflight_whmcs_target(match)
        server = services.create_server(vm, user_id)
    server = _merge_resumed_add_ipv4_server_state(server, completed_stages)
    server = _merge_preserved_hostname(server, completed_stages)

    if enable_whmcs and "rewrite_whmcs" not in completed_stages and "boot_target" in completed_stages:
        raise RuntimeError(
            "Cannot resume WHMCS rewrite after boot_target completed; clear or repair the state before retrying"
        )

    if "build_server" not in completed_stages:
        services.build_server(server, vm, match)

    if not _preserve_hostname_stage_complete(completed_stages):
        services.preserve_hostname(vm, server)
    else:
        server = _merge_preserved_hostname(server, completed_stages)

    if "shutdown_source" not in completed_stages:
        services.shutdown_source(vm)
    if "copy_disks" not in completed_stages:
        services.copy_disks(vm, server)
    if "inject_guest" not in completed_stages:
        services.inject_guest(vm, server)
    if not _add_ipv4_stage_complete(completed_stages):
        services.add_ipv4(vm, server)
    else:
        server = _merge_resumed_add_ipv4_server_state(server, completed_stages)
    if "preserve_ipv6" not in completed_stages:
        services.preserve_ipv6(vm, server)

    if enable_whmcs:
        if "rewrite_whmcs" not in completed_stages:
            if match is None:
                match = services.resolve_whmcs(vm)
            services.rewrite_whmcs(vm, match, server)
    if "boot_target" not in completed_stages:
        services.boot_target(server)

    return VmResult(
        vpsid=vm.vpsid,
        vps_name=vm.vps_name,
        hostname=vm.hostname,
        old_rbd_image=_old_rbd_image(vm),
        new_vf_server_id=str(server["server_id"]),
        new_rbd_image=server["storage_names"][0],
        ip_addresses=_join_ip_addresses(vm),
        status="complete",
        notes="migration finished",
    )
