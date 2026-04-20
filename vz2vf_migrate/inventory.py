from collections import defaultdict
from dataclasses import replace
from decimal import Decimal, ROUND_CEILING
import ipaddress
from pathlib import Path

from vz2vf_migrate.models import SourceVm
from vz2vf_migrate.xml_disks import parse_rbd_disks


GB_UNITS = {"", "GB", "GIB"}
MB_UNITS = {"MB", "MIB"}
TB_UNITS = {"TB", "TIB"}
NULL_IP_SENTINELS = {r"\N", "NULL"}


def _size_gb(raw_size: str, raw_unit: str) -> int:
    value = Decimal(raw_size)
    unit = (raw_unit or "").strip().upper()
    if unit in GB_UNITS:
        return int(value.to_integral_value(rounding=ROUND_CEILING))
    if unit in MB_UNITS:
        return int((value / Decimal("1024")).to_integral_value(rounding=ROUND_CEILING))
    if unit in TB_UNITS:
        return int((value * Decimal("1024")).to_integral_value(rounding=ROUND_CEILING))
    raise ValueError(f"Unsupported disk size unit: {raw_unit}")


def _group_rows(rows: list[dict[str, str]]) -> list[list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["vpsid"]].append(row)
    return [grouped[vpsid] for vpsid in sorted(grouped, key=int)]


def _disk_name(disk_path: str) -> str:
    return disk_path.rsplit("/", 1)[-1]


def _disk_size_fact(row: dict[str, str]) -> tuple[str, Decimal, str, int]:
    disk_path = row["disk_path"]
    raw_size = row.get("disk_size")
    if raw_size is None or not raw_size.strip():
        raise ValueError(f"Missing disk_size for disk {_disk_name(disk_path)}")

    normalized_size = Decimal(raw_size.strip())
    normalized_unit = (row.get("disk_size_unit", "") or "").strip().upper()
    return (
        _disk_name(disk_path),
        normalized_size,
        normalized_unit,
        _size_gb(raw_size.strip(), row.get("disk_size_unit", "")),
    )


def _disk_sizes_by_name(group: list[dict[str, str]]) -> dict[str, int]:
    disk_facts: dict[str, tuple[Decimal, str, int]] = {}
    for row in group:
        disk_path = row.get("disk_path")
        if not disk_path:
            continue

        disk_name, raw_size, raw_unit, size_gb = _disk_size_fact(row)
        fact = (raw_size, raw_unit, size_gb)
        if disk_name in disk_facts and disk_facts[disk_name] != fact:
            raise ValueError(
                "Conflicting disk_size facts for disk "
                f"{disk_name}: {disk_facts[disk_name]!r} != {fact!r}"
            )
        disk_facts[disk_name] = fact
    return {disk_name: fact[2] for disk_name, fact in disk_facts.items()}


def _primary_disk_name(group: list[dict[str, str]], disk_sizes: dict[str, int]) -> str:
    if not disk_sizes:
        raise ValueError("Source VM must include at least one usable RBD disk")

    primary_names = {
        _disk_name(disk_path)
        for row in group
        if (disk_path := row.get("disk_path")) and row.get("disk_primary") == "1"
    }
    if len(disk_sizes) == 1:
        disk_name = next(iter(disk_sizes))
        if primary_names and primary_names != {disk_name}:
            raise ValueError(f"Primary disk metadata does not match resolved disk {disk_name}")
        return disk_name
    if len(primary_names) != 1:
        raise ValueError("Source VM must identify exactly one primary disk")

    disk_name = next(iter(primary_names))
    if disk_name not in disk_sizes:
        raise ValueError(f"Primary disk metadata references unknown disk {disk_name}")
    return disk_name


def _resolve_disks(xml_text: str, disk_sizes: dict[str, int], primary_disk_name: str):
    xml_disks = parse_rbd_disks(xml_text)
    if not xml_disks:
        raise ValueError("Source VM must include at least one usable RBD disk in libvirt XML")
    xml_disk_names = {disk.rbd_name for disk in xml_disks}
    db_disk_names = set(disk_sizes)

    missing_db_facts = sorted(xml_disk_names - db_disk_names)
    if missing_db_facts:
        raise ValueError(f"Missing size for XML disk {', '.join(missing_db_facts)}")

    missing_xml_disks = sorted(db_disk_names - xml_disk_names)
    if missing_xml_disks:
        raise ValueError(f"Missing XML disk for DB fact {', '.join(missing_xml_disks)}")

    if primary_disk_name not in xml_disk_names:
        raise ValueError(f"Primary disk {primary_disk_name} is missing from libvirt XML")

    ordered_disks = [
        *[disk for disk in xml_disks if disk.rbd_name == primary_disk_name],
        *[disk for disk in xml_disks if disk.rbd_name != primary_disk_name],
    ]
    return [
        replace(
            disk,
            slot=index,
            size_gb=disk_sizes[disk.rbd_name],
            is_primary=index == 1,
        )
        for index, disk in enumerate(ordered_disks, start=1)
    ]


def _resolve_ipv6_assignment(ipv6_addresses: list[str]) -> tuple[str | None, int | None]:
    if not ipv6_addresses:
        return None, None

    networks = {
        ipaddress.IPv6Network((ipaddress.IPv6Address(address), 64), strict=False)
        for address in ipv6_addresses
    }
    if len(networks) != 1:
        raise ValueError("IPv6 addresses must belong to a single /64")

    network = next(iter(networks))
    return network.network_address.compressed, network.prefixlen


def _normalize_ip_value(raw_ip: str | None) -> str | None:
    normalized_ip = (raw_ip or "").strip()
    if not normalized_ip or normalized_ip.upper() in NULL_IP_SENTINELS:
        return None
    return normalized_ip


def normalize_inventory_group(group: list[dict[str, str]], xml_text: str) -> SourceVm:
    base = group[0]
    disk_sizes = _disk_sizes_by_name(group)
    resolved_disks = _resolve_disks(xml_text, disk_sizes, _primary_disk_name(group, disk_sizes))

    primary_ipv4 = None
    has_ipv4 = False
    additional_ipv4: list[str] = []
    ipv6_addresses: list[str] = []
    seen_ips: set[str] = set()
    for row in group:
        ip = _normalize_ip_value(row.get("ip"))
        if ip is None or ip in seen_ips:
            continue
        seen_ips.add(ip)
        if row.get("ipv6") == "1":
            ipv6_addresses.append(ip)
        else:
            try:
                ipaddress.IPv4Address(ip)
            except ipaddress.AddressValueError as exc:
                raise ValueError(f"Invalid IPv4 address for non-IPv6 row: {ip}") from exc
            has_ipv4 = True
            if row.get("ip_primary") == "1":
                if primary_ipv4 is not None:
                    raise ValueError("Multiple primary IPv4 addresses found")
                primary_ipv4 = ip
            else:
                additional_ipv4.append(ip)

    if has_ipv4 and primary_ipv4 is None:
        raise ValueError("IPv4 rows require exactly one primary IPv4 address")

    ipv6_subnet, ipv6_cidr = _resolve_ipv6_assignment(ipv6_addresses)

    return SourceVm(
        vpsid=int(base["vpsid"]),
        vps_name=base["vps_name"],
        uuid=base["uuid"],
        hostname=base["hostname"],
        os_name=base["os_name"],
        cores=int(base["cores"]),
        ram_mb=int(base["ram"]),
        bandwidth_gb=int(base["bandwidth"]),
        network_speed_kbps=int(base["network_speed"]),
        upload_speed_kbps=int(base["upload_speed"]),
        disks=resolved_disks,
        primary_ipv4=primary_ipv4,
        additional_ipv4=additional_ipv4,
        ipv6_addresses=ipv6_addresses,
        ipv6_subnet=ipv6_subnet,
        ipv6_cidr=ipv6_cidr,
        virtualizor_uid=int(base["uid"]),
    )


def normalize_inventory_rows(rows: list[dict[str, str]], xml_dir: Path) -> list[SourceVm]:
    vms: list[SourceVm] = []
    for group in _group_rows(rows):
        base = group[0]
        xml_path = xml_dir / f"{base['vps_name']}.xml"
        vms.append(normalize_inventory_group(group, xml_path.read_text(encoding="utf-8")))
    return vms
