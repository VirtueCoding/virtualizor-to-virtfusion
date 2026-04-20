from dataclasses import dataclass, field


@dataclass(frozen=True)
class SourceDisk:
    slot: int
    target_dev: str
    source_path: str
    rbd_name: str
    size_gb: int
    is_primary: bool


@dataclass(frozen=True)
class SourceVm:
    vpsid: int
    vps_name: str
    uuid: str
    hostname: str
    os_name: str
    cores: int
    ram_mb: int
    bandwidth_gb: int
    network_speed_kbps: int
    upload_speed_kbps: int
    disks: list[SourceDisk]
    primary_ipv4: str | None
    additional_ipv4: list[str]
    ipv6_addresses: list[str]
    ipv6_subnet: str | None
    ipv6_cidr: int | None
    virtualizor_uid: int


@dataclass(frozen=True)
class WhmcsMatch:
    source_name: str
    client_id: int
    firstname: str
    lastname: str
    email: str
    product_id: int
    product_name: str
    hosting_id: int
    domain: str


@dataclass(frozen=True)
class VirtFusionServer:
    server_id: int
    uuid: str
    name: str
    hostname: str | None
    storage_names: list[str]
    primary_ipv4: str | None
    ipv4_addresses: list[str]


@dataclass(frozen=True)
class VmResult:
    vpsid: int
    vps_name: str
    hostname: str
    old_rbd_image: str
    new_vf_server_id: str
    new_rbd_image: str
    ip_addresses: str
    status: str
    notes: str


@dataclass
class VmState:
    stages: dict[str, dict] = field(default_factory=dict)
