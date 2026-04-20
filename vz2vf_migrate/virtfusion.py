import json
from dataclasses import dataclass
from typing import Protocol
from urllib import error, request

from vz2vf_migrate.models import SourceVm, VirtFusionServer


class ResponseBody(Protocol):
    def read(self) -> bytes:
        pass


class VirtFusionResponseError(RuntimeError):
    pass


def _require_dict(value: object, context: str) -> dict:
    if not isinstance(value, dict):
        raise VirtFusionResponseError(f"{context} must be an object")
    return value


def _require_list(value: object, context: str) -> list:
    if not isinstance(value, list):
        raise VirtFusionResponseError(f"{context} must be a list")
    return value


def parse_response_data_id(response: dict | None, context: str) -> int:
    payload = _require_dict(response, f"{context} response")
    data = _require_dict(payload.get("data"), f"{context} data")
    if "id" not in data:
        raise VirtFusionResponseError(f"{context} data must include id")
    try:
        return int(data["id"])
    except (TypeError, ValueError) as exc:
        raise VirtFusionResponseError(f"{context} data id must be an integer") from exc


def build_server_payload(
    vm: SourceVm,
    user_id: int,
    hypervisor_id: int,
    package_id: int,
    storage_profile_id: int | None = None,
    network_profile_id: int | None = None,
    additional_storage1_profile_id: int | None = None,
    additional_storage2_profile_id: int | None = None,
) -> dict:
    if not vm.disks:
        raise ValueError(f"VM {vm.vpsid} must include at least one disk")
    if len(vm.disks) > 3:
        raise ValueError(f"VM {vm.vpsid} has more than 3 disks; VirtFusion create API supports only 3")
    payload = {
        "packageId": package_id,
        "userId": user_id,
        "hypervisorId": hypervisor_id,
        "storage": vm.disks[0].size_gb,
        "traffic": vm.bandwidth_gb,
        "memory": vm.ram_mb,
        "cpuCores": vm.cores,
        "ipv4": 1 if vm.primary_ipv4 else 0,
    }
    if storage_profile_id is not None:
        payload["storageProfile"] = storage_profile_id
    if network_profile_id is not None:
        payload["networkProfile"] = network_profile_id
    if len(vm.disks) > 1:
        payload["additionalStorage1Enable"] = True
        payload["additionalStorage1Capacity"] = vm.disks[1].size_gb
        if additional_storage1_profile_id is not None:
            payload["additionalStorage1Profile"] = additional_storage1_profile_id
    if len(vm.disks) > 2:
        payload["additionalStorage2Enable"] = True
        payload["additionalStorage2Capacity"] = vm.disks[2].size_gb
        if additional_storage2_profile_id is not None:
            payload["additionalStorage2Profile"] = additional_storage2_profile_id
    payload["networkSpeedInbound"] = vm.network_speed_kbps
    payload["networkSpeedOutbound"] = 0 if vm.upload_speed_kbps == -1 else vm.upload_speed_kbps
    return payload


def build_user_payload(firstname: str, lastname: str, email: str, client_id: int) -> dict:
    return {
        "name": f"{firstname} {lastname}".strip(),
        "email": email,
        "extRelationId": client_id,
    }


def parse_server_details(response: dict) -> VirtFusionServer:
    data = _require_dict(response.get("data"), "VirtFusion server details data")
    if "id" not in data:
        raise VirtFusionResponseError("VirtFusion server details data must include id")
    if "uuid" not in data:
        raise VirtFusionResponseError("VirtFusion server details data must include uuid")
    storage = _require_list(data.get("storage"), "VirtFusion server details storage")
    network = _require_dict(data.get("network"), "VirtFusion server details network")
    interfaces = _require_list(network.get("interfaces"), "VirtFusion server details network.interfaces")
    ipv4_addresses: list[str] = []
    if interfaces:
        primary_interface = _require_dict(interfaces[0], "VirtFusion server details network.interfaces[0]")
        ipv4 = _require_list(primary_interface.get("ipv4", []), "VirtFusion server details network.interfaces[0].ipv4")
        for index, item in enumerate(ipv4):
            if not isinstance(item, dict) or not item.get("address"):
                raise VirtFusionResponseError(
                    f"VirtFusion server details network.interfaces[0].ipv4[{index}] must include address"
                )
        ipv4_addresses = [str(item["address"]) for item in ipv4]
    primary_ipv4 = ipv4_addresses[0] if ipv4_addresses else None
    storage_names: list[str] = []
    for index, item in enumerate(storage):
        if not isinstance(item, dict) or not item.get("name"):
            raise VirtFusionResponseError(f"VirtFusion server details storage[{index}] must include name")
        storage_names.append(str(item["name"]))
    if not storage_names:
        raise VirtFusionResponseError("VirtFusion server details storage must include at least one disk name")
    return VirtFusionServer(
        server_id=int(data["id"]),
        uuid=str(data["uuid"]),
        name=data.get("name", ""),
        hostname=data.get("hostname"),
        storage_names=storage_names,
        primary_ipv4=primary_ipv4,
        ipv4_addresses=ipv4_addresses,
    )


def _load_response_body(response: ResponseBody) -> dict | None:
    text = response.read().decode("utf-8")
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {"message": text}
    return parsed if isinstance(parsed, dict) else {"data": parsed}


@dataclass(frozen=True)
class VirtFusionClient:
    base_url: str
    token: str
    timeout: int = 30

    def _request(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict | None]:
        payload = None
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        if body is not None:
            payload = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = request.Request(f"{self.base_url.rstrip('/')}{path}", data=payload, method=method, headers=headers)
        try:
            with request.urlopen(req, timeout=self.timeout) as response:
                return response.status, _load_response_body(response)
        except error.HTTPError as exc:
            return exc.code, _load_response_body(exc)

    def find_user_by_ext_relation(self, client_id: int) -> tuple[int, dict | None]:
        return self._request("GET", f"/users/{client_id}/byExtRelation")

    def create_user(self, firstname: str, lastname: str, email: str, client_id: int) -> tuple[int, dict | None]:
        return self._request("POST", "/users", build_user_payload(firstname, lastname, email, client_id))

    def create_server(self, payload: dict) -> tuple[int, dict | None]:
        return self._request("POST", "/servers", payload)

    def get_server(self, server_id: int, remote_state: bool = False) -> tuple[int, dict | None]:
        path = f"/servers/{server_id}"
        if remote_state:
            path = f"{path}?remoteState=true"
        return self._request("GET", path)

    def build_server(
        self,
        server_id: int,
        os_template_id: int,
        name: str,
        hostname: str,
    ) -> tuple[int, dict | None]:
        body: dict = {
            "operatingSystemId": os_template_id,
            "name": name,
            "hostname": hostname,
        }
        return self._request("POST", f"/servers/{server_id}/build", body)

    def modify_server_name(self, server_id: int, name: str) -> tuple[int, dict | None]:
        return self._request("PUT", f"/servers/{server_id}/modify/name", {"name": name})

    def add_ipv4(self, server_id: int, addresses: list[str], interface: str = "secondary") -> tuple[int, dict | None]:
        return self._request("POST", f"/servers/{server_id}/ipv4", {"ip": addresses, "interface": interface})

    def delete_ipv4(self, server_id: int, addresses: list[str]) -> tuple[int, dict | None]:
        return self._request("DELETE", f"/servers/{server_id}/ipv4", {"ip": addresses})

    def boot_server(self, server_id: int) -> tuple[int, dict | None]:
        return self._request("POST", f"/servers/{server_id}/power/boot")

    def shutdown_server(self, server_id: int) -> tuple[int, dict | None]:
        return self._request("POST", f"/servers/{server_id}/power/shutdown")

    def poweroff_server(self, server_id: int) -> tuple[int, dict | None]:
        return self._request("POST", f"/servers/{server_id}/power/poweroff")
