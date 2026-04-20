import json
from dataclasses import dataclass
from json import JSONDecodeError

from vz2vf_migrate.config import VirtFusionMasterConfig
from vz2vf_migrate.models import SourceVm
from vz2vf_migrate.runner import CommandRunner

SSH_CONNECT_TIMEOUT_SECONDS = 30
HELPER_TIMEOUT_SECONDS = 120


def build_preserve_ipv6_payload(vm: SourceVm, server_id: int) -> dict:
    if not vm.ipv6_subnet or vm.ipv6_cidr is None or not vm.ipv6_addresses:
        raise ValueError(
            f"IPv6 assignment is incomplete for VM {vm.vpsid}; subnet, cidr, and addresses are required"
        )
    return {
        "action": "preserve_ipv6",
        "serverId": server_id,
        "subnet": vm.ipv6_subnet,
        "cidr": vm.ipv6_cidr,
        "addresses": vm.ipv6_addresses,
    }


@dataclass(frozen=True)
class VirtFusionMasterClient:
    runner: CommandRunner
    config: VirtFusionMasterConfig

    def preserve_ipv6(self, vm: SourceVm, server_id: int) -> dict:
        result = self.runner.run_with_input(
            self._command(),
            json.dumps(build_preserve_ipv6_payload(vm, server_id)),
            timeout=HELPER_TIMEOUT_SECONDS,
        )
        response = None
        stdout = result.stdout.strip()
        if stdout:
            try:
                response = json.loads(stdout)
            except JSONDecodeError:
                response = None

        if result.returncode != 0:
            if isinstance(response, dict) and response.get("success") is False:
                error = response.get("error")
                message = response.get("message") or (
                    error.get("message") if isinstance(error, dict) else None
                ) or "unknown error"
                raise RuntimeError(f"VirtFusion master helper reported failure: {message}")
            detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
            raise RuntimeError(f"VirtFusion master helper failed: {detail}")

        try:
            response = response if response is not None else json.loads(result.stdout)
        except JSONDecodeError as exc:
            raise RuntimeError("VirtFusion master helper returned invalid JSON") from exc

        if not isinstance(response, dict):
            raise RuntimeError("VirtFusion master helper response must be a JSON object")
        if response.get("success") is False:
            error = response.get("error")
            message = response.get("message") or (
                error.get("message") if isinstance(error, dict) else None
            ) or "unknown error"
            raise RuntimeError(f"VirtFusion master helper reported failure: {message}")

        data = response.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("VirtFusion master helper response data must be an object")
        return data

    def _command(self) -> list[str]:
        return [
            self.config.ssh_bin,
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={SSH_CONNECT_TIMEOUT_SECONDS}",
            "-p",
            str(self.config.port),
            f"{self.config.user}@{self.config.host}",
            self.config.php_bin,
            self.config.helper_path,
        ]
