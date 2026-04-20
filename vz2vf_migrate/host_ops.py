import re


SAFE_RBD_IMAGE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _validate_rbd_image_name(image_name: str) -> None:
    if not image_name or not SAFE_RBD_IMAGE_NAME_RE.fullmatch(image_name):
        raise ValueError(f"unsafe RBD image reference: {image_name!r}")


def build_rbd_copy_commands(
    pool: str,
    old_image: str,
    new_image: str,
) -> list[list[str]]:
    _validate_rbd_image_name(old_image)
    _validate_rbd_image_name(new_image)
    if old_image == new_image:
        raise ValueError(f"target RBD image must not match source RBD image: {new_image!r}")
    target_ref = f"{pool}/{new_image}"
    source_ref = f"{pool}/{old_image}"
    return [
        ["rbd", "rm", target_ref],
        ["rbd", "cp", source_ref, target_ref],
    ]


def build_shutdown_commands(vps_name: str) -> list[list[str]]:
    return [["virsh", "shutdown", vps_name]]


def build_domstate_command(vps_name: str) -> list[str]:
    return ["virsh", "domstate", vps_name]


def build_force_destroy_command(vps_name: str) -> list[str]:
    return ["virsh", "destroy", vps_name]


def _build_optional_package_install_attempt(
    install_command: str,
    output_var: str,
    unavailable_patterns: tuple[str, ...],
) -> str:
    unavailable_case = "|".join(unavailable_patterns)
    stable_install_command = f"LC_ALL=C {install_command}"
    return (
        f"{output_var}=$({stable_install_command} 2>&1) || "
        f'case "${output_var}" in '
        f"{unavailable_case}) ;; "
        f'*) echo "${output_var}" >&2; exit 1 ;; '
        "esac; "
    )


def _build_optional_package_install_command(
    check_command: str,
    install_command: str,
    output_var: str,
    unavailable_patterns: tuple[str, ...],
) -> str:
    return (
        f"if ! {check_command}; then "
        + _build_optional_package_install_attempt(
            install_command,
            output_var,
            unavailable_patterns,
        )
        + "fi; "
    )


def build_guest_inject_command(disk_path: str) -> list[str]:
    install_command = (
        "if command -v apt-get >/dev/null 2>&1; then "
        "if ! dpkg-query -W -f='${Status}' qemu-guest-agent 2>/dev/null | grep -q 'install ok installed'; "
        "then apt_qga_missing=1; else apt_qga_missing=0; fi; "
        "if ! dpkg-query -W -f='${Status}' cloud-init 2>/dev/null | grep -q 'install ok installed'; "
        "then apt_cloud_init_missing=1; else apt_cloud_init_missing=0; fi; "
        'if [ "$apt_qga_missing" -eq 1 ] || [ "$apt_cloud_init_missing" -eq 1 ]; '
        "then DEBIAN_FRONTEND=noninteractive apt-get -y update && "
        'if [ "$apt_qga_missing" -eq 1 ]; '
        "then DEBIAN_FRONTEND=noninteractive apt-get -y install qemu-guest-agent; fi && "
        + 'if [ "$apt_cloud_init_missing" -eq 1 ]; then '
        + _build_optional_package_install_attempt(
            "DEBIAN_FRONTEND=noninteractive apt-get -y install cloud-init",
            "apt_cloud_init_output",
            (
                '*"Unable to locate package cloud-init"*',
                '*"Package \'cloud-init\' has no installation candidate"*',
            ),
        )
        + "fi; "
        + "fi; "
        + "elif command -v dnf >/dev/null 2>&1; then "
        "if ! rpm -q qemu-guest-agent >/dev/null 2>&1; then dnf -y install qemu-guest-agent; fi && "
        + _build_optional_package_install_command(
            "rpm -q cloud-init >/dev/null 2>&1",
            "dnf -y install cloud-init",
            "dnf_cloud_init_output",
            (
                '*"No match for argument: cloud-init"*',
                '*"Unable to find a match: cloud-init"*',
                '*"No package cloud-init available"*',
            ),
        )
        + "elif command -v yum >/dev/null 2>&1; then "
        "if ! rpm -q qemu-guest-agent >/dev/null 2>&1; then yum -y install qemu-guest-agent; fi && "
        + _build_optional_package_install_command(
            "rpm -q cloud-init >/dev/null 2>&1",
            "yum -y install cloud-init",
            "yum_cloud_init_output",
            (
                '*"No package cloud-init available"*',
                '*"No Match for argument: cloud-init"*',
                '*"Unable to find a match: cloud-init"*',
            ),
        )
        + "elif command -v zypper >/dev/null 2>&1; then "
        "if ! rpm -q qemu-guest-agent >/dev/null 2>&1; then "
        "zypper --non-interactive install qemu-guest-agent; fi && "
        + _build_optional_package_install_command(
            "rpm -q cloud-init >/dev/null 2>&1",
            "zypper --non-interactive install cloud-init",
            "zypper_cloud_init_output",
            (
                '*"No provider of \'cloud-init\' found."*',
                '*"Package \'cloud-init\' not found."*',
            ),
        )
        + "elif command -v pacman >/dev/null 2>&1; then "
        "if ! pacman -Q qemu-guest-agent >/dev/null 2>&1; then pacman -Sy --noconfirm qemu-guest-agent; fi && "
        + _build_optional_package_install_command(
            "pacman -Q cloud-init >/dev/null 2>&1",
            "pacman -Sy --noconfirm cloud-init",
            "pacman_cloud_init_output",
            ('*"error: target not found: cloud-init"*',),
        )
        + "elif command -v apk >/dev/null 2>&1; then "
        "if ! apk info -e qemu-guest-agent >/dev/null 2>&1; then apk add qemu-guest-agent; fi && "
        + _build_optional_package_install_command(
            "apk info -e cloud-init >/dev/null 2>&1",
            "apk add cloud-init",
            "apk_cloud_init_output",
            ('*"cloud-init (no such package)"*', '*"cloud-init (no such package:"*'),
        )
        + "else exit 1; fi"
    )
    enable_command = (
        "if command -v systemctl >/dev/null 2>&1; then "
        "systemctl enable qemu-guest-agent || true; "
        "elif command -v rc-update >/dev/null 2>&1; then "
        "rc-update add qemu-guest-agent default || true; "
        "fi"
    )
    return [
        "virt-customize",
        "-a",
        disk_path,
        "--run-command",
        install_command,
        "--run-command",
        enable_command,
    ]


def should_skip_guest_injection(os_name: str) -> bool:
    lowered = os_name.lower()
    return any(token in lowered for token in ("mikrotik", "routeros", "windows")) or bool(
        re.search(r"\bchr\b", lowered)
    )
