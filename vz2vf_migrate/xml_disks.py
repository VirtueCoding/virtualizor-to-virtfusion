from xml.etree import ElementTree as ET

from vz2vf_migrate.models import SourceDisk


def parse_rbd_disks(xml_text: str) -> list[SourceDisk]:
    root = ET.fromstring(xml_text)
    disks: list[SourceDisk] = []

    for disk in root.findall("./devices/disk"):
        if disk.attrib.get("device") != "disk":
            continue

        source = disk.find("source")
        target = disk.find("target")
        if source is None or target is None:
            continue

        source_path = source.attrib.get("dev")
        target_dev = target.attrib.get("dev")
        if not source_path or not target_dev or not source_path.startswith("/dev/rbd/"):
            continue

        disks.append(
            SourceDisk(
                slot=len(disks) + 1,
                target_dev=target_dev,
                source_path=source_path,
                rbd_name=source_path.rsplit("/", 1)[-1],
                size_gb=0,
                is_primary=not disks,
            )
        )

    return disks
