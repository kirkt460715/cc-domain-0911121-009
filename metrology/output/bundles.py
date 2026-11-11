import hashlib
import io
import tarfile
from pathlib import Path
from ..errors import InputError, IntegrityError, StateError
from ..values import canonical, digest, strict_json
from .snapshots import atomic_write


class EvidenceBundles:
    def __init__(self, snapshots, campaigns):
        self.snapshots = snapshots
        self.campaigns = campaigns

    def build(self, campaign_id, snapshot_id):
        campaign = self.campaigns.get(campaign_id)
        if campaign["state"] != "approved":
            raise StateError("evidence bundle requires an approved campaign")
        snapshot = self.snapshots.get(snapshot_id)
        available = {
            (item["id"], item["revision"])
            for item in snapshot["payload"]["observations"]
        }
        required = {
            (item["id"], item["revision"]) for item in campaign["assessment"]["lineage"]
        }
        if not required.issubset(available):
            raise StateError("snapshot does not include reviewed campaign revisions")
        entries = {
            "campaign.json": canonical(campaign).encode("utf-8"),
            "decisions.json": canonical(self.campaigns.decisions(campaign_id)).encode(
                "utf-8"
            ),
            "snapshot.json": canonical(snapshot).encode("utf-8"),
            "windows.csv": self.snapshots.csv(snapshot_id).encode("utf-8"),
        }
        manifest = {
            "schema": 1,
            "campaign": campaign_id,
            "snapshot": snapshot_id,
            "members": [
                {
                    "name": name,
                    "bytes": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(),
                }
                for name, content in sorted(entries.items())
            ],
        }
        entries["manifest.json"] = canonical(manifest).encode("utf-8")
        buffer = io.BytesIO()
        with tarfile.open(
            fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT
        ) as archive:
            for name, content in sorted(entries.items()):
                info = tarfile.TarInfo(name)
                info.size = len(content)
                info.mode = 0o644
                info.mtime = 0
                info.uid = 0
                info.gid = 0
                archive.addfile(info, io.BytesIO(content))
        return buffer.getvalue()

    def export(self, campaign_id, snapshot_id, destination):
        payload = self.build(campaign_id, snapshot_id)
        result = atomic_write(destination, payload)
        result["sha256"] = hashlib.sha256(payload).hexdigest()
        return result


def verify_bundle(path):
    path = Path(path)
    if not path.is_file() or path.is_symlink() or path.stat().st_size > 64000000:
        raise InputError("bundle must be a regular file below size limit")
    members = {}
    try:
        with tarfile.open(path, mode="r:") as archive:
            for item in archive:
                if not item.isfile() or item.name not in {
                    "campaign.json",
                    "decisions.json",
                    "snapshot.json",
                    "windows.csv",
                    "manifest.json",
                }:
                    raise InputError("unexpected bundle member")
                if item.name in members or item.size > 32000000:
                    raise InputError("duplicate or oversized bundle member")
                handle = archive.extractfile(item)
                if handle is None:
                    raise InputError("unreadable bundle member")
                members[item.name] = handle.read()
    except tarfile.TarError as exc:
        raise InputError("invalid evidence archive") from exc
    expected = {
        "campaign.json",
        "decisions.json",
        "snapshot.json",
        "windows.csv",
        "manifest.json",
    }
    if set(members) != expected:
        raise IntegrityError("bundle omits required members")
    manifest = strict_json(members["manifest.json"].decode("utf-8"))
    if manifest.get("schema") != 1:
        raise InputError("unsupported evidence bundle schema")
    declared = manifest.get("members")
    if not isinstance(declared, list) or len(declared) != 4:
        raise IntegrityError("invalid bundle manifest")
    if {item["name"] for item in declared} != expected - {"manifest.json"}:
        raise IntegrityError("manifest member names differ")
    for item in declared:
        content = members[item["name"]]
        if (
            item["bytes"] != len(content)
            or item["sha256"] != hashlib.sha256(content).hexdigest()
        ):
            raise IntegrityError("bundle member digest mismatch", field=item["name"])
    campaign = strict_json(members["campaign.json"].decode("utf-8"))
    snapshot = strict_json(members["snapshot.json"].decode("utf-8"))
    decisions = strict_json(members["decisions.json"].decode("utf-8"))
    if (
        campaign["id"] != manifest["campaign"]
        or snapshot["payload"]["id"] != manifest["snapshot"]
    ):
        raise IntegrityError("bundle identities disagree")
    if (
        campaign["state"] != "approved"
        or not decisions
        or not decisions[-1]["approved"]
    ):
        raise IntegrityError("bundle lacks approval")
    if digest(snapshot["payload"]) != snapshot["fingerprint"]:
        raise IntegrityError("snapshot fingerprint is invalid")
    required = {
        (item["id"], item["revision"]) for item in campaign["assessment"]["lineage"]
    }
    included = {
        (item["id"], item["revision"]) for item in snapshot["payload"]["observations"]
    }
    if not required.issubset(included):
        raise IntegrityError("bundle snapshot omits reviewed evidence")
    return {
        "campaign": campaign["id"],
        "snapshot": snapshot["payload"]["id"],
        "observations": len(required),
        "members": len(members),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
