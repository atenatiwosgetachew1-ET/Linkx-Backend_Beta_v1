import os
from pathlib import Path


def artifact_root():
    return Path(os.getenv("LINKX_ARTIFACT_ROOT", "/mnt/linkx-artifacts")).resolve()


def is_safe_filesystem_uri(uri):
    if not uri:
        return False
    path = Path(uri)
    if not path.is_absolute():
        path = artifact_root() / path
    try:
        path.resolve().relative_to(artifact_root())
    except ValueError:
        return False
    return True


def delete_filesystem_artifact(uri, dry_run=False):
    path = Path(uri)
    if not path.is_absolute():
        path = artifact_root() / path
    path = path.resolve()
    if not is_safe_filesystem_uri(str(path)):
        raise ValueError(f"unsafe_artifact_path:{path}")
    if dry_run:
        return {"path": str(path), "deleted": False, "dry_run": True, "exists": path.exists()}
    if not path.exists():
        return {"path": str(path), "deleted": False, "exists": False}
    if path.is_dir():
        import shutil
        shutil.rmtree(path)
    else:
        path.unlink()
    return {"path": str(path), "deleted": True, "exists": True}
