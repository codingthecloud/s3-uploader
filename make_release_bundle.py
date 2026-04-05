from pathlib import Path
import shutil
import zipfile


PROJECT_ROOT = Path(__file__).resolve().parent
RELEASE_ROOT = PROJECT_ROOT / "release"
BUNDLE_DIR = RELEASE_ROOT / "s3-uploader-desktop"
FILES_TO_COPY = [
    "README.md",
    "s3uploader.py",
    "s3uploader_core.py",
    "s3uploader_ui.py",
    "requirements-desktop.txt",
    "build_windows.ps1",
    "s3uploader_ui.spec",
    ".gitignore",
]


def build_bundle():
    if BUNDLE_DIR.exists():
        shutil.rmtree(BUNDLE_DIR)
    BUNDLE_DIR.mkdir(parents=True, exist_ok=True)

    for relative_name in FILES_TO_COPY:
        source = PROJECT_ROOT / relative_name
        target = BUNDLE_DIR / relative_name
        if not source.exists():
            raise FileNotFoundError(f"Missing required file: {source}")
        shutil.copy2(source, target)

    zip_path = RELEASE_ROOT / "s3-uploader-desktop.zip"
    if zip_path.exists():
        zip_path.unlink()

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file_path in sorted(BUNDLE_DIR.iterdir()):
            archive.write(file_path, arcname=f"{BUNDLE_DIR.name}/{file_path.name}")

    return BUNDLE_DIR, zip_path


def main():
    bundle_dir, zip_path = build_bundle()
    print(f"Release folder created at: {bundle_dir}")
    print(f"Release zip created at: {zip_path}")


if __name__ == "__main__":
    main()
