"""Collect a redacted diagnostics ZIP, without stopping or connecting to MT5."""
from __future__ import annotations
import argparse
import json
import re
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from observability import safe, VERSION
from review_report import build_report


def export(root, start, end, output):
    root = Path(root).resolve()
    secrets = []
    for path in (root / "config").glob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8-sig"))
            def collect(value):
                if isinstance(value, dict):
                    clean = safe(value)
                    for key, item in value.items():
                        if clean.get(str(key)) == "[REDACTED]" and isinstance(item, str) and len(item) >= 4:
                            secrets.append(item)
                        else:
                            collect(item)
                elif isinstance(value, list):
                    for item in value:
                        collect(item)
            collect(data)
        except (ValueError, OSError):
            pass
    def redact(text):
        for secret in sorted(set(secrets), key=len, reverse=True):
            text = text.replace(secret, "[REDACTED]")
        return safe(text)
    output = Path(output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    files, errors = [], []
    with zipfile.ZipFile(output, "x", compression=zipfile.ZIP_DEFLATED) as bundle:
        for directory in ("analysis_archive", "logs", "debug", "state", "config"):
            for path in sorted((root / directory).rglob("*")):
                if not path.is_file() or path.is_symlink() or path.suffix.lower() not in {".json", ".jsonl", ".log", ".txt"}:
                    continue
                relative = path.relative_to(root)
                # Date-partitioned archives/events are exact; console logs can
                # span several days and are included whole to preserve context.
                dates = [part for part in relative.parts if re.fullmatch(r"\d{4}-\d{2}-\d{2}", part)]
                if dates and not start <= dates[-1] <= end:
                    continue
                try:
                    raw = path.read_text(encoding="utf-8-sig", errors="replace")
                    if path.suffix.lower() == ".json":
                        try:
                            raw = json.dumps(safe(json.loads(raw)), ensure_ascii=False, indent=2)
                        except ValueError:
                            if directory == "config":
                                errors.append({"file": str(relative), "error": "Malformed config omitted to protect credentials"})
                                continue
                    bundle.writestr(str(relative).replace("\\", "/"), redact(raw))
                    files.append(str(relative))
                except OSError as error:
                    errors.append({"file": str(relative), "error": str(error)})
        report_text = json.dumps(safe(build_report(root, start, end)), ensure_ascii=False, indent=2)
        bundle.writestr("REPORT.json", redact(report_text))
        manifest_text = json.dumps(safe({"version": VERSION, "from": start, "to": end,
             "collected_at_utc": datetime.now(timezone.utc).isoformat(), "files": files, "errors": errors,
             "note": "Running process may append after collection. Whole console logs are included; no keys/passwords intended."}), ensure_ascii=False, indent=2)
        bundle.writestr("MANIFEST.json", redact(manifest_text))
    return output, len(files), errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    today = datetime.now(timezone.utc).date()
    parser.add_argument("--from", dest="start", default=str(today - timedelta(days=7)))
    parser.add_argument("--to", dest="end", default=str(today))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    for value in (args.start, args.end):
        datetime.strptime(value, "%Y-%m-%d")
    if args.start > args.end:
        parser.error("--from must be <= --to")
    output = args.output or args.root / "diagnostics" / ("robot_review_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".zip")
    path, count, errors = export(args.root, args.start, args.end, output)
    print(f"ZIP: {path}\nFiles: {count}\nRead warnings: {len(errors)}")


if __name__ == "__main__":
    main()
