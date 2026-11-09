import json
import zipfile

from procurement.gold.application.retirement import archive, audit


def test_code_archive_is_recoverable_and_data_is_never_included(tmp_path):
    root = tmp_path / "repo"
    legacy = root / "src/procurement/gold"
    legacy.mkdir(parents=True)
    (legacy / "old.py").write_text("def old(): return 1\n")
    (legacy / "v2").mkdir()
    (legacy / "v2" / "new.py").write_text("VALUE=2\n")
    (root / "src" / "consumer.py").write_text("from procurement.gold.old import old\n")
    (root / "data").mkdir()
    (root / "data" / "keep.parquet").write_text("private source")
    for name in ("scripts", "tests"):
        (root / name).mkdir()
    report = audit(root)
    assert len(report["files"]) == 1 and len(report["blockers"]) == 1
    result = archive(root, tmp_path / "legacy.zip")
    assert not result["deletion_performed"]
    assert (legacy / "old.py").exists() and (root / "data" / "keep.parquet").exists()
    with zipfile.ZipFile(tmp_path / "legacy.zip") as bundle:
        assert len(bundle.namelist()) == 2
        assert "data/keep.parquet" not in bundle.namelist()
        assert json.loads(bundle.read("retirement-manifest.json"))["blockers"]
