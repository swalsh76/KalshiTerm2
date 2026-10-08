from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID
from kalshiterm_server import tls
from kalshiterm_server.cli import app
from typer.testing import CliRunner

NOW = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)
posix_only = pytest.mark.skipif(
    __import__("sys").platform == "win32", reason="POSIX file permissions"
)


def parsed(hosts: list[str], ips: list[str]) -> x509.Certificate:
    cert_pem, _ = tls.generate(hosts, ips, NOW)
    return x509.load_pem_x509_certificate(cert_pem)


def test_the_certificate_is_strict_about_what_it_is_good_for() -> None:
    cert = parsed(["mac-studio"], ["192.168.1.20"])
    constraints = cert.extensions.get_extension_for_class(x509.BasicConstraints)
    assert constraints.critical and constraints.value.ca is False  # can never sign anything
    usage = cert.extensions.get_extension_for_class(x509.KeyUsage).value
    assert usage.digital_signature and not usage.key_cert_sign and not usage.key_encipherment
    ext = cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    assert list(ext) == [ExtendedKeyUsageOID.SERVER_AUTH]
    assert isinstance(cert.public_key(), ec.EllipticCurvePublicKey)
    assert cert.public_key().curve.name == "secp256r1"  # type: ignore[union-attr]
    assert isinstance(cert.signature_hash_algorithm, hashes.SHA256)
    assert cert.issuer == cert.subject  # self-signed


def test_validity_is_one_year_with_a_few_minutes_of_clock_tolerance() -> None:
    cert = parsed(["mac-studio"], [])
    assert cert.not_valid_after_utc == NOW + timedelta(days=365)
    assert NOW - timedelta(minutes=10) < cert.not_valid_before_utc < NOW


def test_names_cover_what_clients_use_plus_local_and_loopback() -> None:
    info = tls.describe(
        tls.generate(["Mac-Studio", "nas.example.com"], ["192.168.1.20", "fe80::1"], NOW)[0]
    )
    assert info.dns_names == ("localhost", "mac-studio", "mac-studio.local", "nas.example.com")
    assert info.ip_addresses == ("127.0.0.1", "::1", "192.168.1.20", "fe80::1")


def test_duplicate_names_are_collapsed() -> None:
    dns, ips = tls.normalise_names(
        ["a", "A", "a.", "localhost"], ["10.0.0.1", "10.0.0.1", "127.0.0.1"]
    )
    assert dns == ["localhost", "a", "a.local"] and ips == ["127.0.0.1", "::1", "10.0.0.1"]


@pytest.mark.parametrize(
    "bad",
    [
        "",
        " ",
        "has space",
        "under_score",
        "*.local",
        "-lead",
        "trail-",
        "a..b",
        "é",
        "x" * 64,
        "a" * 254,
    ],
)
def test_unusable_host_names_are_refused(bad: str) -> None:
    with pytest.raises(tls.TlsError):
        tls.normalise_names([bad], [])


@pytest.mark.parametrize("bad", ["", "not-an-ip", "300.1.1.1", "0.0.0.0", "::", "224.0.0.1"])
def test_unusable_addresses_are_refused(bad: str) -> None:
    with pytest.raises(tls.TlsError):
        tls.normalise_names([], [bad])


def test_the_fingerprint_is_the_sha256_of_the_der_certificate() -> None:
    cert_pem, _ = tls.generate(["mac-studio"], [], NOW)
    info = tls.describe(cert_pem)
    der = x509.load_pem_x509_certificate(cert_pem).public_bytes(serialization.Encoding.DER)
    assert info.fingerprint == tls.fingerprint_of(der)
    assert len(info.fingerprint) == 32 * 3 - 1 and info.fingerprint == info.fingerprint.upper()
    assert tls.describe(tls.generate(["mac-studio"], [], NOW)[0]).fingerprint != info.fingerprint


def test_each_certificate_has_its_own_key_and_the_key_matches() -> None:
    cert_pem, key_pem = tls.generate(["a"], [], NOW)
    key = serialization.load_pem_private_key(key_pem, password=None)
    cert = x509.load_pem_x509_certificate(cert_pem)
    assert key.public_key().public_numbers() == cert.public_key().public_numbers()  # type: ignore[union-attr]
    other_pem, other_key = tls.generate(["a"], [], NOW)
    assert other_key != key_pem and other_pem != cert_pem


def test_garbage_is_not_a_certificate() -> None:
    with pytest.raises(tls.TlsError):
        tls.describe(b"not a certificate")


def test_the_names_to_carry_over_exclude_the_automatic_ones() -> None:
    info = tls.describe(tls.generate(["mac-studio", "nas.example.com"], ["192.168.1.20"], NOW)[0])
    assert tls.user_names(info) == (["mac-studio", "nas.example.com"], ["192.168.1.20"])


@posix_only
def test_files_are_written_with_the_right_permissions_and_replaced_atomically(
    tmp_path: Path,
) -> None:
    first = tls.write_certificate(tmp_path, ["mac-studio"], [], NOW)
    assert (tmp_path / tls.KEY_FILE).stat().st_mode & 0o777 == 0o600
    assert (tmp_path / tls.CERT_FILE).stat().st_mode & 0o777 == 0o644
    second = tls.write_certificate(tmp_path, ["mac-studio"], [], NOW)
    assert second.fingerprint != first.fingerprint
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        tls.CERT_FILE,
        tls.KEY_FILE,
    ]  # no leftovers


def test_cert_show_and_rotate_commands(tmp_path: Path) -> None:
    (tmp_path / "secrets").mkdir()
    tls.write_certificate(tmp_path / "secrets", ["mac-studio"], ["192.168.1.20"])
    runner = CliRunner()
    shown = runner.invoke(app, ["cert", "show", "--out", str(tmp_path)])
    old = tls.describe((tmp_path / "secrets" / tls.CERT_FILE).read_bytes())
    assert shown.exit_code == 0 and old.fingerprint in shown.output
    assert "mac-studio.local" in shown.output and "192.168.1.20" in shown.output

    rotated = runner.invoke(app, ["cert", "rotate", "--out", str(tmp_path)])
    new = tls.describe((tmp_path / "secrets" / tls.CERT_FILE).read_bytes())
    assert rotated.exit_code == 0
    assert new.fingerprint != old.fingerprint  # a new certificate...
    assert (new.dns_names, new.ip_addresses) == (old.dns_names, old.ip_addresses)  # ...same names
    assert old.fingerprint in rotated.output and new.fingerprint in rotated.output
    assert "confirms the new fingerprint" in rotated.output

    renamed = runner.invoke(app, ["cert", "rotate", "--out", str(tmp_path), "--host", "other"])
    assert "other.local" in renamed.output and "mac-studio" not in renamed.output


def test_cert_commands_report_missing_certificates_plainly(tmp_path: Path) -> None:
    runner = CliRunner()
    assert runner.invoke(app, ["cert", "show", "--out", str(tmp_path)]).exit_code == 1
    result = runner.invoke(app, ["cert", "rotate", "--out", str(tmp_path / "x")])
    assert result.exit_code == 1 and "--host" in result.output
