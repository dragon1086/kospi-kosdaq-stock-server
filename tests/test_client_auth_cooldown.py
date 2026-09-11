"""Offline regression tests: failed singleton initialization must not hammer login."""

import importlib.util
from pathlib import Path
import sys
from unittest.mock import Mock
from datetime import datetime, timedelta

import pytest


@pytest.fixture
def client_module(monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "krx_auth_cooldown_test_module",
        Path(__file__).resolve().parents[1] / "krx_data_client.py",
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def test_repeated_auth_failure_attempts_login_only_once(client_module, monkeypatch):
    k = client_module
    constructor = Mock(side_effect=k.KRXAuthError("private login details"))
    monkeypatch.setattr(k, "KRXDataClient", constructor)
    for _ in range(20):
        with pytest.raises(k.KRXAuthError):
            k._get_client()
    assert constructor.call_count == 1


def test_cooldown_expires_and_success_is_reused(client_module, monkeypatch):
    k = client_module
    clock = [100.0]
    monkeypatch.setattr(k.time, "monotonic", lambda: clock[0])
    success = object()
    constructor = Mock(side_effect=[k.KRXAuthError("failed"), success])
    monkeypatch.setattr(k, "KRXDataClient", constructor)
    with pytest.raises(k.KRXAuthError):
        k._get_client()
    clock[0] += k.AUTH_FAILURE_COOLDOWN_SECONDS - 1
    with pytest.raises(k.KRXAuthError):
        k._get_client()
    clock[0] += 1
    assert k._get_client() is success
    assert k._get_client() is success
    assert constructor.call_count == 2


def test_explicit_invalidation_allows_repaired_auth_retry(client_module, monkeypatch):
    k = client_module
    success = object()
    constructor = Mock(side_effect=[k.KRX2FARequiredError(), success])
    monkeypatch.setattr(k, "KRXDataClient", constructor)
    with pytest.raises(k.KRX2FARequiredError):
        k._get_client()
    k.clear_auth_failure_cooldown()
    assert k._get_client() is success
    assert constructor.call_count == 2


def test_non_auth_failure_is_not_negative_cached(client_module, monkeypatch):
    k = client_module
    constructor = Mock(side_effect=ValueError("unrelated failure"))
    monkeypatch.setattr(k, "KRXDataClient", constructor)
    for _ in range(2):
        with pytest.raises(ValueError):
            k._get_client()
    assert constructor.call_count == 2


def test_cached_failure_does_not_retain_private_exception(client_module, monkeypatch):
    k = client_module
    monkeypatch.setattr(k, "KRXDataClient", Mock(side_effect=k.KRXAuthError("secret")))
    with pytest.raises(k.KRXAuthError):
        k._get_client()
    with pytest.raises(k.KRXAuthError) as exc:
        k._get_client()
    assert "secret" not in str(exc.value)
    assert exc.value.__context__ is None


@pytest.mark.parametrize("force", [False, True])
def test_browser_disabled_fails_before_lock_or_browser(client_module, monkeypatch, force):
    k = client_module
    monkeypatch.setenv("KRX_ALLOW_BROWSER_LOGIN", "0")
    manager = k.KRXAuthManager.__new__(k.KRXAuthManager)
    manager.LOCK_PATH = Mock()
    manager._acquire_lock = Mock()
    manager._login_async_kakao = Mock()
    manager._login_async_krx = Mock()
    with pytest.raises(k.KRXAuthError, match="KRX_ALLOW_BROWSER_LOGIN"):
        manager._login_with_lock(force)
    manager.LOCK_PATH.touch.assert_not_called()
    manager._acquire_lock.assert_not_called()
    manager._login_async_kakao.assert_not_called()
    manager._login_async_krx.assert_not_called()


@pytest.mark.parametrize("recent", [False, True])
def test_browser_disabled_reuses_valid_cached_session(client_module, monkeypatch, recent):
    k = client_module
    monkeypatch.setenv("KRX_ALLOW_BROWSER_LOGIN", "0")
    manager = k.KRXAuthManager.__new__(k.KRXAuthManager)
    manager._load_session = Mock(return_value=True)
    manager._last_validated = datetime.now() - timedelta(minutes=1 if recent else 10)
    manager._validate_session = Mock(return_value=True)
    manager._update_last_validated = Mock()
    manager.LOCK_PATH = Mock()
    assert manager.login() is True
    manager.LOCK_PATH.touch.assert_not_called()
    assert manager._validate_session.call_count == (0 if recent else 1)


def test_browser_disabled_invalid_session_fails_without_cleanup(client_module, monkeypatch):
    k = client_module
    monkeypatch.setenv("KRX_ALLOW_BROWSER_LOGIN", "0")
    manager = k.KRXAuthManager.__new__(k.KRXAuthManager)
    manager._load_session = Mock(return_value=True)
    manager._last_validated = None
    manager._validate_session = Mock(return_value=False)
    manager._cleanup_session_files = Mock()
    manager.LOCK_PATH = Mock()
    with pytest.raises(k.KRXAuthError, match="KRX_ALLOW_BROWSER_LOGIN"):
        manager.login()
    manager.LOCK_PATH.touch.assert_not_called()
    manager._cleanup_session_files.assert_not_called()


@pytest.mark.parametrize("flag", [None, "1"])
def test_browser_login_default_keeps_existing_path(client_module, monkeypatch, flag):
    k = client_module
    if flag is None:
        monkeypatch.delenv("KRX_ALLOW_BROWSER_LOGIN", raising=False)
    else:
        monkeypatch.setenv("KRX_ALLOW_BROWSER_LOGIN", flag)
    manager = k.KRXAuthManager.__new__(k.KRXAuthManager)
    manager.LOCK_PATH = Mock()
    manager.LOCK_PATH.touch.side_effect = RuntimeError("existing lock path reached")
    with pytest.raises(RuntimeError, match="existing lock path reached"):
        manager._login_with_lock()
