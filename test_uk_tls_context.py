"""No external sockets: shared TLS context retains verification and socket ownership."""
import ssl
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock
import pytest
from test_coordinator_uk import scanner, forbid_unmocked_network


def test_context_initialized_once_across_workers(scanner, monkeypatch):
    monkeypatch.setattr(scanner, '_quality_tls_context', None)
    real_factory = ssl.create_default_context
    factory = Mock(side_effect=real_factory)
    monkeypatch.setattr(scanner.ssl, 'create_default_context', factory)
    with ThreadPoolExecutor(max_workers=4) as executor:
        contexts = list(executor.map(lambda _: scanner._get_quality_tls_context(), range(12)))
    assert all(context is contexts[0] for context in contexts)
    factory.assert_called_once()
    assert contexts[0].check_hostname
    assert contexts[0].verify_mode == ssl.CERT_REQUIRED


@pytest.mark.parametrize('failure', [None, ssl.SSLCertVerificationError('invalid certificate'), ssl.SSLError('bad TLS')])
def test_independent_sockets_closed_and_tls_errors_remain_failures(scanner, monkeypatch, failure):
    manager = scanner.ProxyManager()
    monkeypatch.setattr(scanner, 'proxy_manager', manager)
    monkeypatch.setattr(scanner, '_tcp_preflight_proxy', lambda *a, **kw: (True, None))
    tcp = Mock()
    tls = Mock()
    tls.do_handshake.side_effect = failure
    context = Mock()
    context.wrap_socket.return_value = tls
    monkeypatch.setattr(scanner, '_quality_tls_context', context)
    monkeypatch.setattr(scanner.socket, 'create_connection', Mock(return_value=tcp))
    monkeypatch.setattr(scanner, '_quality_recv_headers', lambda *a: b'HTTP/1.1 200 Connection established\r\n\r\n')
    proxy = 'http://8.8.8.8:80'
    assert scanner._quality_https_preflight_proxy(proxy) == ((True, None) if failure is None else (False, 'proxy_ssl'))
    assert context.wrap_socket.call_args.kwargs['server_hostname'] == scanner.PROXY_QUALITY_HOST
    assert context.wrap_socket.call_args.kwargs['do_handshake_on_connect'] is False
    tls.close.assert_called_once()
    if failure is not None:
        assert manager.quality_state(proxy) == 'bad'
        assert proxy not in manager.quality_ok_until
