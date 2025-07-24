import pytest
import requests

from unittest.mock import patch, MagicMock
from pypeh.adapters.outbound.persistence.hosts import HostFactory


class MockClass:
    pass


@pytest.mark.web
class TestWebIO:
    def test_make_connection(self):
        host = HostFactory.default()
        _ = host.close()
        assert not host._open_session

    def test_resolve_url_success(self):
        webio_instance = HostFactory.default()

        url = "https://example.com/test.json"
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.content = b'{"key": "value"}'
        mock_response.raise_for_status.return_value = None

        with patch.object(webio_instance.session, "get", return_value=mock_response):
            response = webio_instance.resolve_url(url)
            assert response.status_code == 200
            assert response.headers["Content-Type"] == "application/json"

    def test_retrieve_data_with_adapter(self):
        # simple example
        url = "http://maps.googleapis.com/maps/api/directions/json?origin=Chicago,Il&destination=Los+Angeles,CA"
        # resp = requests.get(url=url, params=params)
        webio_instance = HostFactory.default(verify_ssl=False)
        data = webio_instance.retrieve_data(url, format_type="application/json", target_class=None)
        assert data
