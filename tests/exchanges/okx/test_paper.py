import httpx
import pytest
from flaky import flaky

from exchanges.okx.paper import OkxPaperClient, OkxClientError


@pytest.fixture
def mock_httpx_client(mocker):
    return mocker.patch("httpx.Client")


def test_okx_client_error_payload():
    """Ensure the error payload is stored in the exception."""
    error = OkxClientError("test error", payload={"code": "999"})
    assert error.payload.get("code") == "999"


@flaky(max_runs=3)
def test_request_retry_on_5xx_error(mock_httpx_client, caplog):
    """
    Verify that the client retries on 5xx server errors.
    """
    mock_response = httpx.Response(
        503, request=httpx.Request("GET", "https://www.okx.com")
    )
    mock_httpx_client.return_value.request.side_effect = [
        httpx.HTTPStatusError(
            "Service Unavailable",
            request=mock_response.request,
            response=mock_response,
        ),
        httpx.Response(
            200,
            json={"code": "0", "msg": "", "data": [{"balance": "100"}]},
            request=mock_response.request,
        ),
    ]

    client = OkxPaperClient()
    client.authenticate(
        mocker.MagicMock(api_key="key", api_secret="secret", passphrase="pass")
    )

    # This should fail initially, then succeed on the second attempt
    balances = client.fetch_balances()

    assert mock_httpx_client.return_value.request.call_count == 2
    assert "Retrying" in caplog.text
    assert balances == [{"balance": "100"}]


def test_request_no_retry_on_4xx_error(mock_httpx_client):
    """
    Verify that the client does not retry on 4xx client errors.
    """
    mock_response = httpx.Response(
        400, request=httpx.Request("GET", "https://www.okx.com")
    )
    mock_httpx_client.return_value.request.side_effect = httpx.HTTPStatusError(
        "Bad Request",
        request=mock_response.request,
        response=mock_response,
    )

    client = OkxPaperClient()
    client.authenticate(
        mocker.MagicMock(api_key="key", api_secret="secret", passphrase="pass")
    )

    with pytest.raises(httpx.HTTPStatusError):
        client.fetch_balances()

    assert mock_httpx_client.return_value.request.call_count == 1
