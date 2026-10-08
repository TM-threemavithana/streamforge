import grpc
import pytest

from streamforge_replay.grpc_client import IngestionClient, RetryPolicy


class RetryableError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


class InvalidError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.INVALID_ARGUMENT


def test_retry_reuses_the_exact_request_after_transient_outage():
    client = IngestionClient(
        "127.0.0.1:1",
        retry_policy=RetryPolicy(max_attempts=3, initial_backoff_seconds=0, maximum_backoff_seconds=0),
    )
    request = object()
    seen = []

    def method(value, *, timeout):
        seen.append((value, timeout))
        if len(seen) < 3:
            raise RetryableError()
        return "committed"

    try:
        assert client._call_with_retry(method, request) == "committed"
        assert [value for value, _ in seen] == [request, request, request]
    finally:
        client.close()


def test_non_retryable_error_is_not_retried():
    client = IngestionClient(
        "127.0.0.1:1",
        retry_policy=RetryPolicy(max_attempts=4, initial_backoff_seconds=0, maximum_backoff_seconds=0),
    )
    attempts = 0

    def method(_value, *, timeout):
        nonlocal attempts
        attempts += 1
        raise InvalidError()

    try:
        with pytest.raises(InvalidError):
            client._call_with_retry(method, object())
        assert attempts == 1
    finally:
        client.close()
