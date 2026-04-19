from orbiter.api.auth import AuthConfig, extract_api_key, is_authorised, is_public_path


def test_extract_api_key_prefers_explicit_header():
    headers = {
        "x-orbiter-key": "abc",
        "authorization": "Bearer ignored",
    }
    assert extract_api_key(headers) == "abc"


def test_extract_api_key_reads_bearer_token():
    headers = {"authorization": "Bearer secret"}
    assert extract_api_key(headers) == "secret"


def test_public_paths_remain_open():
    assert is_public_path("/")
    assert is_public_path("/healthz")
    assert is_public_path("/auth/config")
    assert is_public_path("/ui/app.js")


def test_authorisation_disabled_allows_requests():
    config = AuthConfig(api_key=None)
    assert is_authorised(config, {}, "/runs")


def test_authorisation_requires_matching_key_for_private_paths():
    config = AuthConfig(api_key="top-secret")
    assert not is_authorised(config, {}, "/runs")
    assert not is_authorised(config, {"x-orbiter-key": "wrong"}, "/runs")
    assert is_authorised(config, {"x-orbiter-key": "top-secret"}, "/runs")
