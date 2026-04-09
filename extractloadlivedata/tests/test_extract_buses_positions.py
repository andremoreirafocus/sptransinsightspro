from src.services.extract_buses_positions import (
    buses_positions_response_is_valid,
    extract_buses_positions,
    extract_buses_positions_with_retries,
    get_buses_positions_summary,
)


class FakeResponse:
    def __init__(self, status_code=200, text="true", json_data=None):
        self.status_code = status_code
        self.text = text
        self._json_data = json_data or {}

    def json(self):
        return self._json_data


class FakeSession:
    def __init__(self, auth_ok=True, pos_ok=True, payload=None):
        self.auth_ok = auth_ok
        self.pos_ok = pos_ok
        self.payload = payload or {"hr": "09:00", "l": []}
        self.post_calls = []
        self.get_calls = []

    def post(self, url):
        self.post_calls.append(url)
        if self.auth_ok:
            return FakeResponse(status_code=200, text="true")
        return FakeResponse(status_code=401, text="false")

    def get(self, url):
        self.get_calls.append(url)
        if self.pos_ok:
            return FakeResponse(status_code=200, json_data=self.payload)
        return FakeResponse(status_code=500, json_data={})


def test_buses_positions_response_is_valid_ok():
    payload = {"hr": "09:00", "l": []}
    assert buses_positions_response_is_valid(payload) is True


def test_buses_positions_response_is_valid_missing_fields():
    payload = {"hr": "09:00"}
    assert buses_positions_response_is_valid(payload) is False


def test_get_buses_positions_summary_counts():
    payload = {
        "hr": "09:00",
        "l": [{"vs": [{}, {}]}, {"vs": [{}]}],
    }
    reference_time, total_vehicles = get_buses_positions_summary(payload)
    assert reference_time == "09:00"
    assert total_vehicles == 3


def test_extract_buses_positions_auth_failure():
    session = FakeSession(auth_ok=False)
    result = extract_buses_positions("http://api", "token", session=session)
    assert result is None


def test_extract_buses_positions_success():
    payload = {"hr": "09:00", "l": []}
    session = FakeSession(auth_ok=True, pos_ok=True, payload=payload)
    result = extract_buses_positions("http://api", "token", session=session)
    assert result == payload


def test_extract_buses_positions_with_retries_success(monkeypatch):
    payload = {"hr": "09:00", "l": []}
    session = FakeSession(auth_ok=True, pos_ok=True, payload=payload)
    config = {
        "TOKEN": "token",
        "API_BASE_URL": "http://api",
        "API_MAX_RETRIES": 3,
    }
    monkeypatch.setattr("src.services.extract_buses_positions.time.sleep", lambda *_: None)
    result = extract_buses_positions_with_retries(config, session=session)
    assert result == payload


def test_extract_buses_positions_with_retries_max(monkeypatch):
    session = FakeSession(auth_ok=True, pos_ok=False)
    config = {
        "TOKEN": "token",
        "API_BASE_URL": "http://api",
        "API_MAX_RETRIES": 2,
    }
    monkeypatch.setattr("src.services.extract_buses_positions.time.sleep", lambda *_: None)
    result = extract_buses_positions_with_retries(config, session=session)
    assert result is None
