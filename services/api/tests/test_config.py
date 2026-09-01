import config


def test_jwt_secrets_are_default_true_when_either_secret_is_the_shipped_default():
    cfg = config.load_config()
    assert cfg.jwt_access_secret == config.DEFAULT_JWT_ACCESS_SECRET
    assert cfg.jwt_refresh_secret == config.DEFAULT_JWT_REFRESH_SECRET
    assert cfg.jwt_secrets_are_default() is True


def test_jwt_secrets_are_default_false_when_both_overridden(monkeypatch):
    monkeypatch.setenv("JWT_ACCESS_SECRET", "a-real-random-secret")
    monkeypatch.setenv("JWT_REFRESH_SECRET", "another-real-random-secret")
    cfg = config.load_config()
    assert cfg.jwt_secrets_are_default() is False


def test_jwt_secrets_are_default_true_when_only_one_overridden(monkeypatch):
    monkeypatch.setenv("JWT_ACCESS_SECRET", "a-real-random-secret")
    monkeypatch.delenv("JWT_REFRESH_SECRET", raising=False)
    cfg = config.load_config()
    assert cfg.jwt_secrets_are_default() is True


def test_environment_defaults_to_development(monkeypatch):
    monkeypatch.delenv("API_ENVIRONMENT", raising=False)
    cfg = config.load_config()
    assert cfg.environment == "development"
