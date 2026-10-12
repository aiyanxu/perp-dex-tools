import os


def _is_truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


# Lighter deployments. Accounts, balances, API keys, nonces and account indexes
# are fully separate per deployment; a transaction signed for one exchange is
# rejected by the other, so the chain id must match the target environment.
LIGHTER_MAINNET_URL = "https://mainnet.zklighter.elliot.ai"
LIGHTER_MAINNET_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_MAINNET_CHAIN_ID = 304

# Lighter on Robinhood Chain (lighter-rh): same transaction format, signer,
# REST API and WebSocket channels as mainnet, different deployment.
LIGHTER_RH_URL = "https://api.rh.lighter.xyz"
LIGHTER_RH_WS_URL = "wss://api.rh.lighter.xyz/stream"
LIGHTER_RH_CHAIN_ID = 466324


def lighter_environment() -> str:
    """Active Lighter deployment: 'rh' (Robinhood Chain) or 'mainnet' (default)."""
    env = (os.getenv("LIGHTER_ENV") or "mainnet").strip().lower()
    if env in ("rh", "lighter-rh", "robinhood", "robinhood-chain"):
        return "rh"
    return "mainnet"


def is_lighter_rh() -> bool:
    """True when the active Lighter deployment is Robinhood Chain."""
    return lighter_environment() == "rh"


def build_lighter_base_url() -> str:
    """REST base URL of the active Lighter deployment."""
    if is_lighter_rh():
        return os.getenv("LIGHTER_BASE_URL", "").strip() or LIGHTER_RH_URL
    return os.getenv("LIGHTER_BASE_URL", "").strip() or LIGHTER_MAINNET_URL


def build_lighter_ws_url() -> str:
    """Build Lighter WebSocket URL with optional legacy server ping fallback."""
    if is_lighter_rh():
        default_url = LIGHTER_RH_WS_URL
    else:
        default_url = LIGHTER_MAINNET_WS_URL

    base_url = os.getenv("LIGHTER_WS_URL", default_url).strip()
    if not base_url:
        base_url = default_url

    use_server_pings = _is_truthy(os.getenv("LIGHTER_WS_SERVER_PINGS", "false"))
    if not use_server_pings:
        return base_url

    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}server_pings=true"


def lighter_ws_connect_kwargs() -> dict:
    """
    WebSocket settings compatible with both old and new Lighter WS behavior.
    - Send client ping frames regularly (required by new server behavior).
    - Keep permessage-deflate enabled.
    """
    return {
        "ping_interval": 50,
        "ping_timeout": 20,
        "compression": "deflate",
        "max_queue": 1024,
    }


def create_lighter_signer_client(url: str, account_index: int, api_private_keys: dict,
                                chain_id: int = 0):
    """Create a lighter-sdk SignerClient for the given deployment.

    The SDK derives the signing chain id from the URL string ("mainnet" or
    "api" in url -> 304, else 300), which is wrong for the Robinhood Chain
    deployment (its signer verifies against chain id 466324). Build the
    client for the target deployment, then re-register every api key with
    the correct chain id before any tx is signed.
    """
    from lighter.signer_client import SignerClient

    client = SignerClient(url=url, account_index=account_index,
                          api_private_keys=api_private_keys)

    if chain_id and client.chain_id != chain_id:
        client.chain_id = chain_id
        for api_key_index in client.api_key_dict:
            client.create_client(api_key_index)

    return client


def create_lighter_signer_client_for_env(account_index: int, api_private_keys: dict):
    """Create a SignerClient for the active deployment (LIGHTER_ENV)."""
    if is_lighter_rh():
        return create_lighter_signer_client(
            url=build_lighter_base_url(),
            account_index=account_index,
            api_private_keys=api_private_keys,
            chain_id=LIGHTER_RH_CHAIN_ID,
        )
    return create_lighter_signer_client(
        url=build_lighter_base_url(),
        account_index=account_index,
        api_private_keys=api_private_keys,
        chain_id=LIGHTER_MAINNET_CHAIN_ID,
    )


def build_lighter_base_url_for_deployment(deployment: str) -> str:
    """REST base URL for an explicit deployment ('mainnet' or 'rh').

    Independent of the global LIGHTER_ENV, so one process can talk to both
    deployments at once. Overridable per deployment via
    LIGHTER_MAINNET_BASE_URL / LIGHTER_RH_BASE_URL.
    """
    if deployment == "rh":
        override = os.getenv("LIGHTER_RH_BASE_URL", "").strip()
        return override or LIGHTER_RH_URL
    if deployment == "mainnet":
        override = os.getenv("LIGHTER_MAINNET_BASE_URL", "").strip()
        return override or LIGHTER_MAINNET_URL
    raise ValueError(f"Unknown Lighter deployment: {deployment}")


def build_lighter_ws_url_for_deployment(deployment: str) -> str:
    """WebSocket URL for an explicit deployment ('mainnet' or 'rh')."""
    if deployment == "rh":
        override = os.getenv("LIGHTER_RH_WS_URL", "").strip()
        default_url = LIGHTER_RH_WS_URL
    elif deployment == "mainnet":
        override = os.getenv("LIGHTER_MAINNET_WS_URL", "").strip()
        default_url = LIGHTER_MAINNET_WS_URL
    else:
        raise ValueError(f"Unknown Lighter deployment: {deployment}")

    base_url = override or default_url

    use_server_pings = _is_truthy(os.getenv("LIGHTER_WS_SERVER_PINGS", "false"))
    if not use_server_pings:
        return base_url

    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}server_pings=true"


def create_lighter_signer_client_for_deployment(deployment: str, account_index: int,
                                                api_private_keys: dict, base_url: str = ""):
    """Create a SignerClient pinned to an explicit deployment ('mainnet' or 'rh').

    Unlike create_lighter_signer_client_for_env this does not depend on the
    global LIGHTER_ENV, so a process can hold clients for both deployments
    at once (e.g. mainnet maker + rh taker hedge mode).
    """
    if deployment == "rh":
        chain_id = LIGHTER_RH_CHAIN_ID
    elif deployment == "mainnet":
        chain_id = LIGHTER_MAINNET_CHAIN_ID
    else:
        raise ValueError(f"Unknown Lighter deployment: {deployment}")

    url = base_url or build_lighter_base_url_for_deployment(deployment)
    return create_lighter_signer_client(
        url=url,
        account_index=account_index,
        api_private_keys=api_private_keys,
        chain_id=chain_id,
    )
