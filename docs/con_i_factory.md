# Connect ODS Server

There are two ways to connect to an ASAM ODS server: directly using `ConI`, or using the `ConIFactory` helper class.

---

## Direct ConI Usage

`ConI` is the core session class. You can use it directly without the factory for full control over authentication.

### Username / Password

The simplest way to connect is by passing a `(username, password)` tuple as the `auth` argument:

```python
from odsbox import ConI

with ConI(
    url="https://my.asam.server.com:8443/api",
    auth=("employee_id", "password")
) as con_i:
    units = con_i.query_data({"AoUnit": {}})
```

**For self-signed certificates:**

```python
with ConI(
    url="https://localhost:8443/api",
    auth=("user", "pass"),
    verify_certificate=False  # Development only!
) as con_i:
    pass
```

---

### Custom Auth (`requests.auth.AuthBase`)

For token-based or other auth schemes, pass any `requests.auth.AuthBase` subclass as the `auth` argument.

**Bearer token example:**

```python
import requests
from odsbox import ConI

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

with ConI(
    url="https://my.asam.server.com:8443/api",
    auth=BearerAuth("YOUR_BEARER_TOKEN")
) as con_i:
    units = con_i.query_data({"AoUnit": {}})
```

You can implement any `requests.auth.AuthBase` subclass this way — digest auth, HMAC signing, API key headers, etc.

---

### Custom Session

If you have a fully pre-configured `requests.Session` (e.g., from an OAuth2 library), pass it as `custom_session`. When `custom_session` is provided, the `auth` and `verify_certificate` parameters are ignored.

```python
import requests
from odsbox import ConI

session = requests.Session()
session.headers.update({"Authorization": "Bearer YOUR_TOKEN"})
session.verify = False  # or path to CA bundle

with ConI(
    url="https://my.asam.server.com:8443/api",
    custom_session=session
) as con_i:
    units = con_i.query_data({"AoUnit": {}})
```

This is particularly useful when integrating with OAuth2 libraries that provide their own `requests.Session` subclass.

---

## ConIFactory Usage

`ConIFactory` is a convenience factory class that wraps `ConI` and handles the authentication setup for the most common scenarios. All methods return a ready-to-use `ConI` connection instance.

### Authentication Flows

#### 1. Basic Authentication (Username/Password)

Use this method for direct username/password authentication to an ODS server.

**When to use:**
- Direct user credentials available
- Development and testing environments
- Legacy systems requiring basic auth

**Method signature:**
```python
@staticmethod
def basic(
    url: str,
    username: str,
    password: str,
    *,
    verify_certificate: bool = True,
    **kwargs: Any,
) -> Any
```

**Example:**
```python
from odsbox.con_i_factory import ConIFactory

con = ConIFactory.basic(
    url="https://my.asam.server.com:8443/api",
    username="employee_id",
    password="password"
)

with con:
    # Use the connection
    # Connection is automatically closed when exiting the with block
    pass
```

**For self-signed certificates:**
```python
con = ConIFactory.basic(
    url="https://localhost:8443/api",
    username="user",
    password="pass",
    verify_certificate=False  # Development only!
)
```

---

#### 2. Machine-to-Machine (M2M) Authentication

Use this method for service-to-service communication where a client authenticates directly with the OAuth2 token endpoint using a client ID and secret, without user interaction.

**When to use:**
- Service-to-service communication
- Automated scripts and batch jobs
- Background processing
- No user interaction available

**Method signature:**
```python
@staticmethod
def m2m(
    url: str,
    token_endpoint: str,
    client_id: str,
    client_secret: str,
    *,
    scope: list[str] | None = None,
    verify_certificate: bool = True,
    **kwargs: Any,
) -> Any
```

**Example with Keyring:**
```python
from odsbox.con_i_factory import ConIFactory
from keyring import get_password

token_endpoint = "https://example.com/auth/realms/myrealm/protocol/openid-connect/token"
client_id = "f0a8cec0-e980-48c4-9898-8a11f40da518"

# Retrieve secret from secure storage (keyring, password manager, etc.)
client_secret = get_password(token_endpoint, client_id)
assert client_secret is not None, f"Secret for {client_id} not found"

con = ConIFactory.m2m(
    url="https://my.asam.server.com:8443/api",
    token_endpoint=token_endpoint,
    client_id=client_id,
    client_secret=client_secret
)

with con:
    # Use the connection
    pass
```

**Custom scopes:**
```python
con = ConIFactory.m2m(
    url="https://my.asam.server.com:8443/api",
    token_endpoint=token_endpoint,
    client_id=client_id,
    client_secret=client_secret,
    scope=["api", "admin"]  # Custom scopes
)
```

---

#### 3. OIDC (OpenID Connect) Authentication

Use this method for user-facing applications where users authenticate through their browser. The OIDC endpoints are automatically discovered via WebFinger if not explicitly provided.

**When to use:**
- Interactive user login flows
- Web applications
- Desktop applications with browser support
- Federated authentication
- Single sign-on (SSO) scenarios

**Method signature:**
```python
@staticmethod
def oidc(
    url: str,
    client_id: str,
    redirect_uri: str,
    *,
    redirect_url_allow_insecure: bool = False,
    client_secret: str | None = None,
    scope: list[str] | None = None,
    authorization_endpoint: str | None = None,
    token_endpoint: str | None = None,
    login_timeout: int = 60,
    verify_certificate: bool = True,
    webfinger_path_prefix: str = "",
    **kwargs: Any,
) -> Any
```

##### Automatic Endpoint Discovery (Recommended)

The simplest approach. ConIFactory discovers endpoints via WebFinger automatically:

```python
from odsbox.con_i_factory import ConIFactory

con = ConIFactory.oidc(
    url="https://my.asam.server.com:8443/api",
    client_id="f243866c-76b7-4e51-a16c-1d6bfe8f0c93",
    redirect_uri="http://127.0.0.1:1234",
    redirect_url_allow_insecure=True,
    webfinger_path_prefix="/ods"  # Optional, depends on server configuration
)

with con:
    # Browser opens for login
    # Once user authenticates, connection is ready to use
    pass
```

##### Explicit Endpoints

If you prefer to skip WebFinger discovery or know the endpoints:

```python
con = ConIFactory.oidc(
    url="https://server:8015/api",
    client_id="your-client-id",
    redirect_uri="http://127.0.0.1:1234",
    redirect_url_allow_insecure=True,
    authorization_endpoint="https://auth.example.com/authorize",
    token_endpoint="https://auth.example.com/token"
)

with con:
    pass
```

###### Local Development with Insecure Redirects

For development with `localhost` redirects (HTTP instead of HTTPS):

```python
con = ConIFactory.oidc(
    url="https://server:8015/api",
    client_id="dev-client-id",
    redirect_uri="http://127.0.0.1:1234",
    redirect_url_allow_insecure=True,  # Allow HTTP for local development
    webfinger_path_prefix="/ods"
)

with con:
    pass
```

##### Custom Scopes

```python
con = ConIFactory.oidc(
    url="https://server:8015/api",
    client_id="client-id",
    redirect_uri="http://127.0.0.1:1234",
    redirect_url_allow_insecure=True,
    scope=["openid", "profile", "email", "custom_api"]
)
```

---

#### WebFinger Discovery

ConIFactory includes a built-in WebFinger discovery feature that automatically locates OIDC endpoints on the server.

**Access the discovery function directly:**
```python
from odsbox.con_i_factory import ConIFactory

auth_endpoint, token_endpoint = ConIFactory.discover_endpoints(
    ods_base_url="https://server:8015/api",
    webfinger_path_prefix="/ods"  # Optional
)

print(f"Auth: {auth_endpoint}")
print(f"Token: {token_endpoint}")
```

This is useful if you need to:
- Inspect the discovered endpoints before creating a connection
- Cache endpoints for later use
- Validate server configuration

---

### Common Patterns

#### Pattern 1: Secret Retrieval from Keyring

Store secrets securely and retrieve them at runtime:

```python
from odsbox.con_i_factory import ConIFactory
from keyring import get_password

# For basic auth
url = "https://server:8443/api"
user = "employee_id"
password = get_password(url, user)
assert password is not None, f"Password for {user} not found"

con = ConIFactory.basic(url=url, username=user, password=password)
```

#### Pattern 2: Configuration from Environment

Load connection details from environment variables:

```python
import os
from odsbox.con_i_factory import ConIFactory

con = ConIFactory.oidc(
    url=os.getenv("ODS_URL"),
    client_id=os.getenv("ODS_OIDC_CLIENT_ID"),
    webfinger_path_prefix=os.getenv("ODS_OIDC_WEBFINGER_PREFIX", "")
)
```

#### Pattern 3: Fallback Authentication

Try OIDC first, fall back to basic auth:

```python
from odsbox.con_i_factory import ConIFactory

try:
    con = ConIFactory.oidc(...)
except Exception:
    con = ConIFactory.basic(...)
```

#### Pattern 4: Additional ConI Parameters

Pass advanced options to the underlying ConI:

```python
con = ConIFactory.basic(
    url="https://server/api",
    username="user",
    password="pass",
    timeout=30,           # Additional ConI parameter
    verify_certificate=False
)
```

---

### Error Handling

#### OIDC Errors

```python
from odsbox.con_i_factory import ConIFactory

try:
    con = ConIFactory.oidc(
        url="https://server/api",
        client_id="client-id",
        redirect_uri="http://invalid-uri"  # Missing port
    )
except ValueError as e:
    print(f"Invalid redirect URI: {e}")

try:
    con = ConIFactory.oidc(
        url="https://server/api",
        client_id="client-id",
        login_timeout=5
    )
except ValueError as e:
    print(f"Login timed out: {e}")
```

#### WebFinger Discovery Errors

```python
try:
    auth_ep, token_ep = ConIFactory.discover_endpoints(
        ods_base_url="https://invalid-server/api"
    )
except ValueError as e:
    print(f"Discovery failed: {e}")
    # Fall back to explicit endpoints
```

---

##### Performance Considerations

- **WebFinger Discovery:** First call discovers endpoints, subsequent calls reuse them
- **Token Caching:** OAuth2Session automatically caches and refreshes tokens
- **Connection Pooling:** ConI handles HTTP connection pooling

---

### Security Best Practices

1. **Never hardcode secrets:**
   ```python
   # ❌ DO NOT DO THIS
   con = ConIFactory.m2m(..., client_secret="hardcoded_secret")

   # ✅ Use keyring or environment variables instead
   con = ConIFactory.m2m(..., client_secret=get_password(...))
   ```

2. **Verify certificates in production:**
   ```python
   # ❌ Development only
   con = ConIFactory.basic(..., verify_certificate=False)

   # ✅ Production (default)
   con = ConIFactory.basic(...)  # verify_certificate=True by default
   ```

3. **Use HTTPS redirect URIs for OIDC:**
   ```python
   # ❌ Development only
   con = ConIFactory.oidc(..., redirect_uri="http://localhost:1234", redirect_url_allow_insecure=True)

   # ✅ Production
   con = ConIFactory.oidc(..., redirect_uri="https://app.example.com/callback")
   ```

4. **OIDC: Use client secrets securely:**
   ```python
   from keyring import get_password

   client_secret = get_password(token_endpoint, client_id)
   con = ConIFactory.oidc(..., client_secret=client_secret)
   ```

---

### Troubleshooting

#### "OAUTHLIB_INSECURE_TRANSPORT" Error

**Problem:** OIDC with HTTP redirect fails

**Solution:** Use `redirect_url_allow_insecure=True` for local development

```python
con = ConIFactory.oidc(
    ...,
    redirect_uri="http://127.0.0.1:1234",
    redirect_url_allow_insecure=True
)
```

#### WebFinger Discovery Fails

**Problem:** "OIDC issuer not found" or "WebFinger request failed"

**Solution:** Try explicit endpoints or adjust `webfinger_path_prefix`

```python
# Option 1: Use explicit endpoints
con = ConIFactory.oidc(
    url=url,
    client_id=client_id,
    authorization_endpoint="https://...",
    token_endpoint="https://..."
)

# Option 2: Adjust WebFinger path prefix
con = ConIFactory.oidc(
    url=url,
    client_id=client_id,
    webfinger_path_prefix="/ods"  # or "/api", etc.
)
```

#### Certificate Verification Fails

**Problem:** "SSL: CERTIFICATE_VERIFY_FAILED" or similar

**Solution:**

For development:
```python
con = ConIFactory.basic(..., verify_certificate=False)
```

For production, ensure your CA certificate bundle is up-to-date.

---

### API Reference

See the docstrings in `con_i_factory.py` for full API details:

```python
from odsbox.con_i_factory import ConIFactory

help(ConIFactory.basic)
help(ConIFactory.m2m)
help(ConIFactory.oidc)
help(ConIFactory.discover_endpoints)
```


Use this method for direct username/password authentication to an ODS server.

**When to use:**
- Direct user credentials available
- Development and testing environments
- Legacy systems requiring basic auth

**Method signature:**
```python
@staticmethod
def basic(
    url: str,
    username: str,
    password: str,
    *,
    verify_certificate: bool = True,
    **kwargs: Any,
) -> Any
```

**Example:**
```python
from odsbox.con_i_factory import ConIFactory

con = ConIFactory.basic(
    url="https://my.asam.server.com:8443/api",
    username="employee_id",
    password="password"
)

with con:
    # Use the connection
    # Connection is automatically closed when exiting the with block
    pass
```

**For self-signed certificates:**
```python
con = ConIFactory.basic(
    url="https://localhost:8443/api",
    username="user",
    password="pass",
    verify_certificate=False  # Development only!
)
```
