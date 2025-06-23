#app/core/sso_auth.py
from fastapi import Request
from jose import JWTError, jwt
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

# === Config ===
# Replace these with your actual values from your Identity Provider (IdP)
SECRET_KEY = "your-secret-key-from-idp"
ALGORITHM = "HS256"  # or RS256 if using public/private keys

async def sso_authentication_middleware(request: Request, call_next):
    auth_header = request.headers.get("Authorization")

    if not auth_header or not auth_header.startswith("Bearer "):
        return JSONResponse(status_code=401, content={"detail": "Missing or invalid Authorization header"})

    token = auth_header.split(" ")[1]

    try:
        # Decode token and validate
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        request.state.user = payload  # Save user info in request
    except JWTError:
        return JSONResponse(status_code=403, content={"detail": "Invalid or expired token"})

    return await call_next(request)
