import hmac
import hashlib
import base64
import time

TIMESTAMP_FORMAT = "{:.3f}"

def generate_okx_signature(secret_key: str, method: str, path: str, body: str = ""):
    timestamp = TIMESTAMP_FORMAT.format(time.time())
    payload = f"{timestamp}{method.upper()}{path}{body}".encode()
    signature = base64.b64encode(
        hmac.new(secret_key.encode(), payload, hashlib.sha256).digest()
    ).decode()
    return timestamp, signature

if __name__ == "__main__":
    SECRET_KEY = "283C0A2342D369B9C36B352814C22D74"
    METHOD = "GET"
    PATH = "/api/v5/account/balance"
    BODY = ""

    ts, sig = generate_okx_signature(SECRET_KEY, METHOD, PATH, BODY)
    print("timestamp:", ts)
    print("signature:", sig)
