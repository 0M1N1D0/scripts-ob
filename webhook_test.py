from fastapi import FastAPI, Request, HTTPException
import hmac
import hashlib

app = FastAPI()
SECRET = '2d001d6f-0adc-4896-bb1c-97a2f7e240ea'

def verify_hmac_signature(payload, signature, secret):
    mac = hmac.new(secret.encode(), msg=payload, digestmod=hashlib.sha256)
    return hmac.compare_digest(mac.hexdigest(), signature)

@app.post("/webhookrastreos")
async def handle_webhook(request: Request):
    raw_body = await request.body()
    signature = request.headers.get("HTTP-X-Hmac-SHA256")

    if not signature or not verify_hmac_signature(raw_body, signature, SECRET):
        raise HTTPException(status_code=400, detail="Invalid HMAC signature")

    payload = await request.json()
    print(payload)
    return {"message": "Webhook received successfully"}
