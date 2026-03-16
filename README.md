# smponline-utcon

uvicorn utcon.main:app --host 0.0.0.0 --port 8080

GET /v1/transactions/lookup/auction?item=DIAMOND&min_price=5

GET /v1/transactions/lookup/shop?item=DARK_OAK_LOG&shop_type=buyFromShop

# Service
```
administrator@prod-utcon-r001:/git/smponline-utcon$ cat /etc/systemd/system/utcon.service
[Unit]
Description=UTCON FastAPI Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=administrator
WorkingDirectory=/git/smponline-utcon
Environment=PYTHONUNBUFFERED=1
ExecStart=/bin/bash -lc 'mkdir -p logs && LOGFILE="logs/utcon_$(date +%%Y-%%m-%%d_%%H-%%M-%%S).log" && exec /usr/bin/python3 -m uvicorn utcon.main:app
--host 0.0.0.0 --port 8080 >> "$LOGFILE" 2>&1'
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
administrator@prod-utcon-r001:/git/smponline-utcon$
```
