# smponline-utcon

uvicorn utcon.main:app --host 0.0.0.0 --port 8080

GET /v1/transactions/lookup/auction?item=DIAMOND&min_price=5

GET /v1/transactions/lookup/shop?item=DARK_OAK_LOG&shop_type=buyFromShop