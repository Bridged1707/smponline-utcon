def normalize_shop(shop: dict) -> dict:

    owner = shop.get("owner") or {}
    location = shop.get("location") or {}
    item = shop.get("item") or {}

    return {
        "shop_id": shop.get("id"),

        "owner_name": owner.get("name"),
        "owner_uuid": owner.get("uuid"),

        "world": location.get("world"),
        "x": location.get("x"),
        "y": location.get("y"),
        "z": location.get("z"),

        "type": shop.get("type"),
        "price": shop.get("price"),
        "remaining": shop.get("remaining"),

        "item_type": item.get("type"),
        "item_quantity": item.get("quantity"),
        "item_name": item.get("name"),
        "item_snbt": item.get("snbt"),
    }