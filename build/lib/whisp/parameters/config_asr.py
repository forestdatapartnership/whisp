from ..src.agstack_to_gee import start_agstack_session, get_agstack_token

email = "whisp2024@gmail.com"
phone_num = "0123456789"
password = "Whisp2024"
discoverable = True

# details for asset registry api calls
asset_registry_base = "https://api-ar.agstack.org"
user_registry_base = "https://user-registry.agstack.org"

session = start_agstack_session(email, password, user_registry_base)
token = get_agstack_token(email, password, asset_registry_base)
