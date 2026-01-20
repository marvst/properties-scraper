# config.py

BASE_URL = "https://www.apolar.com.br/alugar/apartamento/2-quartos?mensal&country=Brasil&garage=1&price_max=R%24%203.100,00&area_min=50,00%20m%C2%B2&include_condominium_price=true"
CSS_SELECTOR = "[class^='property-component']"
REQUIRED_KEYS = [
    "full_address",
    "rent_price_brl",
    "description",
]