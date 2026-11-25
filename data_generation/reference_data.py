"""
Fixed lookup lists used by generate_star_schema.py.

Everything here is made up for the class project. Courier, seller and product
names are invented, and exchange rates are rounded values, not live rates.
"""

# TransactionDT counts seconds from this moment (same as ingestion/scripts/config.py)
REFERENCE_DATE = "2026-01-01 00:00:00"

# country: share of customers, currency, USD->local rate, UTC offset (hours),
# phone prefix, first IP octets, cities with weights
COUNTRIES = {
    "Kenya":        dict(weight=0.28, currency="KES", fx=129.0,  utc=3, phone="+254",
                         ip=[41, 105, 197], cities={"Nairobi": 0.55, "Mombasa": 0.18, "Kisumu": 0.12, "Nakuru": 0.10, "Eldoret": 0.05}),
    "Nigeria":      dict(weight=0.24, currency="NGN", fx=1550.0, utc=1, phone="+234",
                         ip=[102, 105, 197], cities={"Lagos": 0.50, "Abuja": 0.20, "Port Harcourt": 0.12, "Ibadan": 0.10, "Kano": 0.08}),
    "South Africa": dict(weight=0.14, currency="ZAR", fx=18.2,   utc=2, phone="+27",
                         ip=[41, 102, 196], cities={"Johannesburg": 0.45, "Cape Town": 0.30, "Durban": 0.15, "Pretoria": 0.10}),
    "Ghana":        dict(weight=0.10, currency="GHS", fx=15.4,   utc=0, phone="+233",
                         ip=[41, 154], cities={"Accra": 0.60, "Kumasi": 0.30, "Tamale": 0.10}),
    "Uganda":       dict(weight=0.08, currency="UGX", fx=3700.0, utc=3, phone="+256",
                         ip=[41, 102], cities={"Kampala": 0.70, "Entebbe": 0.15, "Gulu": 0.15}),
    "Tanzania":     dict(weight=0.08, currency="TZS", fx=2600.0, utc=3, phone="+255",
                         ip=[41, 154, 196], cities={"Dar es Salaam": 0.60, "Arusha": 0.25, "Dodoma": 0.15}),
    "Rwanda":       dict(weight=0.04, currency="RWF", fx=1350.0, utc=2, phone="+250",
                         ip=[41, 197], cities={"Kigali": 0.85, "Huye": 0.15}),
    "Egypt":        dict(weight=0.04, currency="EGP", fx=48.5,   utc=2, phone="+20",
                         ip=[41, 156, 197], cities={"Cairo": 0.60, "Alexandria": 0.30, "Giza": 0.10}),
}

# Payment methods people use in each country (weights add up to 1).
# CARD is split into visa / mastercard etc. using the card4 column.
PAYMENT_MIX = {
    "Kenya":        {"M-PESA": 0.55, "CARD": 0.20, "CASH_ON_DELIVERY": 0.15, "AIRTEL_MONEY": 0.10},
    "Nigeria":      {"CARD": 0.40, "BANK_TRANSFER": 0.25, "CASH_ON_DELIVERY": 0.25, "MOBILE_MONEY": 0.10},
    "South Africa": {"CARD": 0.65, "INSTANT_EFT": 0.20, "CASH_ON_DELIVERY": 0.10, "MOBILE_MONEY": 0.05},
    "Ghana":        {"MOBILE_MONEY": 0.55, "CARD": 0.20, "CASH_ON_DELIVERY": 0.25},
    "Uganda":       {"MOBILE_MONEY": 0.60, "AIRTEL_MONEY": 0.15, "CARD": 0.10, "CASH_ON_DELIVERY": 0.15},
    "Tanzania":     {"M-PESA": 0.45, "AIRTEL_MONEY": 0.15, "MOBILE_MONEY": 0.10, "CARD": 0.10, "CASH_ON_DELIVERY": 0.20},
    "Rwanda":       {"MOBILE_MONEY": 0.65, "CARD": 0.20, "CASH_ON_DELIVERY": 0.15},
    "Egypt":        {"CARD": 0.35, "CASH_ON_DELIVERY": 0.45, "MOBILE_MONEY": 0.20},
}

# ProductCD (from the original data) -> product categories it covers.
# The real meaning of ProductCD is hidden in the source data, so this mapping is
# our own choice. W and H are mostly shipped goods, C, R and S mostly digital.
PRODUCT_GROUPS = {
    "W": {"Electronics": 0.30, "Fashion": 0.25, "Home & Kitchen": 0.20, "Beauty": 0.15, "Groceries": 0.10},
    "H": {"Home & Kitchen": 0.40, "Furniture": 0.30, "Appliances": 0.30},
    "C": {"Airtime & Data": 0.40, "Digital Vouchers": 0.20, "Gaming": 0.10, "Electronics": 0.30},
    "R": {"Travel & Bus Tickets": 0.40, "Event Tickets": 0.20, "Fashion": 0.20, "Beauty": 0.20},
    "S": {"Gift Cards": 0.40, "Subscriptions": 0.20, "Electronics": 0.20, "Home & Kitchen": 0.20},
}

# Categories that are shipped by a courier (the rest are delivered digitally)
PHYSICAL_CATEGORIES = {"Electronics", "Fashion", "Home & Kitchen", "Beauty", "Groceries",
                       "Furniture", "Appliances"}

# Product name pieces for each category: (adjectives, nouns, typical USD price range)
PRODUCT_WORDS = {
    "Electronics":          (["Smart", "Wireless", "Solar", "Pro", "Mini", "Ultra"], ["Phone", "Earbuds", "Power Bank", "Tablet", "Speaker", "Smartwatch", "Charger", "Laptop"], (8, 900)),
    "Fashion":              (["Classic", "Ankara", "Kitenge", "Slim-fit", "Leather", "Cotton"], ["Dress", "Shirt", "Sneakers", "Handbag", "Jacket", "Sandals", "Kaftan"], (5, 150)),
    "Home & Kitchen":       (["Non-stick", "Steel", "Ceramic", "Family", "Compact"], ["Cookware Set", "Blender", "Water Filter", "Kettle", "Dinner Set", "Jiko Stove"], (6, 250)),
    "Beauty":               (["Shea", "Herbal", "Natural", "Glow", "Argan"], ["Body Lotion", "Hair Oil", "Face Cream", "Perfume", "Soap Pack"], (3, 80)),
    "Groceries":            (["Organic", "Family", "Premium", "Value"], ["Rice 5kg", "Cooking Oil 3L", "Coffee Beans", "Tea Leaves", "Maize Flour 10kg"], (2, 60)),
    "Furniture":            (["Mahogany", "Rattan", "Modern", "Office"], ["Chair", "Sofa", "Bed Frame", "Desk", "Wardrobe"], (40, 1200)),
    "Appliances":           (["Energy-saver", "Inverter", "Double-door", "Portable"], ["Fridge", "Washing Machine", "Microwave", "Air Cooler", "Generator"], (60, 1500)),
    "Airtime & Data":       (["Daily", "Weekly", "Monthly", "Unlimited"], ["Data Bundle", "Airtime Top-up", "Voice Bundle"], (1, 60)),
    "Digital Vouchers":     (["Prepaid", "Instant"], ["Electricity Token", "Shopping Voucher", "Pay-TV Voucher"], (2, 200)),
    "Gaming":               (["Premium", "Starter"], ["Game Credits", "Game Pass"], (2, 100)),
    "Travel & Bus Tickets": (["Express", "Overnight", "Economy", "Business"], ["Bus Ticket", "Flight Ticket", "Train Ticket", "Ferry Ticket"], (5, 900)),
    "Event Tickets":        (["VIP", "Regular", "Early-bird"], ["Concert Ticket", "Festival Pass", "Match Ticket"], (5, 300)),
    "Gift Cards":           (["Digital", "Store"], ["Gift Card"], (5, 500)),
    "Subscriptions":        (["Monthly", "Annual"], ["Music Subscription", "Streaming Subscription", "Cloud Storage Plan"], (2, 120)),
}

# Invented seller and courier names
SELLER_WORDS_1 = ["Savanna", "Baobab", "Kilima", "Sahel", "Zuri", "Jua", "Nyota", "Amani", "Upendo", "Mto",
                  "Simba", "Tembo", "Ubuntu", "Imani", "Kijani", "Afya", "Bahari", "Duma", "Mawingu", "Pamoja"]
SELLER_WORDS_2 = ["Traders", "Electronics", "Fashion House", "Supplies", "Mart", "Store", "Hub", "Outlet",
                  "Emporium", "Goods", "Market", "Depot", "Boutique", "Wholesale", "Express"]
COURIERS = ["Savanna Express", "Baobab Logistics", "Kilima Couriers", "Delta Dispatch", "Sahel Freight"]

COMPLAINT_CHANNELS = {"APP": 0.45, "WEB_CHAT": 0.20, "CALL_CENTRE": 0.20, "EMAIL": 0.10, "SOCIAL_MEDIA": 0.05}

# Average hours to resolve a complaint, by category
RESOLUTION_HOURS = {
    "Missing Package": 96, "Refund Issue": 120, "Wrong Item": 72, "Damaged Product": 72,
    "Suspicious Seller": 168, "Unauthorized Charge": 48,
    "Late Delivery": 48, "Chargeback Dispute": 240,
}
