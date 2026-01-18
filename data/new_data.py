import csv
import os
import random
from datetime import datetime, timedelta

# ==========
# SETTINGS
# ==========
NUM_NEW_PRODUCTS = 10
NUM_ORDERS = 2000
prev_file_path = "products/ingestion_date=2026-01-01/products.csv"
new_file_path = "products/ingestion_date=2026-02-01/products.csv"
sales_file_path = "sales/ingestion_date=2026-02-01/sales.csv"

categories = {
    "Limpeza": ["cozinha", "lavanderia", "casa"],
    "Higiene": ["banho", "banheiro", "higiene_pessoal"],
    "Alimentos": ["cafe_da_manha", "lanche", "refeicao"],
    "Bebidas": ["alcoolica", "nao_alcoolica"],
    "Pet": ["cachorro", "gato"],
    "Bebe": ["fralda", "higiene_bebe"],
    "Hortifruti": ["frutas", "legumes", "verduras"],
    "Acougue": ["carnes", "aves"],
    "Padaria": ["paes", "bolos"],
    "Utilidades": ["cozinha", "organizacao"]
}

# ========================
# READ EXISTING PRODUCTS
# ========================
existing_products = []
try:
    with open(prev_file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        existing_products = list(reader)
except FileNotFoundError:
    pass

# ========================
# Generate new products
# ========================
next_id = len(existing_products) + 1
new_products = []
for i in range(NUM_NEW_PRODUCTS):
    product_id = f"P{next_id:03}"
    category = random.choice(list(categories.keys()))
    usage_type = random.choice(categories[category])
    new_products.append({
        "product_id": product_id,
        "product_name": f"Produto {product_id}",
        "category": category,
        "usage_type": usage_type
    })
    next_id += 1

# ===============
# FULL SNAPSHOT
# ===============
all_products = existing_products + new_products

os.makedirs(os.path.dirname(new_file_path), exist_ok=True)
with open(new_file_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["product_id", "product_name", "category", "usage_type"])
    writer.writeheader()
    writer.writerows(all_products)

print(f"Produtos existentes: {len(existing_products)}")
print(f"Novos produtos adicionados: {len(new_products)}")
print(f"Total no snapshot: {len(all_products)}")

# ==========================
# MIXED COMBOS (old + new)
# ==========================
mixed_combos = [
    ["P001", "P120"],                 # café (antigo) + suco (novo)
    ["P002", "P121", "P090"],         # pão (antigo) + biscoito (novo) + arroz (antigo)
    ["P030", "P125"],                 # ração cachorro (antigo) + detergente (novo)
    ["P050", "P060", "P122"],         # fralda + lenço (antigos) + leite (novo)
    ["P070", "P080", "P126"],         # shampoo + condicionador (antigos) + esponja (novo)
    ["P100", "P127", "P128"],         # feijão (antigo) + carne bovina + linguiça (novos)
    ["P111", "P129", "P130"],         # salgadinho (antigo) + farofa + refrigerante (novos)
    ["P113", "P114", "P118", "P119"], # alface + cebola (antigos) + maçã + banana (novos)
]

# =============================
# READ LAST EXISTING ORDER_ID
# =============================
last_order_num = 0
prev_sales_file = "sales/ingestion_date=2026-01-01/sales.csv"

try:
    with open(prev_sales_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        prev_sales = list(reader)
        if prev_sales:
            # retrieves the highest order_id number already used
            last_order_num = max(int(s["order_id"][1:]) for s in prev_sales)
except FileNotFoundError:
    pass

# ====================
# Generate new sales
# ====================
sales = []
order_id_counter = last_order_num + 1   # It starts after the last ID.
START_DATE = datetime(2026, 1, 1)
END_DATE = datetime(2026, 2, 28)

def random_date():
    delta = END_DATE - START_DATE
    return START_DATE + timedelta(days=random.randint(0, delta.days))

product_dict = {p["product_id"]: p for p in all_products}

for _ in range(NUM_ORDERS):
    order_id = f"O{order_id_counter:06}"   # Unique and continuous IDs
    customer_id = f"C{random.randint(1, 2000):05}"
    order_date = random_date().strftime("%Y-%m-%d")

    chosen_products = []

    # 20% of orders will have mixed combos.
    if random.random() < 0.2:
        combo = random.choice(mixed_combos)
        chosen_products.extend(combo)

    # add random products in addition to the combos
    items_in_order = random.randint(1, 4)
    chosen_products.extend(random.sample(product_dict.keys(), items_in_order))

    for product_id in set(chosen_products):
        if product_id in product_dict:
            product = product_dict[product_id]
            quantity = random.randint(1, 3)
            price = round(random.uniform(3.5, 25.0), 2)
            sales.append([
                order_id,
                product_id,
                product["category"],
                quantity,
                price,
                order_date,
                customer_id
            ])

    order_id_counter += 1

# ================
# SAVE NEW SALES
# ================
os.makedirs(os.path.dirname(sales_file_path), exist_ok=True)
with open(sales_file_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "product_id", "category", "quantity", "price", "order_date", "customer_id"])
    writer.writerows(sales)

print(f"New sales generated: {len(sales)}")