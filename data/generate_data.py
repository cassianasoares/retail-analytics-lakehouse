import random
import os
import csv
from datetime import datetime, timedelta

# ========================
# CRIAÇÃO DE PASTAS
# ========================
products_file_path = "products/ingestion_date=2026-01-01/products.csv"
sales_file_path = "sales/ingestion_date=2026-01-01/sales.csv"

os.makedirs(os.path.dirname(products_file_path), exist_ok=True)
os.makedirs(os.path.dirname(sales_file_path), exist_ok=True)

# ========================
# CONFIGURAÇÕES
# ========================
NUM_PRODUCTS = 120
NUM_ORDERS = 5000
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 6, 30)

LOW_SALES_RATIO = 0.1
random.seed(42)

# ========================
# PRODUTOS
# ========================
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

products = []
for i in range(1, NUM_PRODUCTS + 1):
    category = random.choice(list(categories.keys()))
    usage_type = random.choice(categories[category])
    product_id = f"P{i:03}"
    product_name = f"Produto {product_id}"
    products.append([product_id, product_name, category, usage_type])

low_sales_products = set(
    random.sample([p[0] for p in products], int(NUM_PRODUCTS * LOW_SALES_RATIO))
)

# ========================
# COMBOS REPETIDOS
# ========================
# Definimos alguns combos que devem aparecer em vários pedidos
combos = [
    ["P001", "P002"],                       # café + pão
    ["P010", "P020", "P021"],               # cerveja + carne + carvão
    ["P030", "P040"],                       # ração cachorro + brinquedo pet
    ["P050", "P060", "P061"],               # fralda + lenço + pomada
    ["P070", "P080"],                       # shampoo + condicionador
    ["P090", "P100", "P101", "P102"],       # arroz + feijão + óleo + sal
    ["P110", "P111"],                       # refrigerante + salgadinho
    ["P112", "P113", "P114"],               # tomate + alface + cebola
    ["P115", "P116"], 
]

# ========================
# VENDAS
# ========================
sales = []
order_id_counter = 1

def random_date():
    delta = END_DATE - START_DATE
    return START_DATE + timedelta(days=random.randint(0, delta.days))

for _ in range(NUM_ORDERS):
    order_id = f"O{order_id_counter:06}"
    customer_id = f"C{random.randint(1, 2000):05}"
    order_date = random_date().strftime("%Y-%m-%d")

    chosen_products = []

    # 20% dos pedidos terão combos repetidos
    if random.random() < 0.35:
        combo = random.choice(combos)
        chosen_products.extend(combo)

    # adiciona produtos aleatórios além dos combos
    items_in_order = random.randint(1, 4)
    for _ in range(items_in_order):
        if random.random() < 0.15:
            product = random.choice(list(low_sales_products))
        else:
            product = random.choice(products)[0]
        chosen_products.append(product)

    for product_id in set(chosen_products):
        product = next(p for p in products if p[0] == product_id)
        quantity = random.randint(1, 3)
        price = round(random.uniform(3.5, 25.0), 2)
        sales.append([
            order_id,
            product_id,
            product[2],
            quantity,
            price,
            order_date,
            customer_id
        ])

    order_id_counter += 1

# ========================
# SALVAR CSVs
# ========================
with open(products_file_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["product_id", "product_name", "category", "usage_type"])
    writer.writerows(products)

with open(sales_file_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "order_id", "product_id", "category",
        "quantity", "price", "order_date", "customer_id"
    ])
    writer.writerows(sales)

print("Arquivos gerados com sucesso!")
print(f"Produtos: {len(products)}")
print(f"Vendas (linhas): {len(sales)}")
print(f"Produtos com baixa venda: {len(low_sales_products)}")