from jobs.bronze.products_bronze import run as bronze_products
from jobs.bronze.sales_bronze import run as bronze_sales
from jobs.silver.products_silver import run as silver_products
from jobs.silver.sales_silver import run as silver_sales
from jobs.gold.cross_sell_rules import run as gold_cross_sell
from jobs.gold.lower_sales_products import run as gold_lower_sales
from jobs.gold.promotion_candidates import run as gold_promo

def main():
    bronze_products()
    bronze_sales()

    silver_products()
    silver_sales()

    gold_cross_sell()
    gold_lower_sales()
    gold_promo()

if __name__ == "__main__":
    main()