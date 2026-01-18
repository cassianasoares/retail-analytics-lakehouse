CSV_PRODUCTS_FILE_PATH = "products/ingestion_date"
CSV_SALES_FILE_PATH = "sales/ingestion_date"

FILE_BASE_PATH = "data"
FILE_PRODUCTS = f"{FILE_BASE_PATH}/products"
FILE_SALES = f"{FILE_BASE_PATH}/sales"

DATA_LAKE = "datalake"

BRONZE_PRODUCTS = f"{DATA_LAKE}/bronze/products"
BRONZE_SALES = f"{DATA_LAKE}/bronze/sales"

SILVER_PRODUCTS = f"{DATA_LAKE}/silver/products"
SILVER_SALES = f"{DATA_LAKE}/silver/sales"

GOLD_CROSS_SELL = f"{DATA_LAKE}/gold/cross_sell_rules"
GOLD_LOW_SALES = f"{DATA_LAKE}/gold/low_sales_products"
GOLD_PROMOTION_CANDIDATES = f"{DATA_LAKE}/gold/promotion_candidates"


HEADER_READ_FILE = "header"
INFER_SCHEMA_READ_FILE = "inferSchema"
TRUE = "true"


MODE_APPEND = "append"
MODE_OVERWRITE = "overwrite"

FORMAT_DELTA = "delta"
INGESTION_DATE = "ingestion_date"

ORDER_DATE = "order_date"
YEAR = "year"
MONTH = "month"
DAY = "day"

PRODUCT_ID = "product_id"
PRODUCT_Y_ID = "product_y_id"
QUANTITY = "quantity"
TOTAL_QUANTITY = "quantity"

VALUE_TO_DEFINE_LOW_SALES = 200
