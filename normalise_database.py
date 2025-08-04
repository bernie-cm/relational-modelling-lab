import pandas as pd

# Load denormalized data
df = pd.read_csv('denormalized_orders.csv')

print("=== NORMALIZATION PROCESS ===")

# Step 1: Identify Entities
print("\n1. ENTITIES IDENTIFIED:")
print("- Customers (customer info)")
print("- Products (product catalog)")
print("- Orders (order headers)")
print("- Order_Items (order details/line items)")

# Step 2: Create normalized tables
# Customers table (no duplicates)
customers_df = df[['customer_id', 'customer_name', 'customer_email', 
                   'customer_address', 'customer_city', 'customer_state', 
                   'customer_zip']].drop_duplicates()
print(f"\nCustomers table: {len(customers_df)} unique customers")

# Products table
products_df = df[['product_id', 'product_name', 'product_description', 
                  'product_price', 'product_category']].drop_duplicates()
print(f"Products table: {len(products_df)} unique products")

# Orders table (order headers)
orders_df = df[['order_id', 'order_date', 'customer_id']].drop_duplicates()
print(f"Orders table: {len(orders_df)} orders")

# Order_Items table (order details)
order_items_df = df[['order_id', 'product_id', 'quantity', 'total_amount']]
order_items_df['line_item_id'] = range(1, len(order_items_df) + 1)
print(f"Order_Items table: {len(order_items_df)} line items")

# Step 3: Show the relationships
print("\n=== RELATIONSHIPS ===")
print("customers.customer_id (PK) -> orders.customer_id (FK)")
print("orders.order_id (PK) -> order_items.order_id (FK)")
print("products.product_id (PK) -> order_items.product_id (FK)")

# Save normalized tables
customers_df.to_csv('normalized_customers.csv', index=False)
products_df.to_csv('normalized_products.csv', index=False)
orders_df.to_csv('normalized_orders.csv', index=False)
order_items_df.to_csv('normalized_order_items.csv', index=False)

print("\n✓ Normalization complete! Files saved.")