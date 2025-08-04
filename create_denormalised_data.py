import pandas as pd
import random
from datetime import datetime, timedelta

# Generate intentionally denormalized e-commerce data
def create_denormalized_orders():
    # Sample data with redundancy
    customers = [
        ("CUST001", "Alice Johnson", "alice@email.com", "123 Main St", "New York", "NY", "10001"),
        ("CUST002", "Bob Smith", "bob@email.com", "456 Oak Ave", "Los Angeles", "CA", "90001"),
        ("CUST003", "Carol White", "carol@email.com", "789 Pine Rd", "Chicago", "IL", "60601"),
    ]
    
    products = [
        ("PROD001", "Laptop", "High-performance laptop", 999.99, "Electronics"),
        ("PROD002", "Mouse", "Wireless mouse", 29.99, "Electronics"),
        ("PROD003", "Notebook", "Spiral notebook", 4.99, "Stationery"),
        ("PROD004", "Pen", "Ballpoint pen pack", 9.99, "Stationery"),
    ]
    
    # Create denormalized orders (everything in one table - bad design!)
    orders = []
    order_id = 1000
    
    for _ in range(20):  # Generate 20 orders
        customer = random.choice(customers)
        product = random.choice(products)
        quantity = random.randint(1, 5)
        order_date = datetime.now() - timedelta(days=random.randint(1, 30))
        
        order = {
            'order_id': order_id,
            'order_date': order_date,
            'customer_id': customer[0],
            'customer_name': customer[1],
            'customer_email': customer[2],
            'customer_address': customer[3],
            'customer_city': customer[4],
            'customer_state': customer[5],
            'customer_zip': customer[6],
            'product_id': product[0],
            'product_name': product[1],
            'product_description': product[2],
            'product_price': product[3],
            'product_category': product[4],
            'quantity': quantity,
            'total_amount': quantity * product[3]
        }
        orders.append(order)
        order_id += 1
    
    df = pd.DataFrame(orders)
    df.to_csv('denormalized_orders.csv', index=False)
    print(f"Created {len(df)} denormalized orders")
    return df

# Analyze the problems
def analyze_denormalization_issues(df):
    print("\n=== DENORMALIZATION ISSUES ===")
    
    # 1. Data Redundancy
    print("\n1. DATA REDUNDANCY:")
    print(f"Unique customers: {df['customer_id'].nunique()}")
    print(f"Customer records: {len(df)}")
    print(f"→ Customer data repeated {len(df)/df['customer_id'].nunique():.1f}x\n")
    
    # 2. Update Anomalies
    print("2. UPDATE ANOMALIES:")
    alice_records = df[df['customer_name'] == 'Alice Johnson']
    print(f"If Alice changes email, must update {len(alice_records)} records")
    
    # 3. Storage Waste
    print("\n3. STORAGE WASTE:")
    repeated_chars = df['customer_address'].str.len().sum()
    unique_chars = df.drop_duplicates('customer_id')['customer_address'].str.len().sum()
    print(f"Address storage: {repeated_chars} chars vs {unique_chars} chars needed")
    print(f"Wasted: {repeated_chars - unique_chars} characters")
    
    return df

# Run analysis
df = create_denormalized_orders()
analyze_denormalization_issues(df)
print("\nSample of denormalized data:")
print(df.head())