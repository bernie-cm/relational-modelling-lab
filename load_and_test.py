import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def load_normalized_data():
    conn = psycopg2.connect(
        host='localhost',
        port=5433,
        database='warehouse',
        user='dataeng',
        password='dataeng123'
    )
    cur = conn.cursor()
    
    try:
        # Load CSVs
        customers = pd.read_csv('normalized_customers.csv')
        products = pd.read_csv('normalized_products.csv')
        orders = pd.read_csv('normalized_orders.csv')
        order_items = pd.read_csv('normalized_order_items.csv')
        
        # Convert timestamps
        orders['order_date'] = pd.to_datetime(orders['order_date'])
        
        # Load customers
        customer_records = [tuple(x) for x in customers.to_numpy()]
        execute_values(cur, """
            INSERT INTO customers (customer_id, customer_name, customer_email, 
                                 address, city, state, zip_code)
            VALUES %s
        """, customer_records)
        logger.info(f"✓ Loaded {len(customers)} customers")
        
        # Load products
        product_records = [tuple(x) for x in products.to_numpy()]
        execute_values(cur, """
            INSERT INTO products (product_id, product_name, description, price, category)
            VALUES %s
        """, product_records)
        logger.info(f"✓ Loaded {len(products)} products")
        
        # Load orders
        order_records = [(row.order_id, row.order_date.to_pydatetime(), row.customer_id) 
                        for row in orders.itertuples()]
        execute_values(cur, """
            INSERT INTO orders (order_id, order_date, customer_id)
            VALUES %s
        """, order_records)
        logger.info(f"✓ Loaded {len(orders)} orders")
        
        # Load order items
        order_item_records = [tuple(x) for x in order_items[['order_id', 'product_id', 
                                                            'quantity', 'total_amount']].to_numpy()]
        execute_values(cur, """
            INSERT INTO order_items (order_id, product_id, quantity, line_total)
            VALUES %s
        """, order_item_records)
        logger.info(f"✓ Loaded {len(order_items)} order items")
        
        conn.commit()
        
        # Test foreign key constraint
        logger.info("\n=== Testing Foreign Key Constraints ===")
        try:
            cur.execute("INSERT INTO orders (order_id, order_date, customer_id) VALUES (9999, NOW(), 'INVALID')")
            conn.commit()
        except psycopg2.IntegrityError as e:
            logger.info("✓ Foreign key working: Cannot add order for non-existent customer")
            conn.rollback()
            
    except Exception as e:
        logger.error(f"Error: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def test_queries():
    conn = psycopg2.connect(
        host='localhost',
        port=5433,
        database='warehouse',
        user='dataeng',
        password='dataeng123'
    )
    cur = conn.cursor()
    
    print("\n=== Testing JOIN Queries ===")
    
    # Reconstruct original view
    cur.execute("""
        SELECT 
            o.order_id,
            o.order_date,
            c.customer_name,
            p.product_name,
            oi.quantity,
            oi.line_total
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products p ON oi.product_id = p.product_id
        ORDER BY o.order_id
        LIMIT 5
    """)
    
    print("\nReconstructed denormalized view:")
    for row in cur.fetchall():
        print(row)
    
    # Customer order summary
    cur.execute("""
        SELECT 
            c.customer_name,
            COUNT(DISTINCT o.order_id) as total_orders,
            SUM(oi.line_total) as total_spent
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY c.customer_name
        ORDER BY total_spent DESC
    """)
    
    print("\nCustomer Summary:")
    for row in cur.fetchall():
        print(f"{row[0]}: {row[1]} orders, ${row[2]:.2f} total")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    load_normalized_data()
    test_queries()