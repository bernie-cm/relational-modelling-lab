import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def create_normalised_schema():
    # Connect to your warehouse DB
    conn = psycopg2.connect(
        host='localhost',
        port=5433,
        database='warehouse',
        user='dataeng',
        password='dataeng123'
    )
    cur = conn.cursor()
    
    try:
        # Drop tables if exist (in correct order due to foreign keys)
        logger.info("Dropping existing tables...")
        cur.execute("DROP TABLE IF EXISTS order_items CASCADE")
        cur.execute("DROP TABLE IF EXISTS orders CASCADE")
        cur.execute("DROP TABLE IF EXISTS customers CASCADE")
        cur.execute("DROP TABLE IF EXISTS products CASCADE")
        
        # 1. Create Customers table
        cur.execute("""
            CREATE TABLE customers (
                customer_id VARCHAR(10) PRIMARY KEY,
                customer_name VARCHAR(100) NOT NULL,
                customer_email VARCHAR(100) UNIQUE NOT NULL,
                address VARCHAR(200),
                city VARCHAR(50),
                state CHAR(2),
                zip_code VARCHAR(10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logger.info("✓ Created customers table")
        
        # 2. Create Products table
        cur.execute("""
            CREATE TABLE products (
                product_id VARCHAR(10) PRIMARY KEY,
                product_name VARCHAR(100) NOT NULL,
                description TEXT,
                price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
                category VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logger.info("✓ Created products table")
        
        # 3. Create Orders table
        cur.execute("""
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                order_date TIMESTAMP NOT NULL,
                customer_id VARCHAR(10) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                    ON DELETE RESTRICT
                    ON UPDATE CASCADE
            );
        """)
        logger.info("✓ Created orders table")
        
        # 4. Create Order_Items table (junction table)
        cur.execute("""
            CREATE TABLE order_items (
                line_item_id SERIAL PRIMARY KEY,
                order_id INTEGER NOT NULL,
                product_id VARCHAR(10) NOT NULL,
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                line_total DECIMAL(10,2) NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(order_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES products(product_id)
                    ON DELETE RESTRICT
            );
        """)
        logger.info("✓ Created order_items table")
        
        # Create indexes for foreign keys (performance)
        cur.execute("CREATE INDEX idx_orders_customer ON orders(customer_id)")
        cur.execute("CREATE INDEX idx_order_items_order ON order_items(order_id)")
        cur.execute("CREATE INDEX idx_order_items_product ON order_items(product_id)")
        logger.info("✓ Created indexes")
        
        conn.commit()
        logger.info("\n=== Schema created successfully! ===")
        
        # Show the schema
        cur.execute("""
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name IN ('customers', 'products', 'orders', 'order_items')
            ORDER BY table_name, ordinal_position
        """)
        
        print("\nDatabase Schema:")
        for row in cur.fetchall():
            print(f"{row[0]}.{row[1]} - {row[2]} - Nullable: {row[3]}")
            
    except Exception as e:
        logger.error(f"Error: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    create_normalised_schema()