import time
import random
import json
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

def generate_business_streams():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    orders_topic = 'stream_orders'
    payments_topic = 'stream_payments'
    order_id = 1001
    pending_payments = []
    
    messy_genders = ['m', 'M', 'male', '1', 'f', 'F', 'female', '0', 'UNKNOWN']
    
    # All counts
    stats = {
        "total_orders": 0,
        "total_payments": 0,
        "total_hackers": 0,
	"total_late": 0
    }
    
    print("Starting REAL-WORLD BUSINESS streams...")
    print("Hackers are hidden in the stream! Check the final summary when you press Ctrl+C to verify your PySpark results.")
    
    try:
        while True:
            now = datetime.now(timezone.utc)
            
            # ------------------------------------------------
            # STEP 1: GENERATE ORDER
            # ------------------------------------------------
            quantity = random.randint(1, 5)
            price = round(random.uniform(50.00, 500.00), 2)
            discount = round(random.uniform(0.00, 20.00), 2)
            
            order_event = {
                "order_id": order_id,
                "customer_id": random.randint(100, 500),
                "customer_gender": random.choice(messy_genders),
                "product_id": random.randint(1, 50),
                "quantity": quantity,
                "price": price,
                "discount": discount,
                "order_time": now.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            producer.send(orders_topic, order_event)
            stats["total_orders"] += 1
            print(f"[ORDER] Sent Order {order_id}")
            
            # ------------------------------------------------
            # STEP 2: SIMULATE PAYMENT DELAY & HACKERS
            # ------------------------------------------------
            if random.random() < 0.90: # 90% chance to pay
                delay_seconds = random.randint(2, 6)
		if delay_seconds > 5:
 		stats["total_late"] += 1

                actual_required_amount = round((quantity * price) - discount, 2)
                
                is_hacker = False
                # 10% chance to be a hacker and pay less
                if random.random() < 0.10:
                    paid_amount = round(actual_required_amount * 0.10, 2)
                    is_hacker = True
                else:
                    paid_amount = actual_required_amount
                
                
                payment_event = {
                    "payment_id": str(uuid.uuid4())[:8],
                    "order_id": order_id,
                    "payment_method": random.choice(["Credit_Card", "PayPal", "Wallet"]),
                    "paid_amount": paid_amount,
                    "payment_time": ""
                }
                
                time_to_send = time.time() + delay_seconds
                pending_payments.append((time_to_send, payment_event, is_hacker))
            
            # ------------------------------------------------
            # STEP 3: SEND DUE PAYMENTS
            # ------------------------------------------------
            current_time = time.time()
            for p in pending_payments[:]: 
                send_time, payment_data, is_hacker = p
                
                if current_time >= send_time:
                    payment_data["payment_time"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    producer.send(payments_topic, payment_data)
                    
                    stats["total_payments"] += 1
                    if is_hacker:
                        stats["total_hackers"] += 1
                        
                    print(f"   ---> [PAYMENT] Received payment for Order {payment_data['order_id']}")
                    pending_payments.remove(p)
            
            order_id += 1
            time.sleep(random.uniform(1.0, 2.0))
            
    except KeyboardInterrupt:
        # The final report to check taht my work
        print("\n" + "="*40)
        print("🛑 STREAM STOPPED. FINAL SUMMARY:")
        print("="*40)
        print(f"📦 Total Orders Sent: {stats['total_orders']}")
        print(f"💳 Total Payments Sent: {stats['total_payments']}")
        print(f"😈 Total Hackers Generated: {stats['total_hackers']}")
        print(f"  Total Late Payments (> 5s): {stats['total_late']}")
	print("="*40)
        print("Use these numbers to verify if your PySpark code caught everyone!")
        producer.close()

if __name__ == "__main__":
    generate_business_streams()
