import psycopg2
import math
from psycopg2 import sql


# credentials
db_host = "localhost"
db_port = 5432
db_name = "postgres"
db_user = "postgres"
db_password = "postgres"

# Establish a connection to the PostgreSQL database
connection = psycopg2.connect(
    host=db_host,
    port=db_port,
    database=db_name,
    user=db_user,
    password=db_password
)

# Create a cursor to interact with the database
cursor = connection.cursor()

query = sql.SQL("""
    SELECT *
    FROM merchant_logs
    WHERE timestamp::date = CURRENT_DATE;
""")
cursor.execute(query)

# Fetch all todays trans
today_transactions = cursor.fetchall()
# print("today_transactions", today_transactions)

for row in today_transactions:
    merchantId,curr_day,transaction_amount,refund_amount,pa_customer,ntb_approved,vas_service_count,ntb_reject, dpd_count, npa_count,timestamp=row

    query = sql.SQL("""
        SELECT *
        FROM merchant_score_aggregator
        WHERE merchantId = %s;
    """)
    cursor.execute(query, (merchantId,))

    merchant_stats=cursor.fetchall()

    score, remaining_net_amt, remaining_count_dpd_npa_trans,remaining_pa_customer = 0,0,0,0
    # print("merchant_stats", merchant_stats)
    if merchant_stats:
        merchid, score, remaining_net_amt,remaining_count_dpd_npa_trans,remaining_pa_customer=merchant_stats[0]

#logic calc score and update values
    net_amount=transaction_amount+remaining_net_amt-refund_amount
    score+= math.floor(net_amount/10000)
    remaining_net_amt=net_amount%10000

    net_count_dpd_npa_transac=dpd_count+npa_count+remaining_count_dpd_npa_trans
    score -= math.floor(net_count_dpd_npa_transac/30)
    remaining_count_dpd_npa_trans=net_count_dpd_npa_transac%30

    score += 2*vas_service_count

    # 1% of what?
    # Assuming 1% of the day:
    net_pa_customer=remaining_pa_customer+pa_customer
    score+= 1.5*math.floor(net_pa_customer/ 100)
    remaining_pa_customer=net_pa_customer%100

    score+= ntb_approved-ntb_reject

    if not merchant_stats:
    # updating the values in the aggregote table
        insert_query = sql.SQL("""
            INSERT INTO merchant_score_aggregator (merchantId, score, remaining_net_amt, remaining_count_dpd_npa_trans,remaining_pa_customer)
            VALUES (%s, %s, %s, %s, %s);
        """)

        cursor.execute(insert_query, (merchantId, score, remaining_net_amt, remaining_count_dpd_npa_trans,remaining_pa_customer))
    else:
        update_query = sql.SQL("""
            UPDATE merchant_score_aggregator
            SET score = %s, remaining_net_amt = %s, remaining_count_dpd_npa_trans = %s, remaining_pa_customer = %s
            WHERE merchantId = %s;
        """)
        cursor.execute(update_query, (score, remaining_net_amt, remaining_count_dpd_npa_trans, remaining_pa_customer, merchantId))

    # Commit the changes to the database
    connection.commit()

print("Success!")

# Close the cursor and connection
cursor.close()
connection.close()
