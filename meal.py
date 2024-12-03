import boto3
import json
import mysql.connector
import time
from datetime import datetime, timezone, timedelta

def lambda_handler(event, context):
    client = boto3.client('iot-data', region_name='ap-northeast-2') 
    topic = 'esp32/sub'
    topic2 = 'App/sub'

    current_time = datetime.now(timezone(timedelta(hours=9))).strftime('%H:%M')

    db_host = ''
    db_user = ''
    db_password = ''
    db_name = ''

    connection = mysql.connector.connect(host=db_host, user=db_user, password=db_password, database=db_name)

    try:
        cursor = connection.cursor()
        sql = "SELECT medicine_date, medicine_time, medicine_when FROM medicine_schedule WHERE medicine_date = %s AND medicine_time = %s"
        cursor.execute(sql, (datetime.now().date(), current_time))
        schedule = cursor.fetchone()

        if schedule:
            medicine_date, medicine_time, medicine_when = schedule

            if medicine_when == "아침":
                message = {"message": "Breakfast"}
                client.publish(topic=topic, qos=1, payload=json.dumps(message))
                time.sleep(5)
                
            elif medicine_when == "점심":
                message = {"message": "Lunch"}
                client.publish(topic=topic, qos=1, payload=json.dumps(message))
                time.sleep(5)
                
            elif medicine_when == "저녁":
                message = {"message": "Dinner"}
                client.publish(topic=topic, qos=1, payload=json.dumps(message))
                time.sleep(5)
                
    except Exception as e:
        pass
            



    if isinstance(event, str):
        try:
            payload = json.loads(event)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            
    else:
        payload = event            

    if "Breakfast" in payload:
        breakfast_value = int(payload['Breakfast'])
        if breakfast_value > 10:
            msg = {"message": "아침약 복용했습니다."}
            client.publish(topic=topic2, qos=1, payload=json.dumps(msg))
            cursor.execute("UPDATE medicine_schedule SET medicine_status = 'true'")
            #cursor.execute("UPDATE medicine_schedule SET medicine_status = 'true' WHERE medicine_date = %s AND medicine_time = %s", (medicine_date, medicine_time))
            connection.commit()
        else:
            msg = {"message": "아침약 복용하지 않았습니다."}
            client.publish(topic=topic2, qos=1, payload=json.dumps(msg))
            cursor.execute("UPDATE medicine_schedule SET medicine_status = 'false'")
            #cursor.execute("UPDATE medicine_schedule SET medicine_status = 'false' WHERE medicine_date = %s AND medicine_time = %s", (medicine_date, medicine_time))
            connection.commit()
        
        
    elif "Lunch" in payload:
        lunch_value = int(payload['Lunch'])
        if lunch_value > 100:
            msg = {"message": "점심약 복용했습니다."}
            client.publish(topic=topic2, qos=1, payload=json.dumps(msg))
            cursor.execute("UPDATE medicine_schedule SET medicine_status = 'true'")
            #cursor.execute("UPDATE medicine_schedule SET medicine_status = 'true' WHERE medicine_date = %s AND medicine_time = %s", (medicine_date, medicine_time))
            connection.commit()
        else:
            msg = {"message": "점심약 복용하지 않았습니다."}
            client.publish(topic=topic2, qos=1, payload=json.dumps(msg))
            cursor.execute("UPDATE medicine_schedule SET medicine_status = 'false'")
            #cursor.execute("UPDATE medicine_schedule SET medicine_status = 'false' WHERE medicine_date = %s AND medicine_time = %s", (medicine_date, medicine_time))
            connection.commit()
        
        
    elif "Dinner" in payload:    
        dinner_value = int(payload['Dinner'])
        if dinner_value > 100:
            msg = {"message": "저녁약 복용했습니다."}
            client.publish(topic=topic2, qos=1, payload=json.dumps(msg))
            cursor.execute("UPDATE medicine_schedule SET medicine_status = 'true'")
            #cursor.execute("UPDATE medicine_schedule SET medicine_status = 'true' WHERE medicine_date = %s AND medicine_time = %s", (medicine_date, medicine_time))
            connection.commit()
        else:
            msg = {"message": "저녁약 복용하지 않았습니다."}
            client.publish(topic=topic2, qos=1, payload=json.dumps(msg))
            cursor.execute("UPDATE medicine_schedule SET medicine_status = 'false'")
            #cursor.execute("UPDATE medicine_schedule SET medicine_status = 'false' WHERE medicine_date = %s AND medicine_time = %s", (medicine_date, medicine_time))
            connection.commit()

    cursor.close()
    connection.close()