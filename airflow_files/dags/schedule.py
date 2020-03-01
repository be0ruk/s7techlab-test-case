import json
import requests

import airflow
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='upload_flights_schedule',
    default_args=args,
    schedule_interval='@daily',
    description='Upload flights schedule data'
)


# Test for email alerts
def throw_error(**context):
    raise ValueError('Intentionally throwing an error to send an email.')


def flight_upload(departureCode, arrivalCode, carrierCode, ds, **kwargs):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    date = datetime.strptime(ds, '%Y-%m-%d')
    app_key = Variable.get('app_key')
    app_id = Variable.get('app_id')
    url_schedule = Variable.get('url_schedule')
    url_flight_features = Variable.get('url_flight_features')
    # Link for history data about flights
    url_flight_history = Variable.get('url_flight_history')

    airports_insert = "INSERT INTO flightstats.airports(iata, name, citycode, city, countrycode, countryname) " \
                      "VALUES(%(iata)s, %(name)s, %(cityCode)s, %(city)s, %(countryCode)s, %(countryName)s)" \
                      "ON CONFLICT (iata) DO UPDATE SET (name, citycode, city, countrycode, countryname) = (%(name)s, " \
                      "%(cityCode)s, %(city)s, %(countryCode)s, %(countryName)s) "
    flight_insert = "INSERT INTO flightstats.flights(upload_at, carrierfscode, flightnumber, departureairportfscode, " \
                    "arrivalairportfscode, departuretime, arrivaltime, flightequipmentiatacode) " \
                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
    flight_features_insert = "INSERT INTO flightstats.aircrafts(airlinefscode, flightnumber, iata, name, economy, " \
                             "premium_economy, business, first) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"

    pg_hook = PostgresHook(postgre_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    for i in range(1):
        date += timedelta(days=1)
        url = url_flight_history if (datetime.now() - date).days > 0 else url_schedule
        url = url % (departureCode, arrivalCode, date.strftime('%Y/%m/%d'), app_id, app_key)
        r = requests.get(url).json()
        schedule = r["flightStatuses"] if (datetime.now() - date).days > 0 else r["scheduledFlights"]

        for s in schedule:
            if s["carrierFsCode"] == carrierCode:
                cur.execute(flight_insert, (now,
                                            s["carrierFsCode"],
                                            s["flightNumber"],
                                            s["departureAirportFsCode"],
                                            s["arrivalAirportFsCode"],
                                            s["departureDate"]["dateLocal"] if (datetime.now() - date).days > 0
                                                else s["departureTime"],
                                            s["arrivalDate"]["dateLocal"] if (datetime.now() - date).days > 0
                                                else s["arrivalTime"],
                                            s["flightEquipment"]["scheduledEquipmentIataCode"] if (datetime.now() - date).days > 0
                                                else s["flightEquipmentIataCode"]))

                url = url_flight_features % (
                    s["carrierFsCode"], s["flightNumber"], date.strftime("%Y/%m/%d"), app_id, app_key)
                f = requests.get(url).json()
                i = 0
                for fl in f["flights"]:
                    cur.execute(flight_features_insert, (fl["airlineFsCode"],
                                                         fl["flightNumber"],
                                                         f["appendix"]["equipments"][i]["iata"],
                                                         f["appendix"]["equipments"][i]["name"],
                                                         fl["economy"]["seatCount"],
                                                         fl["premiumEconomy"]["seatCount"],
                                                         fl["business"]["seatCount"],
                                                         fl["first"]["seatCount"]
                                                         ))
                    i += 1

    airports = r["appendix"]["airports"]
    cur.executemany(airports_insert, airports)
    conn.commit()


directions = json.loads(Variable.get('flight_directions'))

# Create taks for each direction
for d in directions:
    task = PythonOperator(
        task_id='flight_upload_' + d['departureCode'] + '_to_' + d['arrivalCode'] + '_by_' + d['carrierCode'],
        python_callable=flight_upload,
        provide_context=True,
        email_on_failure=True,
        email='bezrukaviy@gmail.com',
        op_kwargs=d,
        dag=dag)
