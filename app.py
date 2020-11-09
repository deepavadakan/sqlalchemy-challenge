import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import datetime as dt
import numpy as np

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

print(Base.classes.keys())

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of dates and precipitaion for the latest year of data"""

    # Find the last date recorded
    last_date_row = session.query(Measurement.date).\
        order_by(Measurement.date.desc()).first()
    last_date = dt.datetime.strptime(last_date_row.date, '%Y-%m-%d')
    
    # Calculate the date 1 year ago from the last data point in the database
    yearago_date = last_date - dt.timedelta(days=365)
    
    # Perform a query to retrieve the date and precipitation scores
    results = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= yearago_date).all()
    
    session.close()

    # Create a dictionary from the row data and append to a list of prcp_12months
    prcp_12months = []
    for date, prcp in results:
        prcp_dict = {}
        prcp_dict["date"] = date
        prcp_dict["prcp"] = prcp
        prcp_12months.append(prcp_dict)

    return jsonify(prcp_12months)

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all stations"""
    # Query all passengers
    results = session.query(Station.name).all()

    session.close()

    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of dates and temperature observations of the most active station for the latest year of data"""
    # List the stations and the counts in descending order.
    active_station = session.query(Station.name, Station.id, func.count(Measurement.station)).\
        filter(Measurement.station == Station.station).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).all()

    # Find most active station
    (station_name, station_id, station_count) = active_station[0]

    # Find the last date recorded
    station_last_date_row = session.query(Measurement.date).\
        filter(Measurement.station == station_id).\
        order_by(Measurement.date.desc()).first()
    station_last_date = dt.datetime.strptime(last_date_row.date, '%Y-%m-%d')

    # Calculate the date 1 year ago from the last data point in the database
    station_yearago_date = station_last_date - dt.timedelta(days=365)

    # Perform a query to retrieve date and temperature 
    results = session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.date > station_yearago_date).all()

    session.close()

    # Create a dictionary from the row data and append to a list of active_station_tobs
    active_station_tobs = []
    for date, tobs in results:
        active_station_tobs_dict = {}
        active_station_tobs_dict["date"] = date
        active_station_tobs_dict["tobs"] = tobs
        active_station_tobs.append(active_station_tobs_dict)

    return jsonify(active_station_tobs)

@app.route("/api/v1.0/<start>")
def temp_stats_from_date(start):

    # Check if date is in the correct format
    try:
        dt.datetime.strptime(start, "%Y-%m-%d")
        dt.datetime.strptime(end, "%Y-%m-%d")
        print("This is the correct date string format.")
    except ValueError:
        print("This is the incorrect date string format. It should be YYYY-MM-DD")
        return jsonify("This is the incorrect date string format. It should be YYYY-MM-DD"), 404

    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of stations and their max, min and average temperature from the given date"""

    # Perform a query to retrieve station name, max, min and avg temperature from given date 
    results2 = session.query(Station.name, 
                              func.max(Measurement.tobs), 
                              func.min(Measurement.tobs), 
                              func.avg(Measurement.tobs)).\
        filter(Measurement.station == Station.station).\
        filter(Measurement.date >= start).\
        group_by(Measurement.station).all()

    results = session.query(func.max(Measurement.tobs), 
                            func.min(Measurement.tobs), 
                            func.avg(Measurement.tobs)).\
        filter(Measurement.date >= start).first()

    session.close()
    
    # Create a dictionary from the row data and append to a list of station_stats
    (max_temp, min_temp, avg_temp) = results
    station_stats = []
    station_stats_dict = {}
    station_stats_dict["max_temp"] = max_temp
    station_stats_dict["min_temp"] = min_temp
    station_stats_dict["avg_temp"] = round(avg_temp, 2)
    station_stats.append(station_stats_dict)

    station_stats2 = []
    for name, max_temp, min_temp, avg_temp in results2:
        station_stats_dict2 = {}
        station_stats_dict2["max_temp"] = max_temp
        station_stats_dict2["min_temp"] = min_temp
        station_stats_dic2t["avg_temp"] = round(avg_temp, 2)
        station_stats.append(station_stats_dict2)

    return jsonify(station_stats)

@app.route("/api/v1.0/<start>/<end>")
def temp_stats_date_range(start, end):

    # Check if date is in the correct format
    try:
        dt.datetime.strptime(start, "%Y-%m-%d")
        dt.datetime.strptime(end, "%Y-%m-%d")
        print("This is the correct date string format.")
    except ValueError:
        print("This is the incorrect date string format. It should be YYYY-MM-DD")
        return jsonify("This is the incorrect date string format. It should be YYYY-MM-DD"), 404

    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of stations and their max, min and average temperature for the given date range"""

    # Perform a query to retrieve station name, max, min and avg temperature ffor the given date range 
    results2 = session.query(Station.name, 
                              func.max(Measurement.tobs), 
                              func.min(Measurement.tobs), 
                              func.avg(Measurement.tobs)).\
        filter(Measurement.station == Station.station).\
        filter(Measurement.date >= start).\
        filter(Measurement.date <= end).\
        group_by(Measurement.station).all()

    results = session.query(func.max(Measurement.tobs), 
                            func.min(Measurement.tobs), 
                            func.avg(Measurement.tobs)).\
        filter(Measurement.date >= "2012-01-11").\
        filter(Measurement.date <= "2012-12-11").first()

    session.close()

    # Create a dictionary from the row data and append to a list of station_stats
    (max_temp, min_temp, avg_temp) = results
    station_stats = []
    station_stats_dict = {}
    station_stats_dict["max_temp"] = max_temp
    station_stats_dict["min_temp"] = min_temp
    station_stats_dict["avg_temp"] = round(avg_temp, 2)
    station_stats.append(station_stats_dict)
    
    station_stats2 = []
    for name, max_temp, min_temp, avg_temp in results2:
        station_stats_dict2 = {}
        station_stats_dict2["max_temp"] = max_temp
        station_stats_dict2["min_temp"] = min_temp
        station_stats_dict2["avg_temp"] = round(avg_temp, 2)
        station_stats2.append(station_stats_dict2)

    return jsonify(station_stats)

if __name__ == '__main__':
    app.run(debug=True)
