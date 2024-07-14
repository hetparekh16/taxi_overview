from flask import Flask, jsonify, render_template
import redis

app = Flask(__name__)

# Initialize Redis connection
r = redis.Redis(host='redis', port=6379)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/taxi-locations')
def get_taxi_locations():
    area_violations = r.smembers('taxi:area_violations_ids')
    area_violations = {v.decode('utf-8') for v in area_violations}

    keys = r.keys('taxi:location:*')
    locations = []
    for key in keys:
        taxi_id = key.decode('utf-8').split(':')[-1]
        if taxi_id not in area_violations:
            location_data = r.hgetall(key)
            location = {
                'taxi_id': taxi_id,
                'latitude': location_data[b'latitude'].decode('utf-8'),
                'longitude': location_data[b'longitude'].decode('utf-8'),
                'timestamp': location_data[b'timestamp'].decode('utf-8')
            }
            locations.append(location)
    return jsonify(locations)

@app.route('/dashboard-data')
def get_dashboard_data():
    currently_driving = len(r.smembers('taxi:currently_driving'))
    total_distance = sum(float(distance.decode('utf-8')) for distance in r.hvals('taxi:distance'))
    area_violations = r.smembers('taxi:area_violations_ids')
    speeding_incidents_ids = r.smembers('taxi:speeding_ids')  # Fetch speeding incident IDs
    leaving_area_warnings = r.smembers('taxi:leaving_area_warning_ids')  # Fetch leaving area warning IDs

    dashboard_data = {
        'currently_driving': currently_driving,
        'total_distance': total_distance,
        'area_violations': [v.decode('utf-8') for v in area_violations],
        'speeding_incidents': [v.decode('utf-8') for v in speeding_incidents_ids],
        'leaving_area_warnings': [v.decode('utf-8') for v in leaving_area_warnings]  # Include warning IDs
    }
    return jsonify(dashboard_data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
