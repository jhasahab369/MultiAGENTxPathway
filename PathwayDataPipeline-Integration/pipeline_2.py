import pathway as pw
from pathway.stdlib.ml.index import KNNIndex
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import requests
from dataclasses import dataclass

# Schema definitions for our data streams
class TrafficEventSchema(pw.Schema):
    timestamp: str
    event_type: str
    latitude: float
    longitude: float
    severity: int
    description: str

class TrafficFlowSchema(pw.Schema):
    timestamp: str
    segment_id: str
    speed: float
    congestion_level: int
    free_flow_speed: float

class RouteSchema(pw.Schema):
    route_id: str
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    estimated_time: int
    distance: float

# Helper functions for TomTom API interactions
def fetch_traffic_data(api_key: str, bbox: str) -> Dict:
    base_url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
    params = {"key": api_key, "bbox": bbox}
    response = requests.get(base_url, params=params)
    return response.json()

# Real-time traffic processing pipeline
class TrafficProcessor:
    def __init__(self, tomtom_api_key: str):
        self.api_key = tomtom_api_key
        
    def build_pipeline(self):
        # Input streams
        traffic_events = pw.io.csv.read(
            "./traffic_events/",
            schema=TrafficEventSchema,
            mode="streaming"
        )
        
        traffic_flow = pw.io.csv.read(
            "./traffic_flow/",
            schema=TrafficFlowSchema,
            mode="streaming"
        )

        # Process traffic events
        filtered_events = traffic_events.filter(
            traffic_events.severity >= 2  # Filter significant events
        )

        
        event_windows = filtered_events.windowby(
            pw.temporal.minutes(15),
            timestamp=pw.this.timestamp
        )

        
        area_events = event_windows.groupby(
            lambda x: (round(x.latitude, 2), round(x.longitude, 2))
        ).reduce(
            event_count=pw.reducers.count(),
            max_severity=pw.reducers.max(pw.this.severity),
            events=pw.reducers.concat_list(pw.this.description)
        )

        
        flow_windows = traffic_flow.windowby(
            pw.temporal.minutes(5),
            timestamp=pw.this.timestamp
        )

        congestion_analysis = flow_windows.groupby(
            pw.this.segment_id
        ).reduce(
            avg_speed=pw.reducers.avg(pw.this.speed),
            congestion_index=pw.reducers.avg(
                pw.this.speed / pw.this.free_flow_speed
            ),
            update_time=pw.reducers.max(pw.this.timestamp)
        )

       
        combined_analysis = congestion_analysis.join(
            area_events,
            how="left"
        ).select(
            segment_id=pw.this.segment_id,
            avg_speed=pw.this.avg_speed,
            congestion_index=pw.this.congestion_index,
            event_count=pw.coalesce(pw.this.event_count, 0),
            max_severity=pw.coalesce(pw.this.max_severity, 0),
            update_time=pw.this.update_time
        )

        
        alerts = combined_analysis.filter(
            (pw.this.congestion_index < 0.5) | 
            (pw.this.event_count > 2) |
            (pw.this.max_severity >= 4)
        ).select(
            alert_type=pw.case(
                (pw.this.congestion_index < 0.5, "SEVERE_CONGESTION"),
                (pw.this.event_count > 2, "MULTIPLE_INCIDENTS"),
                (pw.this.max_severity >= 4, "CRITICAL_EVENT")
            ),
            segment_id=pw.this.segment_id,
            details=pw.concat(
                "Congestion: ", pw.cast(str, pw.this.congestion_index),
                ", Events: ", pw.cast(str, pw.this.event_count)
            ),
            timestamp=pw.this.update_time
        )

        
        pw.io.csv.write(combined_analysis, "./output/analysis/")
        pw.io.csv.write(alerts, "./output/alerts/")
        
        
        pw.io.http.expose_on_http(
            combined_analysis,
            host="localhost",
            port=8000,
            endpoint="/traffic-analysis"
        )

        return combined_analysis, alerts

class SmartRoutingEngine:
    def __init__(self, traffic_analysis):
        self.traffic_analysis = traffic_analysis
        
    def build_routing_pipeline(self):
        # Input stream for route requests
        route_requests = pw.io.csv.read(
            "./route_requests/",
            schema=RouteSchema,
            mode="streaming"
        )

        # Join route segments with traffic analysis
        route_with_traffic = route_requests.join(
            self.traffic_analysis,
            how="left"
        )

        # Calculate optimal routes
        optimized_routes = route_with_traffic.select(
            route_id=pw.this.route_id,
            adjusted_time=pw.this.estimated_time * (
                1 + pw.coalesce(1 - pw.this.congestion_index, 0)
            ),
            risk_score=pw.case(
                (pw.this.max_severity >= 4, 1.0),
                (pw.this.max_severity >= 2, 0.5),
                default=0.0
            ),
            alternative_needed=(
                pw.this.congestion_index < 0.6 |
                (pw.this.event_count > 1)
            )
        )

        # Generate route recommendations
        recommendations = optimized_routes.select(
            route_id=pw.this.route_id,
            recommendation=pw.case(
                (pw.this.alternative_needed, "FIND_ALTERNATIVE"),
                (pw.this.risk_score > 0.5, "CAUTION_ADVISED"),
                default="ROUTE_OK"
            ),
            adjusted_eta=pw.this.adjusted_time,
            risk_level=pw.this.risk_score
        )

        pw.io.csv.write(recommendations, "./output/recommendations/")
        
        return recommendations

def main():
    # Initialize the pipeline
    processor = TrafficProcessor("your-tomtom-api-key")
    traffic_analysis, alerts = processor.build_pipeline()
    
    # Initialize routing engine
    routing_engine = SmartRoutingEngine(traffic_analysis)
    recommendations = routing_engine.build_routing_pipeline()
    
    # Run the pipeline
    pw.run()

if __name__ == "__main__":
    main()