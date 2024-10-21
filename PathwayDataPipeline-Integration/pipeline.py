
from pathway import Pipeline, Input, Output
from math import exp
import os
from crewai import Agent, Task, Crew, Process
from langchain_groq import ChatGroq
from langchain.tools import DuckDuckGoSearchRun
from langchain.tools import StructuredTool
from pydantic import BaseModel
import requests
from datetime import datetime, timedelta
import json
from dataclasses import dataclass
from typing import List, Dict, Optional,Callable
from dotenv import load_dotenv
import requests
from langchain.tools import StructuredTool
from pydantic import BaseModel
import os
from dotenv import load_dotenv

class SearchInput(BaseModel):
    query: str 
search_tool = StructuredTool(
    name="Internet Search",
    func=DuckDuckGoSearchRun().run,  
    description="Search the internet for up-to-date information on traffic condition , real time incidents realted to the place in context asked by user",
    args_schema=SearchInput  
)
class Tool(BaseModel):
    name: str
    function: Callable
    description: str

    def run(self, *args, **kwargs):
        return self.function(*args, **kwargs)

#env loadings...

load_dotenv()


TOMTOM_API_KEY = os.environ["TOMTOM_API_KEY"]
groq_api_key = os.environ["GROQ_API_KEY"]

#we'll use groqq
#todo - try with openai also to see which gives better results

llm = ChatGroq(
    api_key=groq_api_key,
    model_name="groq/llama3-8b-8192",
    temperature=0.7,
    max_tokens=1024
)

# 
# class SearchInput(BaseModel):
#     query: str

# search_tool = StructuredTool(
#     name="Internet Search",
#     func=DuckDuckGoSearchRun().run,
#     description="Search the internet for up-to-date information on traffic conditions, route options, and travel advisories.",
#     args_schema=SearchInput
# )

@dataclass
class Location:
    lat: float
    lon: float
    name: str
    
    def to_dict(self):
        return {
            "lat": self.lat,
            "lon": self.lon,
            "name": self.name
        }

@dataclass
class TrafficIncident:
    type: str
    location: Location
    description: str
    severity: int
    delay: int
    
    def to_dict(self):
        return {
            "type": self.type,
            "location": self.location.to_dict(),
            "description": self.description,
            "severity": self.severity,
            "delay": self.delay
        }

class TomTomAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.tomtom.com"

    def get_traffic_flow(self, lat: float, lon: float, radius: int = 1000) -> Optional[Dict]:
        """
        Fetches real-time traffic data from TomTom Traffic Flow API.
        
        :param lat: Latitude of the center point
        :param lon: Longitude of the center point
        :param radius: Radius (in meters) to search around the center point
        :return: Dictionary containing traffic speed and congestion level, or None if the request fails
        """
        endpoint = f"{self.base_url}/traffic/services/4/flowSegmentData/absolute/10/json"
        params = {
            'key': self.api_key,
            'point': f"{lat},{lon}",
            'radius': radius
        }
        
        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            flow_segment = data.get('flowSegmentData', {})
            
            if flow_segment:
                return {
                    "current_speed": flow_segment['currentSpeed'],
                    "free_flow_speed": flow_segment['freeFlowSpeed'],
                    "congestion_level": flow_segment['confidence']  
                }
            return None
        except requests.RequestException as e:
            print(f"Error fetching traffic flow data: {str(e)}")
            return None

    def get_incidents(self, bbox: str) -> List[Dict]:
        endpoint = f"{self.base_url}/traffic/services/5/incidentDetails"
        params = {
            "key": self.api_key,
            "bbox": bbox,
            "fields": "{incidents{type,geometry,description,severity,delay}}"
        }
        response = self._make_request(endpoint, params)
        return response.get('incidents', [])

    def calculate_route(self, start: Location, end: Location, 
                        alternatives: bool = True) -> Dict:
        endpoint = f"{self.base_url}/routing/1/calculateRoute/{start.lat},{start.lon}:{end.lat},{end.lon}/json"
        params = {
            "key": self.api_key,
            "traffic": "true",
            "alternatives": str(alternatives).lower(),
            "maxAlternatives": 3,
            "reportGeometry": "true"
        }
        return self._make_request(endpoint, params)

    def _make_request(self, endpoint: str, params: Dict) -> Dict:
        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error making request to {endpoint}: {str(e)}")
            return {}

def get_bbox(start: Location, end: Location) -> str:
    
    min_lon = min(start.lon, end.lon)
    min_lat = min(start.lat, end.lat)
    max_lon = max(start.lon, end.lon)
    max_lat = max(start.lat, end.lat)
    
    return f"{min_lon},{min_lat},{max_lon},{max_lat}"

##CHANGED THIS INTEGRATION WITH PATHWAY

class TrafficDataManager:
       def __init__(self, tomtom_api: TomTomAPI):
           self.api = tomtom_api
           self.pipeline = self.create_traffic_data_pipeline()
           self.cache_duration = timedelta(minutes=5)
           self.cache_timestamp = None
           self.cached_data = {}
   
       def create_traffic_data_pipeline(self):
           # Define the data sources
           start_location = Input("start_location")
           end_location = Input("end_location")
   
           # Define the data transformations
           def get_traffic_data(start: Location, end: Location):
               bbox = get_bbox(start, end)
               start_traffic = self.api.get_traffic_flow(start.lat, start.lon)
               end_traffic = self.api.get_traffic_flow(end.lat, end.lon)
               incidents = self.api.get_incidents(bbox)
               routes = self.api.calculate_route(start, end)
   
               return {
                   'start_traffic': start_traffic,
                   'end_traffic': end_traffic,
                   'incidents': incidents,
                   'routes': routes
               }
   
           # Define the pipeline
           pipeline = Pipeline(
               inputs=[start_location, end_location],
               outputs=[Output("traffic_data", get_traffic_data)],
           )
   
           return pipeline
   
       def get_current_traffic_situation(self, start: Location, end: Location) -> Dict:
           current_time = datetime.now()
           if (self.cache_timestamp is None or
               current_time - self.cache_timestamp > self.cache_duration):
               self.cached_data = self.pipeline.run(
                   start_location=start,
                   end_location=end
               )
               self.cache_timestamp = current_time
           return self.cached_data
#just initialize serivces 
tomtom = TomTomAPI(TOMTOM_API_KEY)
traffic_manager = TrafficDataManager(tomtom)

tools = [
    Tool(
        name="Get Traffic Flow", 
        function=tomtom.get_traffic_flow, 
        description="Get real-time traffic flow data."
    ),
    Tool(
        name="Get Incidents", 
        function=tomtom.get_incidents, 
        description="Retrieve current traffic incidents."
    ),
    Tool(
        name="Calculate Route", 
        function=tomtom.calculate_route, 
        description="Calculate the best route between two points."
    ),
    search_tool  
]



route_planner = Agent(
    role='Route Planning Specialist',
    goal='Plan optimal routes considering real-time conditions',
    backstory="""You are an expert in route optimization with deep understanding 
    of traffic patterns and routing algorithms. You analyze real-time data to 
    suggest the best possible routes while considering multiple factors.""",
    tools=tools,
    verbose=True,
    llm=llm
)

traffic_analyzer = Agent(
    role='Traffic Pattern Analyst',
    goal='Analyze traffic patterns and predict congestion',
    backstory="""You are a traffic pattern specialist who excels at analyzing 
    real-time traffic data and historical patterns. You can predict congestion 
    and suggest timing adjustments for better travel experience.""",
    tools=tools,
    verbose=True,
    llm=llm
)

safety_advisor = Agent(
    role='Travel Safety Specialist',
    goal='Provide safety recommendations and incident alerts',
    backstory="""You are a safety expert specialized in traffic incident analysis 
    and prevention. You provide crucial safety advice based on current conditions, 
    weather, and reported incidents.""",
    tools=tools,
    verbose=True,
    llm=llm
)

optimization_agent = Agent(
    role='Journey Optimization Expert',
    goal='Optimize overall journey experience',
    backstory="""You are an expert in journey optimization who considers multiple 
    factors like comfort, convenience, and user preferences. You provide 
    comprehensive advice for the best possible travel experience.""",
    verbose=True,
    llm=llm
)

def create_navigation_tasks(start: Location, end: Location, user_preferences: Dict, traffic_data: Dict):
    route_planning_task = Task(
           description=f"""Analyze routes and provide optimal path recommendations:
           Start: {json.dumps(start.__dict__)}
           End: {json.dumps(end.__dict__)}
           Current Traffic Data: {json.dumps(traffic_data)}
           User Preferences: {json.dumps(user_preferences)}
           
           1. Evaluate all possible routes
           2. Consider real-time traffic conditions
           3. Account for user preferences
           4. Provide top 3 route recommendations with reasoning""",
           agent=route_planner,
           expected_output="A list of 3 recommended routes with detailed explanations for each, considering traffic conditions and user preferences."
       )

    traffic_analysis_task = Task(
        description=f"""Analyze traffic patterns and provide insights:
        Traffic Data: {json.dumps(traffic_data)}
        Time: {datetime.now().isoformat()}
        
        1. Identify current congestion patterns
        2. Predict upcoming traffic changes
        3. Suggest optimal departure times
        4. Highlight areas to avoid""",
        agent=traffic_analyzer,
        expected_output="A comprehensive traffic analysis report including current congestion patterns, predicted changes, optimal departure times, and areas to avoid."
    )

    safety_task = Task(
        description=f"""Provide safety analysis and recommendations:
        Incidents: {json.dumps(traffic_data['incidents'])}
        Route Data: {json.dumps(traffic_data['routes'])}
        
        1. Analyze current incidents and hazards
        2. Identify high-risk areas along routes
        3. Provide safety recommendations
        4. Suggest emergency alternatives""",
        agent=safety_advisor,
        expected_output="A safety report detailing current incidents, high-risk areas, safety recommendations, and emergency alternatives for the journey."
    )

    optimization_task = Task(
        description=f"""Optimize overall journey experience:
        Route Options: {json.dumps(traffic_data['routes'])}
        Traffic Analysis: {json.dumps(traffic_data)}
        User Preferences: {json.dumps(user_preferences)}
        
        1. Consider comfort factors
        2. Evaluate route stress levels
        3. Suggest breaks and points of interest
        4. Provide comprehensive journey optimization""",
        agent=optimization_agent,
        expected_output="A detailed journey optimization plan including comfort considerations, stress reduction strategies, recommended breaks, and points of interest along the route."

    )

    return [route_planning_task, traffic_analysis_task, safety_task, optimization_task]

def run_navigation_system(start: Location, end: Location, user_preferences: Dict):
    tasks = create_navigation_tasks(start, end, user_preferences)
    crew = Crew(
        agents=[route_planner, traffic_analyzer, safety_advisor, optimization_agent],
        tasks=tasks,
        process=Process.sequential
    )
    return crew.kickoff()

if __name__ == "__main__":
    start_location = Location(40.7128, -74.0060, "Manhattan")
    end_location = Location(40.6782, -73.9442, "Brooklyn")
    
    user_preferences = {
        "priority": "balanced",
        "avoid_highways": False,
        "avoid_tolls": False,
        "preferred_stops": ["gas_station", "restaurant"],
        "max_walking_distance": 500,
        "safety_priority": "high"
    }
    
    try:
        print("Starting Smart Traffic Navigation System...")
        tomtom_api = TomTomAPI(TOMTOM_API_KEY)
        traffic_manager = TrafficDataManager(tomtom_api)
        
        # Get current traffic situation
        traffic_data = traffic_manager.get_current_traffic_situation(start_location, end_location)
        
        # Create navigation tasks using the traffic data
        tasks = create_navigation_tasks(start_location, end_location, user_preferences, traffic_data)
        
        crew = Crew(
            agents=[route_planner, traffic_analyzer, safety_advisor, optimization_agent],
            tasks=tasks,
            process=Process.sequential
        )
        results = crew.kickoff()
        print("\nNavigation Recommendations:")
        print(results)
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")