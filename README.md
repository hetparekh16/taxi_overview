# Taxi Fleet Monitoring

This project is a real-time dashboard for monitoring taxi fleets. It displays the current locations of taxis on a map, tracks various metrics such as the number of currently driving taxis, total distance covered, area violations, and speeding incidents.

## Table of Contents

[Overview](https://collaborating.tuhh.de/e-19/teaching/bd24_project_m6_b#overview) 

[Features](https://collaborating.tuhh.de/e-19/teaching/bd24_project_m6_b#features) 

[Prerequisites](https://collaborating.tuhh.de/e-19/teaching/bd24_project_m6_b#prerequisites) 

[Setup](https://collaborating.tuhh.de/e-19/teaching/bd24_project_m6_b#setup) 

[Usage](https://collaborating.tuhh.de/e-19/teaching/bd24_project_m6_b#usage) 

[Folder Structure](https://collaborating.tuhh.de/e-19/teaching/bd24_project_m6_b#folder-structure) 


## Overview
The Taxi Fleet Monitoring system uses Apache Flink for real-time data processing, Redis for data storage, and Flask for serving the web application. The dashboard visualizes taxi locations using Leaflet.js and updates data dynamically.

## Features

-> Real-time display of taxi locations on a map.

-> Tracks the number of currently driving taxis.

-> Calculates the total distance covered by all taxis.

-> Identifies area violations and speeding incidents.

-> Displays taxi information in an easy-to-read table format.

## Prerequisites

-> Docker

-> Docker Compose

## Setup

1. Clone the repository:

```bash
git clone <repository-url>
cd <repository-directory>
```
2. Build and start the services:
```bash
docker-compose up --build
```
3. Access the dashboard:

Open your browser and navigate to http://localhost:5000.

## Usage

The dashboard updates every 5 seconds to show the latest taxi locations and metrics. The map displays the current position of each taxi with a custom taxi icon. The table below the map provides additional statistics.

## Folder Structure
```bash
.
├── flask-server
│   ├── templates
│   │   ├── static
│   │   │   └── car.png
│   │   ├── index.html
│   ├── app.py
│   ├── Dockerfile
│   ├── requirements.txt
├── kafka-python-scripts
│   ├── consumer
│   ├── data
│   ├── producer
│   │   ├── producer.py
│   │   ├── Dockerfile
│   │   ├── requirements.txt
├── docker-compose.yaml
├── README.md
```

