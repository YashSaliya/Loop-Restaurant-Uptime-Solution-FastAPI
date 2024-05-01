# Project Title: Store Status Report Generation

## Table of Contents
- [Project Overview](#project-overview)
- [Technologies Used](#technologies-used)
- [Installation](#installation)
- [Usage](#usage)
- [API Endpoints](#api-endpoints)
  - [Generate Report](#generate-report)
  - [Get Report Status/Download](#get-report-statusdownload)
- [Video Demonstration](#video-demonstration)

## Project Overview
This project implements a RESTful API using FastAPI for generating and retrieving reports asynchronously. The API allows users to trigger the generation of a report, which runs in the background, and then retrieve the report once it's completed.

## Technologies Used
- FastAPI: A high-performance web framework for building APIs with Python.
- Uvicorn: An ASGI server for running Python web applications.
- UUID: Used for generating unique report IDs.

## Installation
Clone the repository:
```bash
git clone https://github.com/[your-username]/[your-repository-name].git
```

## Usage 
Install the Required Packages:

```bash
pip install -r requirements.txt
```

Run the application
```bash
uvicorn main:app --reload
```
This will start the server on localhost:8000

## API Endpoints
### Generate Report
- Endpoint: POST/report
- Description: This endpoint triggers the generation of a new report in the background and returns a JSON response containing the unique report ID.
- Response:
- ```json
  {
    "report_id": "a1b2c3d4-e5f6-7890-1234-567890abcdef"
  }
  ```
### Get Report status/download
- Endpoint: GET/report?report_id={report_id}
- Description:This endpoint retrieves the status or the generated report based on the provided report ID.

## Vide Demonstration
 [Loom Demo](https://www.loom.com/share/f5fedd259cdd412b93c96a49ed1f0df2?sid=7a624e0b-3cbb-491e-853d-52e38b57ee20)
  

