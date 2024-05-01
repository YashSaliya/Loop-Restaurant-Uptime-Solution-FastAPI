from fastapi import FastAPI, BackgroundTasks,responses, HTTPException
from starlette.responses import JSONResponse
import uvicorn
import uuid
import os
from service import generate_and_save_report,reports_processing

"""
  API polling logic : 
  1. Generate report -> outputs an report id 
  2. Use the report id to get the report 
  3. If reports/{report_id}.csv exists then give the status as completed and give the csv file in output
  4. If report_id in report_processing set then give status as running
  5. Else return Not found 
"""


app = FastAPI()

@app.post("/report")
async def trigger_report(background_tasks: BackgroundTasks):
  """
  Triggers the report generation process in the background.
  """
  report_id = str(uuid.uuid4())
  reports_processing.add(report_id)
  background_tasks.add_task(generate_and_save_report, report_id)
  return JSONResponse({"report_id": report_id})



@app.get("/report")
async def get_report(report_id: str):
  """
  Retrieves the status or the generated report.
  """
  report_path = f"reports/{report_id}.csv"
  if os.path.exists(report_path):
    return responses.FileResponse(path= report_path,status_code= 200, media_type="text/csv",filename= 'Report.csv')
  elif report_id in reports_processing:
    return JSONResponse({'status':"Running"})
  else:
    raise HTTPException(status_code = 404, detail = "Report not found") 

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
