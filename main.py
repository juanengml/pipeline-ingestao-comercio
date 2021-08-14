import requests, dataset, schedule, os, time
from console_logging.console import Console

console = Console()

url_db = os.getenv("DATABASE_URL")
db = dataset.connect(url_db)
table = db['tbl_commerce']

endpoint = "https://random-data-api.com/api/commerce/random_commerce"
data = {"size":100}

def job():
  for i in range(3): 
      console.log("EXTRAINDO BASE DE DADOS - COMMERCE ")
      result = requests.get(endpoint,data).json()
      for item in result:
        console.info("LOAD DATA - "+str(item))
        table.insert(item)
      console.success("DONE !")
      time.sleep(10)
  
def main():
  schedule.every(1).minutes.do(job)
  while True:
    console.log(".")
    schedule.run_pending()
    time.sleep(1)

if __name__ == "__main__":
     main()