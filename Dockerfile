FROM python:3.6

RUN apt-get update 
RUN apt install libgl1-mesa-glx --yes

WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /usr/src/app

CMD ["python", "main.py"]
