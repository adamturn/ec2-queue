FROM python:3.6

COPY src /app/src

CMD [ "python", "/app/src/main.py", "cmd=start", "ec2=i-021833cc22360cb5c" ]
