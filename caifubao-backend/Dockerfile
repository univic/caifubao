FROM python:3.10
LABEL authors="univic"
WORKDIR /app
ADD . /app
ADD requirements.txt /app/
RUN pip3 install -r requirements.txt -i https://pypi.mirrors.ustc.edu.cn/simple/
CMD ["python", "manage.py"]
