FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

# Copy RDS CA file
COPY global-bundle.pem ${LAMBDA_TASK_ROOT}

# Copy the function's code
COPY main.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD ["main.handler"]
