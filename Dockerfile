FROM public.ecr.aws/lambda/python:3.12

### AGREGAR COSAS AL IMAGE

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}
COPY country_codes.txt ${LAMBDA_TASK_ROOT}
COPY prompt.txt ${LAMBDA_TASK_ROOT}

# Install the function's dependencies using file requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --target ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD ["app.handler"]
