from datetime import datetime, timedelta
from openai import OpenAI
import pandas as pd
import boto3
import json
import time
import ast
import re

import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def convert_seconds_to_hhmmss(seconds):
    # Ensure seconds are converted to an integer
    seconds = float(seconds)
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def answer_question(question, prompt_data, model='sonnet', tokens=1000):
    bedrock = boto3.client(
        service_name = 'bedrock-runtime',
        region_name = 'us-east-1'
    )

    if model == 'sonnet':
        modelId = "us.anthropic.claude-3-5-sonnet-20241022-v2:0"
        modelId = "us.anthropic.claude-3-5-sonnet-20240620-v1:0"
        # modelId = "anthropic.claude-v2:1"
    elif model == 'nova':
        modelId = "us.amazon.nova-lite-v1:0"
    elif model == 'haiku':
        # modelId = "us.anthropic.claude-3-5-haiku-20241022-v1:0"
        modelId = "us.anthropic.claude-3-haiku-20240307-v1:0"
        # modelId = "anthropic.claude-v2:1"
    elif model == "openai":
        modelId = "gpt-4o-mini"

    input = {
        "modelId": modelId,
        "contentType": "application/json",
        "accept": "*/*"
    }
    
    if modelId in ["us.anthropic.claude-3-5-sonnet-20240620-v1:0", "us.anthropic.claude-3-5-sonnet-20241022-v2:0"]:
        body = json.dumps(
            {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": tokens,
                "system": prompt_data,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": question
                            }
                        ]
                    }
                ]
            }
        )
        response = bedrock.invoke_model(body=body,
                                        modelId=input['modelId'],
                                        accept=input['accept'],
                                        contentType=input['contentType'])
        response_body = json.loads(response['body'].read())

    elif model == "nova":
        print("Using NOVA")
        system_list = [{"text": prompt_data}]

        # Define one or more messages using the "user" and "assistant" roles.
        message_list = [{"role": "user", "content": [{"text": question}]}]

        # Configure the inference parameters.
        inf_params = {"max_new_tokens": tokens, "top_p": 0.9, "top_k": 20, "temperature": 0.7}

        body = {
            "schemaVersion": "messages-v1",
            "messages": message_list,
            "system": system_list,
            "inferenceConfig": inf_params,
        }

        response = bedrock.invoke_model(body=json.dumps(body),
                                        modelId=modelId)
        response_body = json.loads(response['body'].read())
        

    elif model == "openai":
        client = OpenAI()

        completion = client.chat.completions.create(
            model=input['modelId'],
            messages=[
                {"role": "system", "content": prompt_data},
                {
                    "role": "user",
                    "content": question
                }
            ]
        )

        response_body = completion.choices[0].message.content
        print(f"Type of texto: {type(response_body)}, Value: {response_body}")


    return response_body


def get_query(question, model="sonnet", tokens=1000):
    prompt_data = open("prompt.txt", "r").read()
    country_codes = open("country_codes.txt", "r").read()
    prompt_data += country_codes

    response_body = answer_question(question, prompt_data, model=model, tokens=tokens)

    if model=="nova":
        texto = response_body["output"]["message"]["content"][0]["text"]
    elif model=="sonnet":
        texto = response_body["content"][0]["text"] 
    elif model=="openai":
        texto = response_body 
    
    match = re.search(r'<query>(.*?)</query>', texto, re.DOTALL)
    if match:
        query = match.group(1).strip()
    else:
        query = ""

    match = re.search(r'<contexto>(.*?)</contexto>', texto, re.DOTALL)
    if match:
        contexto = match.group(1).strip()

    match = re.search(r'<thinking>(.*?)</thinking>', texto, re.DOTALL)
    if match:
        thinking = match.group(1).strip()

    match = re.search(r'<column_names>(.*?)</column_names>', texto, re.DOTALL)
    if match:
        column_names = match.group(1).strip()
    else:
        column_names = ""

    match = re.search(r'<tipo_pregunta>(.*?)</tipo_pregunta>', texto, re.DOTALL)
    if match:
        tipo_pregunta = match.group(1).strip()

    return query, contexto, thinking, column_names, tipo_pregunta


def handler(event, context):
    logging.info('Event: %s', event)
    # question = event.get('question', 'No message provided')
    start_time = datetime.now()
    question = event['question']

    query, contexto, thinking, column_names, tipo_pregunta = get_query(question, model="openai", tokens=1000)

    time_to_query = datetime.now() - start_time
    print(f"Time to query: {time_to_query}")

    if tipo_pregunta == "resultado":
    #    print("--- thinking")
    #    print(thinking)
    #    print("--- end thinking")

        print("--- query")
        print(query)
        print("--- end query")

        print("--- column_names")
        print(column_names)
        print("--- end column_names")

        database = 'marathon'
        
        athena_client = boto3.client('athena', region_name='us-east-2')
        
        QueryResponse = athena_client.start_query_execution(
            QueryString = query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': 's3://zarruk/Unsaved/2024/05/05/'
                }
        )
            
        QueryID = QueryResponse['QueryExecutionId']
        
        time.sleep(5)
        
        query_results = athena_client.get_query_results(QueryExecutionId=QueryID)
        time_to_query_results = datetime.now() - start_time
        print(f"Time to query results: {time_to_query_results}")
        
        # Extract rows from the JSON
        rows = query_results['ResultSet']['Rows']

        # Convert rows to a list of lists
        data = [[col['VarCharValue'] for col in row['Data']] for row in rows]

        # Create DataFrame
        df = pd.DataFrame(data[1:], columns=data[0])  # Skip the first row for column names

        print(df)

        columns = df.columns.tolist()  # Save the original column order
        new_columns = []  # To track the modified columns

        for col in columns:
            if "seconds" in col:
                new_col = col.replace("seconds", "time")
                df[new_col] = df[col].apply(convert_seconds_to_hhmmss)
                df = df.drop(columns=[col])
                new_columns.append(new_col)
            else:
                new_columns.append(col)

        df = df[new_columns] 

        df.columns = column_names.split(";")

        prompt_data = f"""
        Toma esta información: 
        <respuesta>
        {contexto}:
        {df.to_string(index=False)}
        </respuesta>
        
        Toma esta respuesta y agrega un resumen de los resultados en una o dos 
        frases y, si encuentras, da datos curiosos.

        Toda tu respuesta, incluida la que te estoy pasando, debe estar en formato HTML.
        """

    #    print(prompt_data)
    #    response = answer_question(question, prompt_data, tokens=100)

        resultado = f"""
        {contexto}
        {df.to_html(index=False)}
        """
    else: 
        resultado = f"""
        {contexto}
        """

#    {response['content'][0]['text']}
#    print("--- response")
#    print(resultado)
#    print("--- end response")

    return {
        'statusCode': 200,
        'body': resultado
    }


if __name__ == "__main__":

    response = handler(
        {
            "question": "cual fue el tiempo promedio de los colombianos cada año?"
        }, {})

    print(response)
