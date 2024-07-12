import asyncio
from ollama import AsyncClient
import time
import pandas as pd
import math


def split(a, n) -> list:
    """
    Splits a list into n equal-sized sublists.

    Args:
        a (list): The list to be split.
        n (int): The number of sublists to create.

    Returns:
        generator: A generator that yields n sublists.

    Example:
        >>> a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        >>> n = 3
        >>> result = split(a, n)
        >>> list(result)
        [[1, 2, 3, 4], [5, 6, 7], [8, 9, 10]]
    """
    k, m = divmod(len(a), n)
    return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))


async def run_query_async(model, query, instance_id):
    """
    Run a query asynchronously using the specified model and instance ID.

    Args:
        model (str): The model to use for the query.
        query (str): The query to be executed.
        instance_id (int): The ID of the instance.

    Returns:
        str: The response message content.

    Raises:
        Exception: If an error occurs during the query execution.
    """
    try:
        client = AsyncClient()
        print(f"\nInstance {instance_id}: Starting query")
        response = await client.chat(
            model=model,
            messages=[{'role': 'user', 'content': query}],
        )
        print(f"\nInstance {instance_id}: [DONE]")
        return response['message']['content']
    except Exception as e:
        print(f"\nInstance {instance_id}: Error - {str(e)}")


async def main(adress):
    """
    This function takes a list of addresses and performs queries to retrieve the house numbers of each address.
    The function uses a specified model and a predefined set of queries to extract the house numbers.
    The extracted house numbers are returned as a JSON object.

    Args:
        adress (list): A list of addresses to process.

    Returns:
        list: A list of JSON objects containing the extracted house numbers for each address.
    """
    model = "llama3"  # Replace with your desired model

    queries = '''Help me create a new corrected address.
                Taking the first number after the first street, and the last number of the second street, the other streets must alternate in the same logic.
                Use the sequence of numbers.
                Each street must contain only one number.
                The streets are divided by '/'. 
                Return the new address keeping the same pattern.
                Response in json format e.g.: {'result':new_adress}  no more text, no explications.
                like :RUA NOME, 1,2 / RUA SAO PAULO, 1,2,3 / RUA COSTA, 1,2
                to {"result":"RUA DOS GUAICURUS, 1 / RUA SAO PAULO, 3 / RUA COSTA, 1"}

                input: RUA DONA ANITA, 283,285,287,289,291,293 / RUA MENDES PIMENTEL, 80 / RUA ESTEVAO DE OLIVEIRA, 134,136,138'''

    tasks = []

    for i in range(len(adress)):
        tasks.append(run_query_async(model, queries+adress[i], i))

    # Wait for all tasks to complete
    return await asyncio.gather(*tasks)


if __name__ == "__main__":

    df = pd.read_csv('2024-06_projetos_edificacoes_licenciados.csv', sep=';')

    adress = df['ENDERECO'].to_list()

    size = int(math.ceil((len(adress)/100)))

    part = list(split(adress,  size))

    number_list = []

    start_time = time.time()
    for i in range(len(part)):
        number_list.append(asyncio.run(main(part[i])))
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")

    with open('numeros.txt', 'w') as arquivo:
        arquivo.write(str(number_list))
