import os
import json
import time
import requests
import logging
from typing import Dict
from dotenv import load_dotenv

load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCADE_API_KEY = os.getenv('SCADE_API_KEY')
logger.info(f"Loaded SCADE API Key: {SCADE_API_KEY}")

def execute_scade_flow(flow_id: str, start_node_id: str, end_node_id: str, result_node_id: str, input_values: Dict[str, str]) -> str:
    logger.info(f"Starting SCADE flow execution: flow_id={flow_id}, start_node_id={start_node_id}, end_node_id={end_node_id}, result_node_id={result_node_id}")
    logger.info(f"Input values: {input_values}")

    url = f"https://api.scade.pro/api/v1/scade/flow/{flow_id}/execute"
    payload = {
        "start_node_id": start_node_id,
        "end_node_id": end_node_id,
        "result_node_id": result_node_id,
        "node_settings": {
            start_node_id: {
                "data": input_values
            }
        }
    }
    headers = {
        'Authorization': f'Basic {SCADE_API_KEY}',
        'Content-Type': 'application/json'
    }

    logger.info(f"Sending request to SCADE API: {url}")
    logger.debug(f"Payload: {json.dumps(payload, indent=2)}")

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Проверка на успешность запроса
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to execute SCADE flow: {e}")
        raise

    logger.info(f"Received response from SCADE API: {response.status_code}")
    logger.debug(f"Response content: {response.text}")

    task_id = response.json().get('id')
    if not task_id:
        logger.error("Не удалось получить task_id из ответа API.")
        raise Exception("Не удалось получить task_id из ответа API.")

    logger.info(f"Task ID received: {task_id}")

    result = wait_for_task_result(task_id)

    output = result.get("result", {}).get("output")
    if output is None:
        output = result.get("result", {}).get("success", {}).get("output")
    if output is None:
        logger.error("Не удалось получить output из результата SCADE.")
        raise ValueError("Не удалось получить output из результата SCADE.")

    logger.info(f"SCADE flow executed successfully. Output: {output}")
    return output

def wait_for_task_result(task_id: str, max_attempts: int = 300, delay: int = 3) -> Dict:
    url = f"https://api.scade.pro/api/v1/task/{task_id}"
    headers = {
        'Authorization': f'Basic {SCADE_API_KEY}',
        'Content-Type': 'application/json'
    }

    logger.info(f"Waiting for task result: task_id={task_id}")

    for attempt in range(max_attempts):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error while checking task status: {e}")
            raise

        task_data = response.json()
        logger.debug(f"Task status response: {json.dumps(task_data, indent=2)}")

        if task_data.get('status') == 3:  # Предполагаем, что статус 3 означает "выполнено"
            logger.info(f"Task completed successfully on attempt {attempt + 1}")
            return task_data  # Возвращаем результат задачи

        logger.info(f"Task is still running. Attempt {attempt + 1} of {max_attempts}")
        time.sleep(delay)  # Задержка перед следующей попыткой

    logger.error("Превышено максимальное количество попыток ожидания результата.")
    raise TimeoutError("Превышено максимальное количество попыток ожидания результата.")

# Пример использования (может быть удален в финальной версии)
if __name__ == "__main__":
    result = execute_scade_flow(
        flow_id="",
        start_node_id="axi1-start",
        end_node_id="AQ6K-end",
        result_node_id="AQ6K-end",
        input_values = {
            "": ""
        }
    )

    logger.info(f"Final result: {result}")
