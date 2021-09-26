import logging

import azure.functions as func
from . import mutualFund


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    mutualFund.evaluate()

    return func.HttpResponse("Hello,  This HTTP triggered function executed successfully.")
