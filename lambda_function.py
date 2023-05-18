import json
from custom_encoder import CustomEncoder
import boto3
from botocore.exceptions import ClientError
from botocore.paginate import TokenEncoder
import logging
from dynamodb_json import json_util as jsons
 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Creating the DynamoDB Table Resource
dynamodbTableName = 'wi-keys-dev'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodbTableName)

# Creating the DynamoDB and S3 Client
client = boto3.client('dynamodb', 'eu-west-3')
client_s3 = boto3.client('s3')

# Methods and Paths
getMethod = 'GET'
postMethod = 'POST'
deleteMethod = 'DELETE'
patchMethod = 'PATCH'
productPath = '/product'
productPathDeleteImage = '/product/delete_image'
productPathUpdateProduct_id = '/product/product_id'
productsPath = '/products'



# ------Principle Fuction---------
def lambda_handler(event, context):
    logger.info(event)
    try:
        httpMethod = event['httpMethod']
        path = event['path']
    except Exception as e:
        return buildResponse(504, f"error1 {str(e)}")
    if httpMethod == postMethod and path == productPath:
        try:
            response = saveProduct(json.loads(event['body']))
        except Exception as e:
            return buildResponse(504, f"error2 {str(e)}")
    elif httpMethod == getMethod and path == productsPath:
        try:
            next_token = None
            page_size = event['queryStringParameters']['pageSize']
            max_items = event['queryStringParameters']['maxItems']
            product_id = event['queryStringParameters']['productId']
            if product_id != "None" :
                next_token = {'ExclusiveStartKey': {'productId': {'S': str(product_id) }}}
            response = getProducts(page_size,next_token,max_items)
        except Exception as e:
            return buildResponse(504, f"error3 {str(e)}")
    elif httpMethod == patchMethod and path == productPath:
        try:
            requestBody = json.loads(event['body'])
            productId = event['queryStringParameters']['productId']
            response = modifyProduct(productId, requestBody)
        except Exception as e:
            return buildResponse(504, f"error4 {str(e)}")
    elif httpMethod == deleteMethod and path == productPath:
        try:
            productId = event['queryStringParameters']['productId']
            response = deleteProduct(productId)
        except Exception as e:
            return buildResponse(504, f"error5 {str(e)}")
    elif httpMethod == deleteMethod and path == productPathDeleteImage:
        try:
            productId = event['queryStringParameters']['productId']
            requestBody = json.loads(event['body'])
            image_url = str(requestBody['imgURL'])
            response = delete_image(productId, image_url)
        except Exception as e:
            return buildResponse(504, f"error6 {str(e)}")
            
            
            
            
            
            
            
            
    elif httpMethod == patchMethod and path == productPathUpdateProduct_id:
        try:
            productId = event['queryStringParameters']['productId']
            requestBody = json.loads(event['body'])
            response = update_product_id(productId, requestBody)
        except Exception as e:
            return buildResponse(504, f"error6 {str(e)}")
            
            
            
            
            
            
            
            
            
            
            
    else:
        return buildResponse(504, f"error7 {str(e)}")
    return response


# Function to save new product in DynamouDB
def saveProduct(requestBody):
    try:
        table.put_item(Item=requestBody)
        body = {
            'Operation': 'SAVE',
            'Message': 'SUCCESS',
            'Item': requestBody
        }
        return buildResponse(200, body)
    except:
        logger.exception('Do your custom error handling here !')
        
        
# Function to get All product from DynamoDB
def getProducts(page_size=None, starting_token=None, max_items=None):
    try:
        if starting_token != None :
            encoder = TokenEncoder()
            paginator = client.get_paginator('scan')
            response_iterator = paginator.paginate(TableName = dynamodbTableName,
                                                   
                                                   PaginationConfig={'PageSize': page_size,
                                                                     'MaxItems': max_items,
                                                                     "StartingToken": encoder.encode(starting_token)
                                                                    }
                                                    )
        else :
            paginator = client.get_paginator('scan')
            response_iterator = paginator.paginate(TableName = dynamodbTableName,
                                                   
                                                   PaginationConfig={'PageSize': page_size,
                                                                     'MaxItems': max_items
                                                                    }
                                                    )
    except ClientError as e:
      raise Exception("boto3 client error in getProducts: " + e.__str__())
    except Exception as e:
      raise Exception("Unexpected error in getProducts: " + e.__str__())
    response = []
    for page in response_iterator:
        clean = jsons.loads(page)
        response.append(clean)
    body = {
            'Message': 'SUCCESS',
            'Pages': response
        }
    return buildResponseAll(200, body)


# Function To Delete Image from DynamoDb and S3
def delete_image(productId, image_url):
    image_key = image_url.split("/")[-1]
    if image_key != 'default_image.jpg':
        response1 = client_s3.get_object(
                                Bucket='catalogue-products-images',
                                Key = f'images/{image_key}'
                                 )
        img_exist = response1['ResponseMetadata']['HTTPStatusCode']
        if img_exist == 200:
            try:
                s3_response1 = client_s3.delete_object(
                    Bucket='catalogue-products-images',
                    Key = f'images/{image_key}'
                )
                s3_response2 = client_s3.delete_object(
                    Bucket='catalogue-products-images',
                    Key = f'thumbs/{image_key}'
                )
            except:
                logger.exception('Do your custom error handling here !')
    if (image_key == 'default_image.jpg') or ((s3_response1["ResponseMetadata"]["HTTPStatusCode"] == 204) and (s3_response2["ResponseMetadata"]["HTTPStatusCode"] == 204)):
        ddb_response = client.get_item(
            TableName=dynamodbTableName,
            Key={
                'productId': {'S': productId}
            }
        )
    if ddb_response:
        clean_response = jsons.loads(ddb_response)
        images = clean_response['Item']['imgURL']
        if image_url in images:
            try:
                images.remove(image_url)
                dynamodb_response = client.update_item(
                TableName=dynamodbTableName,
                Key={
                    'productId': {'S': productId}
                },
                UpdateExpression='set imgURL = :g',
                ExpressionAttributeValues = { 
                                                ':g' : {'SS':images}
                                                },
                ReturnValues='UPDATED_NEW'
                )
                body = {
                    'Operation': 'DELETE',
                    'Message': 'SUCCESS'
                        }
                return buildResponse(200, body)
            except:
                logger.exception('Do your custom error handling here !')
    











# Function to update productId
def update_product_id(productId, requestBody):
    try:
        insertion = table.put_item(Item=requestBody)
    except Exception as e:
        return str(e)
    if insertion:
        try:
            deletion = table.delete_item(
                                            Key = {
                                                'productId': productId
                                            },
                                            ReturnValues='ALL_OLD'
                                        )
        except Exception as e:
            return str(e)
        response = {
            "MESSAGE": "SUCCESS",
            "It": requestBody
        }
        
        return buildResponse(200, response)












# Function to delete item from DynamoDB Table
def deleteProduct(productId):
    try:
        response = table.delete_item(
            Key = {
                'productId': productId
            },
            ReturnValues='ALL_OLD'
        )
        body = {
            'Operation': 'DELETE',
            'Message': 'SUCCESS',
            'deleteItem': response
        }
        return buildResponse(200, body)
    except:
        logger.exception('Do your custom error handling here !')


# Help function for    modifyProduct()
def get_update_params(body):
    update_expression = ["set "]
    update_values = dict()

    for key, val in body.items():
        update_expression.append(f" {key} = :{key},")
        update_values[f":{key}"] = val
    return "".join(update_expression)[:-1], update_values

# Function to modify item in DynamoDb table
def modifyProduct(productId, event_body):
    try:
        a, v = get_update_params(event_body)
        response = table.update_item(
            Key={
                'productId': productId
            },
            UpdateExpression=a,
            ExpressionAttributeValues = v,
            ReturnValues='UPDATED_NEW'
        )
        body = {
            'Operation': 'UPDATE',
            'Message': 'SUCCESS',
            'UpdateAttributes': response
        }
        return buildResponse(200, body)
    except:
        logger.exception('Do your custom error handling here !')


# Help functio tronsform datetime to string
def datetimeToString(body):
    for page in body['Pages']:
        for item in page['Items']:
            if 'createDate' in item.keys(): 
                item['createDate'] = str(item['createDate'])
            else:
                item['createDate'] = "Item have no date1"
                
            if 'modifyDate' in item.keys(): 
                item['modifyDate'] = str(item['modifyDate'])
            else:
                item['modifyDate'] = "Item have no date1"
    return body
    
# Build the response for get all product
def buildResponseAll(statusCode, body=None):
    response = {
        'statusCode': statusCode,
        'headers': {
            'content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }
    if body is not None:   
        response['body'] = json.dumps(datetimeToString(body), cls=CustomEncoder)
    return response

# Build the response of functions
def buildResponse(statusCode, body=None):
    response = {
        'statusCode': statusCode,
        'headers': {
            'content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }
    if body is not None:
        response['body'] = json.dumps(body, cls=CustomEncoder)
    return response