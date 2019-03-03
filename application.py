from flask import Flask, request
import boto3
from boto3.dynamodb.conditions import Key, Attr
import requests

##########################################################
## IMPORTANT VARIABLES
mainpage_html = '''
<!DOCTYPE html>
<html>
   <head>
      <title>FAHAD'S PROGRAM4</title>
   </head>
   <body>
      <div id="container">
         <div class="title">
            <h1>FAHAD'S PROGRAM4</h1>
         </div>
         <div id="content">
            <form method="GET" action="/load">
               <input type="submit" value="Load Data" />
            </form>
            <form method="GET" action="/query">
                <p>First Name: <input name="first_name"></p>
                <p>Last Name: <input name="last_name"></p>
                <p>(Input is case sensitive)</p>
                <p><button type="submit">Query</button></p>
            </form>
            <form method="GET" action="/clear">
               <input type="submit" value="Clear Data" />
            </form>
         </div>
      </div>
   </body>
</html>
'''

query_open_html = '''
<!DOCTYPE html>
<html>
   <body>
      <div id="container">
         <div class="title">
            <h1>Query Results</h1>
         </div>
'''

s3_c = boto3.client('s3', region_name='us-west-2')
s3_r = boto3.resource('s3', region_name='us-west-2')
dynamodb_client = boto3.client('dynamodb', region_name='us-west-2')
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
##########################################################


# EB looks for an 'application' callable by default
application = Flask(__name__)

##########################################################
## DIRECT TO THE MAIN PAGE
application.add_url_rule('/', 'mainpage', (lambda: mainpage_html))
##########################################################


##########################################################
## LOAD DATA INTO MY S3 AND SHRED DATA INTO MY DYNAMODB
application.add_url_rule('/load', 'load', (lambda: mainpage_html + load_and_shred()))
def load_and_shred():

    while True:
        try:
            r = requests.get('https://s3-us-west-2.amazonaws.com/css490/input.txt')
            #r = requests.get('https://s3-us-west-2.amazonaws.com/drdoran-program4/data80entries2.txt')
        except requests.exceptions.RequestException:
            pass

        else:
            break

    # S3 overrides object with same key
    response = s3_c.put_object(Body=r.text, Key='input.txt', Bucket='ghostbucket490')
    response = s3_c.put_object_acl(ACL='public-read', Key='input.txt', Bucket='ghostbucket490')

    table_name = 'ghost'
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if table_name not in existing_tables:
        #create and wait for creation
        table = create_table()

    obj2list = r.text.splitlines()
    for line in obj2list:
        print(line)
        if len(line) > 0:
            person2dict = {}

            person2list = line.split()
            person2dict['lastName'] = {'S':person2list[0]}
            person2dict['firstName'] = {'S':person2list[1]}

            # for additional attributes
            for otherAtt in person2list[2:]:
                otherAtt2list = otherAtt.rsplit('=')
                person2dict[otherAtt2list[0]] = {'S':otherAtt2list[1]}
                dynamodb_client.put_item(TableName='ghost',Item=person2dict)

    return '<p>LOAD COMPLETE!</p>'

def create_table():
    table = dynamodb_client.create_table(
        TableName='ghost',
        KeySchema=[
            {
                'AttributeName': 'lastName',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'firstName',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'lastName',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'firstName',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    dynamodb_client.get_waiter('table_exists').wait(TableName='ghost')
    print('table created!!!')
    return table

##########################################################


##########################################################
## RUN A QUERY BASED ON USER INPUT
application.add_url_rule('/query', 'query', (lambda: query_db()))


def query_db():
    # if table is empty, return to mainpage and ask load
    # if table.item_count == 0:
    #     return mainpage_html + '<p>PLEASE CLICK LOAD FIRST</p>'

    table_name = 'ghost'
    existing_tables = dynamodb_client.list_tables()['TableNames']

    if table_name not in existing_tables:
        return '<p>Sorry load first!</p>'
    db = boto3.resource('dynamodb', 'us-west-2')
    table = db.Table('ghost')
    lastName = request.args.get('last_name')
    firstName = request.args.get('first_name')

    if len(lastName) is 0:
        if len(firstName) is 0:
            response = table.scan()

        else:
            response = table.scan(FilterExpression=Attr('firstName').eq(firstName))

    else:
        condition = Key('lastName').eq(lastName)
        if len(firstName) is not 0:
            # firstName is nonempty
            condition = condition & Key('firstName').eq(firstName)

        response = table.query(KeyConditionExpression=condition)


    #find the original rows from S3
    #myObj = s3_r.Bucket('ghostbucket490').Object('input.txt')
    #myOrigData = myObj.get()['Body'].read().decode().splitlines()

    result_list = []


    for line in response['Items']:
        _lastName = line['lastName']
        _firstName = line['firstName']
        fullName = _lastName + _firstName
        person = ""

        for key, value in line.items():
            person += key + ": " + value + "  "
            
        #for lineOrig in myOrigData:
            #if lineOrig.replace(" ", "").startswith(fullName):
                #result_list.append(lineOrig)
        result_list.append(person)    


    if len(result_list) == 0:
        return '<p>No results found!</p>' + '<p><a href="/">Back</a></p></div></body></html>'



    query_open_html_current = query_open_html
    for result in result_list:
        query_open_html_current += '\n<p>' + result + '</p>'

    query_closed_html = query_open_html_current + '<p><a href="/">Back</a></p></div></body></html>'

    return query_closed_html

application.add_url_rule('/clear', 'clear', (lambda: mainpage_html + clear()))
def clear():
    table_name = 'ghost'
    existing_tables = dynamodb_client.list_tables()['TableNames']

    if table_name not in existing_tables:
        return '<p>Sorry load first!</p>'
    else:
     s3 = boto3.resource('s3')
     bucket = s3.Bucket('ghostbucket490')
     bucket.objects.all().delete()
     #dynamodb_client.delete_table(TableName='ghost')
     #waiter = dynamodb_client.get_waiter('table_not_exists')
     #waiter.wait(TableName='ghost')


    
    table = dynamodb.Table(table_name)

    scan = table.scan()

    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(Key={
            'lastName' : each['lastName'],
            'firstName' : each['firstName']
        }
    )

    return '<p>Clear is successful!</p>'




##########################################################

# run the app
if __name__ == "__main__":
    application.run()
