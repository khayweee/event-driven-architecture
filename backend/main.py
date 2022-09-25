import json
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from redis_om import get_redis_connection, HashModel
import consumers

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_methods=['*'],
    allow_headers=['*']
)

redis = get_redis_connection(
    host="redis-15755.c292.ap-southeast-1-1.ec2.cloud.redislabs.com",
    port=15755,
    password='egbSroEPc0jFhqXRNE9wGfxzudr96zG4',
    decode_responses=True
)

class Delivery(HashModel):
    '''
    Delivery class is used to obtain delivery ID
    '''
    budget: int = 0
    notes: str = ''

    class Meta:
        database = redis

class Event(HashModel):
    '''
    Event class is the main object
    '''
    delivery_id: str = None
    type: str
    data: str #json in event request body

    class Meta:
        database = redis

@app.get('/deliveries/{pk}/status')
async def get_state(pk: str):
    state = redis.get(f'delivery:{pk}')
    
    if state is not None:
        return json.loads(state)
    return {}

@app.post('/deliveries/create')
async def create(request: Request):
    body = await request.json()

    #Storing in redis database
    delivery = Delivery(budget=body['data']['budget'],
                        notes=body['data']['notes']).save()
    event = Event(delivery_id=delivery.pk,
                  type=body['type'],
                  data=json.dumps(body['data'])).save()
    
    # Storing in Redis Cache
    state = consumers.CONSUMERS[event.type](state={}, event=event)
    redis.set(f'delivery:{delivery.pk}', json.dumps(state))
    return state

@app.post('/event')
async def dispatch(request: Request):
    """
    All events would sent through this endpoint,
    only differentiating factor is the type of event coming through

    Using dynamic functions to handle all events

    """
    body = await request.json()
    delivery_id = body['delivery_id']
    event = Event(delivery_id=delivery_id,
                type=body['type'],
                data=json.dumps(body['data'])).save()
    state = await get_state(delivery_id)
    
    new_state = consumers.CONSUMERS[event.type](state, event)
    redis.set(f'delivery:{delivery_id}', json.dumps(new_state))
    return new_state