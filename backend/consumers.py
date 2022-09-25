import json

def create_delivery(state, event):
    "Construct the state to be returned"
    data=json.loads(event.data)
    return {
        "id": event.delivery_id,
        "budget": int(data['budget']),
        "notes": data['notes'],
        "status": "ready"
    }

def start_delivery(state, event):
    # state['status'] = 'active'
    return state | {
        "status" : "active"
    }

CONSUMERS = {
    "CREATE_DELIVERY": create_delivery,
    "START_DELIVERY": start_delivery
}