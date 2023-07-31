from google.cloud import pubsub
import os
import json

def whitelist_req(req, ranges):
    from ipaddress import ip_address, ip_network

    for r in ranges.split(','):
        if ip_address(req.remote_addr) in ip_network(r):
            return True

    return False


def pubsub_webhook(req):
    if req.method != 'POST':
        return ('Method not allowed', 405)

    if 'IP_WHITELIST' in os.environ:
        if not whitelist_req(req, os.environ['IP_WHITELIST']):
            return ('Forbidden', 403)

    if 'TOKENS' in os.environ:
        if req.headers.get('x-token') not in (
                os.environ.get('TOKENS') or "").split(","):
            return ('Unauthorized', 403)

    client = pubsub.PublisherClient()

    topic_project = os.environ.get('TOPIC_PROJECT', os.environ['GCP_PROJECT'])
    topic_name = os.environ['TOPIC_NAME']

    topic = f'projects/{topic_project}/topics/{topic_name}'

    data_raw = req.get_data()
    pub_data = None
    message_data = None
    kwargs = {}
    message_type = None
    origin = None
    username = None
    try:
        data_json = json.loads(data_raw)
        message_data = data_json.get("data")
    except json.decoder.JSONDecodeError:
        print("Message is not json, publishing raw")

    if message_data:
        pub_data = json.dumps(message_data).encode("utf-8")
        attributes = data_json.get("attributes")
        if attributes:
            kwargs = attributes
            message_type = attributes.get("type")
            origin = attributes.get("origin")
            username = attributes.get("username")
    else:
        pub_data = data_raw

    # client.publish(topic, pub_data)
    client.publish(
        topic, pub_data, **kwargs
    )

    return f"ok {message_type} {origin} {username}"
