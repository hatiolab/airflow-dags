from datetime import datetime, timedelta
import jwt
from gql import gql, Client as GqlClient
from gql.transport.requests import RequestsHTTPTransport

from airflow.models.variable import Variable


# python task functions
def sync_all_market_place_channel_products(host_url, access_token) -> None:
    # TODO: change graphql mutation with variables to be run on schedule
    graphql_mutation = """
        mutation syncAllMarketplaceChannelProducts {
            syncAllMarketplaceChannelProducts
        }
    """

    # extract a domain name from the access token
    decoded_token = jwt.decode(access_token, options={"verify_signature": False})
    things_factory_domain = decoded_token.get("domain").get("subdomain")

    # set http url with headers(access token and things factory domain on multi-domain configuation)
    reqHeaders = {
        "authorization": access_token,
        "x-things-factory-domain": things_factory_domain,
    }

    # create http transport
    _transport = RequestsHTTPTransport(
        url=f"{host_url}/graphql",
        headers=reqHeaders,
        use_json=True,
    )

    # create grapql client
    client = GqlClient(
        transport=_transport,
        fetch_schema_from_transport=False,
    )

    # execute the mutation
    m = gql(graphql_mutation)
    client_result = client.execute(m)
    print("mutation execution result: ", client_result)
