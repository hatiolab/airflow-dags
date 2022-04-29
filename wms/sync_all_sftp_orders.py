from datetime import datetime, timedelta
import jwt
from gql import gql, Client as GqlClient
from gql.transport.requests import RequestsHTTPTransport

from airflow.models.variable import Variable


def sync_all_sftp_orders(
    host_url, access_token, customer_domain_id
) -> None:
    graphql_mutation = """
        mutation syncSftpOrders {
            syncSftpOrders
        }
    """

    # extract a domain name from the access token
    decoded_token = jwt.decode(access_token, options={"verify_signature": False})
    things_factory_domain = decoded_token.get("domain").get("subdomain")

    # TODO: change graphql mutation with variables to be run on schedule
    graphql_mutation = """
        mutation syncSftpOrders($customerDomainId: String!) {
            syncSftpOrders(customerDomainId: $customerDomainId)
        }
    """
    vars = {
        "customerDomainId": customer_domain_id
    }

    # extract a domain name from the access token
    decoded_token = jwt.decode(access_token, options={"verify_signature": False})
    things_factory_domain = decoded_token.get("domain").get("subdomain")

    # set http url with headers(access token and things factory domain on multi-domain configuation)
    reqHeaders = {
        "authorization": access_token,
        "x-things-factory-domain": things_factory_domain,
    }
    _transport = RequestsHTTPTransport(
        url=f"{host_url}/graphql",
        headers=reqHeaders,
        use_json=True,
    )

    # create grapql client
    client = GqlClient(
        transport=_transport,
        fetch_schema_from_transport=True,
    )

    # execute the mutation
    m = gql(graphql_mutation)
    client_result = client.execute(m, variable_values=vars)
    print("mutation execution result: ", client_result)
