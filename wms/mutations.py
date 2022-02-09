from datetime import datetime, timedelta
import jwt
from gql import gql, Client as GqlClient
from gql.transport.requests import RequestsHTTPTransport

# python task functions
def sync_all_marketpalce_order(host_url, access_token, company_domain_id) -> None:
    try:
        # TODO: Fetch this information from Airflow Variables, so you need to set variables on airflow webserver
        host_url = Variable.get("OPERATO_COREAPP_URL")
        access_token = Variable.get("OPERATO_COREAPP_ACCESS_TOKEN")
        print(f"host_url: {host_url}")
        print(f"access_token: {access_token}")

        from_date = datetime.today() - timedelta(days=3)
        to_date = datetime.now().isoformat()
        company_domain_id = Variable.get("COMPANY_DOMAIN_ID")

        # TODO: change graphql mutation with variables to be run on schedule
        graphql_mutation = """
            mutation syncAllMarketplaceOrder($companyDomainId: String!, $fromDate: String!, $toDate: String!) {
                syncAllMarketplaceOrder(companyDomainId:$companyDomainId, fromDate: $fromDate, toDate: $toDate)
            }
        """
        vars = {
            "companyDomainId": company_domain_id,
            "fromDate": from_date,
            "toDate": to_date,
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

    except Exception as ex:
        print("Exception: ", ex)
