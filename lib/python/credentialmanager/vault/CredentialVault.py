### PostgreSQL Credential, SGDACredential, Local Credential
import inspect
import os
from sqlalchemy.engine.url import make_url
from .AbstractCredentialVault import *

class LocalCredentialsVault(AbstractCredentialVault):
    """
    Local credentials vault
    based on SQLite
    """

    def __init__(self, owner, table_name=None):
        self.sqlite_file = owner.conn_db
        url = make_url(self.sqlite_file)
        if url.drivername == 'sqlite' and url.database !='./.credentials/localVault.sqlite':
            conn_str = self.sqlite_file
        else:
            self.sqlite_file = './.credentials/localVault.sqlite'
            os.makedirs(os.path.dirname(self.sqlite_file), exist_ok=True)
            conn_str = 'sqlite:///'+self.sqlite_file
        if table_name is None:
            super(LocalCredentialsVault, self).__init__(owner, conn_str)
        else:
            super(LocalCredentialsVault, self).__init__(owner, conn_str, table_name=table_name)


class PGSqlCredentialsVault(AbstractCredentialVault):
    """
    PostgreSQL credential vault
    """

    def __init__(self, owner, credential):
        self.setId("pgsql_vault")
        # create the connection string for this credential storage
        conn_str = "postgresql+psycopg2://{}:{}@{}/{}".format(credential.content['username'], credential.content['password'],
                                                   credential.content['host'], credential.content['database'])

        if not credential.hasKey("table"):
            super().__init__(owner, conn_str)
        else:
            super().__init__(owner, conn_str, table_name="credentials")


# Credential vaults need to be created by inheriting a credentialVault class
class SGDACredentialVault(PGSqlCredentialsVault):
    def __init__(self, owner):
        self.setId("sgda_vault")
        # get the credential from the current vault in owner
        credential = owner.retrieve("sgda_vault_credential", False, None)
        super().__init__(owner,credential)
